from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from .config import Config
from .db import ensure_indexes as mongo_ensure_indexes, get_db as mongo_get_db, Collections
from .pg import ensure_schema as pg_ensure_schema, get_engine
from .utils import normalize_username


# FIX (Bug #5): Whitelist of valid column names for job_patch to prevent SQL injection
_JOBS_VALID_COLUMNS: frozenset[str] = frozenset({
    "status", "username_norm", "username_display", "started_at", "finished_at",
    "inserted", "matched_posts", "page", "total_pages", "batch",
    "range_newer_than", "range_older_than", "title_only", "error",
})

# FIX (Bug #9): Batch size for media_upsert_many to prevent oversized single inserts
_UPSERT_BATCH_SIZE = 500


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def init_storage() -> None:
    if Config.DB_BACKEND == "postgres":
        if not Config.DATABASE_URL:
            # Auto-fallback for local dev if DATABASE_URL not provided
            Config.DB_BACKEND = "mongo"
            mongo_ensure_indexes()
            return
        pg_ensure_schema()
        return
    mongo_ensure_indexes()


# -------------------- Jobs --------------------

def job_create(username_display: str) -> str:
    uname_norm = normalize_username(username_display)
    # FIX (Bug #2): append short UUID suffix to prevent collision within the same second
    job_id = f"job_{uname_norm}_{int(utc_now().timestamp())}_{uuid4().hex[:6]}"
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO jobs(job_id, status, username_norm, username_display, created_at)
                    VALUES (:job_id, 'queued', :username_norm, :username_display, :created_at)
                    ON CONFLICT (job_id) DO UPDATE SET
                      status='queued',
                      username_norm=EXCLUDED.username_norm,
                      username_display=EXCLUDED.username_display
                    """
                ),
                {"job_id": job_id, "username_norm": uname_norm, "username_display": username_display, "created_at": utc_now()},
            )
        return job_id

    db = mongo_get_db()
    c = Collections()
    db[c.jobs].update_one(
        {"_id": job_id},
        {"$set": {"_id": job_id, "status": "queued", "username_display": username_display, "username_norm": uname_norm, "created_at": utc_now()}},
        upsert=True,
    )
    return job_id


def job_get(job_id: str) -> dict | None:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(text("SELECT * FROM jobs WHERE job_id=:id"), {"id": job_id}).mappings().first()
            return dict(row) if row else None

    db = mongo_get_db()
    c = Collections()
    return db[c.jobs].find_one({"_id": job_id})


def job_patch(job_id: str, patch: dict[str, Any]) -> None:
    if Config.DB_BACKEND == "postgres":
        # FIX (Bug #5): validate column names against whitelist before building SQL
        safe_patch = {k: v for k, v in patch.items() if k in _JOBS_VALID_COLUMNS}
        unknown = set(patch.keys()) - _JOBS_VALID_COLUMNS
        if unknown:
            import logging
            logging.getLogger("scrape_queue").warning(
                "job_patch: ignoring unknown column(s) %s for job %s", unknown, job_id
            )
        if not safe_patch:
            return
        cols = [f"{k} = :{k}" for k in safe_patch]
        params: dict[str, Any] = {"id": job_id, **safe_patch}
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text(f"UPDATE jobs SET {', '.join(cols)} WHERE job_id=:id"), params)
        return

    db = mongo_get_db()
    c = Collections()
    db[c.jobs].update_one({"_id": job_id}, {"$set": patch}, upsert=True)


def job_cancel_request(job_id: str) -> None:
    if Config.DB_BACKEND == "postgres":
        job_patch(job_id, {"status": "cancel_requested"})
    else:
        db = mongo_get_db()
        c = Collections()
        db[c.jobs].update_one({"_id": job_id}, {"$set": {"status": "cancel_requested"}}, upsert=False)


def job_is_cancel_requested(job_id: str) -> bool:
    j = job_get(job_id)
    if not j:
        return False
    return j.get("status") in ("cancel_requested", "paused")


# FIX (Bug #1): reset any jobs that were left in "running" state after a crash/restart
def reset_stale_running_jobs() -> int:
    """
    Called at startup. Any job stuck in 'running' status means the process crashed
    mid-scrape. Reset them to 'queued' so the queue can pick them up again.
    Returns the number of jobs reset.
    """
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            res = conn.execute(
                text("UPDATE jobs SET status='queued', started_at=NULL, error='reset after restart' WHERE status='running'")
            )
            return int(res.rowcount or 0)

    db = mongo_get_db()
    c = Collections()
    res = db[c.jobs].update_many(
        {"status": "running"},
        {"$set": {"status": "queued", "started_at": None, "error": "reset after restart"}},
    )
    return int(res.modified_count)


# -------------------- Users cache --------------------

def user_is_cached(username_display: str, newer_than: str, older_than: str) -> bool:
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT 1 FROM users
                    WHERE username_norm=:u AND cached_newer_than=:n AND cached_older_than=:o
                    """
                ),
                {"u": uname_norm, "n": newer_than, "o": older_than},
            ).first()
            return row is not None

    db = mongo_get_db()
    c = Collections()
    doc = db[c.users].find_one({"username_norm": uname_norm}, projection={"_id": 0, "cached_range": 1})
    if not doc:
        return False
    cr = doc.get("cached_range")
    return isinstance(cr, dict) and cr.get("newer_than") == newer_than and cr.get("older_than") == older_than


def user_mark_cached(username_display: str, newer_than: str, older_than: str, last_scraped_at: datetime) -> None:
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO users(username_norm, username_display, cached_newer_than, cached_older_than, last_scraped_at)
                    VALUES (:u, :d, :n, :o, :t)
                    ON CONFLICT (username_norm) DO UPDATE SET
                      username_display=EXCLUDED.username_display,
                      cached_newer_than=EXCLUDED.cached_newer_than,
                      cached_older_than=EXCLUDED.cached_older_than,
                      last_scraped_at=EXCLUDED.last_scraped_at
                    """
                ),
                {"u": uname_norm, "d": username_display, "n": newer_than, "o": older_than, "t": last_scraped_at},
            )
        return

    db = mongo_get_db()
    c = Collections()
    db[c.users].update_one(
        {"username_norm": uname_norm},
        {"$set": {"username_norm": uname_norm, "username_display": username_display, "cached_range": {"newer_than": newer_than, "older_than": older_than}, "last_scraped_at": last_scraped_at}},
        upsert=True,
    )


def user_delete(username_display: str) -> dict:
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            r1 = conn.execute(text("DELETE FROM media WHERE username_norm=:u"), {"u": uname_norm})
            r2 = conn.execute(text("DELETE FROM users WHERE username_norm=:u"), {"u": uname_norm})
        return {"ok": True, "deleted_media": r1.rowcount or 0, "deleted_user_meta": r2.rowcount or 0}

    db = mongo_get_db()
    c = Collections()
    media_res = db[c.media].delete_many({"username_norm": uname_norm})
    user_res = db[c.users].delete_one({"username_norm": uname_norm})
    return {"ok": True, "deleted_media": media_res.deleted_count, "deleted_user_meta": user_res.deleted_count}


# -------------------- Media write + query --------------------

def media_upsert_one(username_display: str, post_date: str, media_url: str, media_type: str, created_at: datetime) -> bool:
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            res = conn.execute(
                text(
                    """
                    INSERT INTO media(username_norm, username_display, post_date, media_url, media_type, created_at)
                    VALUES (:u, :d, :p, :url, :t, :c)
                    ON CONFLICT (username_norm, media_url) DO NOTHING
                    """
                ),
                {"u": uname_norm, "d": username_display, "p": post_date, "url": media_url, "t": media_type, "c": created_at},
            )
            return (res.rowcount or 0) > 0

    db = mongo_get_db()
    c = Collections()
    r = db[c.media].update_one(
        {"username_norm": uname_norm, "media_url": media_url},
        {"$setOnInsert": {"username_norm": uname_norm, "username_display": username_display, "post_date": post_date, "media_url": media_url, "media_type": media_type, "created_at": created_at}},
        upsert=True,
    )
    return r.upserted_id is not None


def media_upsert_many(rows: list[dict[str, Any]]) -> int:
    """
    FIX (Bug #9): rows are processed in chunks of _UPSERT_BATCH_SIZE to prevent
    oversized single DB transactions on high-volume pages.
    """
    if not rows:
        return 0

    total_inserted = 0

    # Split into batches
    num_batches = math.ceil(len(rows) / _UPSERT_BATCH_SIZE)
    batches = [rows[i * _UPSERT_BATCH_SIZE:(i + 1) * _UPSERT_BATCH_SIZE] for i in range(num_batches)]

    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        stmt = text(
            """
            INSERT INTO media(username_norm, username_display, post_date, media_url, media_type, created_at)
            VALUES (:u, :d, :p, :url, :t, :c)
            ON CONFLICT (username_norm, media_url) DO NOTHING
            """
        )
        for batch in batches:
            payload = []
            for r in batch:
                d = (r.get("username_display") or "").strip()
                payload.append(
                    {
                        "u": normalize_username(d),
                        "d": d,
                        "p": r.get("post_date"),
                        "url": r.get("media_url"),
                        "t": r.get("media_type"),
                        "c": r.get("created_at"),
                    }
                )
            with eng.begin() as conn:
                res = conn.execute(stmt, payload)
                total_inserted += int(res.rowcount or 0)
        return total_inserted

    db = mongo_get_db()
    c = Collections()
    from pymongo import UpdateOne
    for batch in batches:
        ops = []
        for r in batch:
            d = (r.get("username_display") or "").strip()
            uname_norm = normalize_username(d)
            ops.append(
                UpdateOne(
                    {"username_norm": uname_norm, "media_url": r.get("media_url")},
                    {
                        "$setOnInsert": {
                            "username_norm": uname_norm,
                            "username_display": d,
                            "post_date": r.get("post_date"),
                            "media_url": r.get("media_url"),
                            "media_type": r.get("media_type"),
                            "created_at": r.get("created_at"),
                        }
                    },
                    upsert=True,
                )
            )
        try:
            result = db[c.media].bulk_write(ops, ordered=False)
            total_inserted += int(result.upserted_count or 0)
        except Exception:
            pass
    return total_inserted


def media_count(username_norms: list[str], *, media_type: str | None = None, year: str | None = None) -> int:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = ["username_norm = ANY(:u)"]
        params: dict[str, Any] = {"u": username_norms}
        if media_type:
            where.append("media_type = :mt")
            params["mt"] = media_type
        if year:
            where.append("post_date LIKE :y")
            params["y"] = f"{year}%"
        q = "SELECT COUNT(*) AS c FROM media WHERE " + " AND ".join(where)
        with eng.begin() as conn:
            return int(conn.execute(text(q), params).scalar() or 0)

    db = mongo_get_db()
    c = Collections()
    f: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if media_type:
        f["media_type"] = media_type
    if year:
        f["post_date"] = {"$regex": f"^{year}"}
    return int(db[c.media].count_documents(f))


def media_page(username_norms: list[str], *, media_type: str | None, year: str | None, page: int, ipp: int) -> list[dict]:
    skip = (page - 1) * ipp
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = ["username_norm = ANY(:u)"]
        params: dict[str, Any] = {"u": username_norms, "limit": ipp, "offset": skip}
        if media_type:
            where.append("media_type = :mt")
            params["mt"] = media_type
        if year:
            where.append("post_date LIKE :y")
            params["y"] = f"{year}%"
        q = (
            "SELECT media_url, media_type, post_date FROM media WHERE "
            + " AND ".join(where)
            + " ORDER BY post_date DESC, id DESC LIMIT :limit OFFSET :offset"
        )
        with eng.begin() as conn:
            rows = conn.execute(text(q), params).mappings().all()
            return [{"src": r["media_url"], "type": r["media_type"], "date": r["post_date"]} for r in rows]

    db = mongo_get_db()
    c = Collections()
    f: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if media_type:
        f["media_type"] = media_type
    if year:
        f["post_date"] = {"$regex": f"^{year}"}
    cur = (
        db[c.media]
        .find(f, projection={"_id": 0, "media_url": 1, "media_type": 1, "post_date": 1})
        .sort("post_date", -1)
        .skip(skip)
        .limit(ipp)
    )
    return [{"src": r.get("media_url", ""), "type": r.get("media_type", "images"), "date": r.get("post_date", "")} for r in cur]


def media_type_counts(username_norms: list[str]) -> dict[str, int]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text("SELECT media_type, COUNT(*)::bigint AS c FROM media WHERE username_norm = ANY(:u) GROUP BY media_type"),
                {"u": username_norms},
            ).all()
        out = {"images": 0, "videos": 0, "gifs": 0}
        for mt, c in rows:
            if mt in out:
                out[mt] = int(c)
        return out

    db = mongo_get_db()
    c = Collections()
    out = {"images": 0, "videos": 0, "gifs": 0}
    for row in db[c.media].aggregate([{"$match": {"username_norm": {"$in": username_norms}}}, {"$group": {"_id": "$media_type", "c": {"$sum": 1}}}]):
        k = row["_id"] or "images"
        if k in out:
            out[k] = int(row["c"])
    return out


def media_year_counts(username_norms: list[str]) -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT SUBSTRING(post_date, 1, 4) AS y, COUNT(*)::bigint AS c
                    FROM media
                    WHERE username_norm = ANY(:u)
                    GROUP BY y
                    ORDER BY y DESC
                    """
                ),
                {"u": username_norms},
            ).all()
        return [{"year": y, "count": int(c)} for (y, c) in rows if isinstance(y, str) and len(y) == 4 and y.isdigit()]

    db = mongo_get_db()
    c = Collections()
    out = []
    for row in db[c.media].aggregate(
        [
            {"$match": {"username_norm": {"$in": username_norms}}},
            {"$project": {"y": {"$substrBytes": ["$post_date", 0, 4]}}},
            {"$group": {"_id": "$y", "c": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
        ]
    ):
        y = row["_id"]
        if isinstance(y, str) and len(y) == 4 and y.isdigit():
            out.append({"year": y, "count": int(row["c"])})
    return out


# -------------------- Username listing & search --------------------

def get_all_usernames() -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT u.username_display, u.username_norm,
                           COALESCE(m.cnt, 0)::bigint AS media_count
                    FROM users u
                    LEFT JOIN (
                        SELECT username_norm, COUNT(*) AS cnt FROM media GROUP BY username_norm
                    ) m ON u.username_norm = m.username_norm
                    ORDER BY u.username_display
                    """
                )
            ).mappings().all()
        return [{"username_display": r["username_display"], "username_norm": r["username_norm"], "media_count": int(r["media_count"])} for r in rows]

    db = mongo_get_db()
    c = Collections()
    users = list(db[c.users].find({}, projection={"_id": 0, "username_display": 1, "username_norm": 1}))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        cnt = db[c.media].count_documents({"username_norm": norm})
        result.append({"username_display": u.get("username_display", norm), "username_norm": norm, "media_count": cnt})
    result.sort(key=lambda x: x.get("username_display", "").lower())
    return result


def get_users_with_latest_date() -> list[dict[str, Any]]:
    """Fetches all users alongside the MAX(post_date) they have in the DB."""
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT u.username_display, u.username_norm, MAX(m.post_date) as latest_date
                    FROM users u
                    LEFT JOIN media m ON u.username_norm = m.username_norm
                    GROUP BY u.username_display, u.username_norm
                    ORDER BY u.username_display
                    """
                )
            ).mappings().all()
        return [{"username_display": r["username_display"], "username_norm": r["username_norm"], "latest_date": r["latest_date"]} for r in rows]

    db = mongo_get_db()
    c = Collections()
    pipeline = [{"$group": {"_id": "$username_norm", "latest_date": {"$max": "$post_date"}}}]
    media_max = {doc["_id"]: doc["latest_date"] for doc in db[c.media].aggregate(pipeline)}
    users = list(db[c.users].find({}, projection={"_id": 0, "username_display": 1, "username_norm": 1}))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        result.append({
            "username_display": u.get("username_display", norm),
            "username_norm": norm,
            "latest_date": media_max.get(norm),
        })
    return result


def search_usernames(query: str) -> list[dict[str, Any]]:
    query = (query or "").strip().lower()
    if not query:
        return get_all_usernames()

    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT u.username_display, u.username_norm,
                           COALESCE(m.cnt, 0)::bigint AS media_count
                    FROM users u
                    LEFT JOIN (
                        SELECT username_norm, COUNT(*) AS cnt FROM media GROUP BY username_norm
                    ) m ON u.username_norm = m.username_norm
                    WHERE u.username_norm ILIKE :q
                    ORDER BY u.username_display
                    """
                ),
                {"q": f"%{query}%"},
            ).mappings().all()
        return [{"username_display": r["username_display"], "username_norm": r["username_norm"], "media_count": int(r["media_count"])} for r in rows]

    db = mongo_get_db()
    c = Collections()
    import re as _re
    pattern = _re.compile(_re.escape(query), _re.IGNORECASE)
    users = list(db[c.users].find({"username_norm": {"$regex": pattern}}, projection={"_id": 0, "username_display": 1, "username_norm": 1}))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        cnt = db[c.media].count_documents({"username_norm": norm})
        result.append({"username_display": u.get("username_display", norm), "username_norm": norm, "media_count": cnt})
    result.sort(key=lambda x: x.get("username_display", "").lower())
    return result


def get_queued_jobs() -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text("SELECT * FROM jobs WHERE status='queued' ORDER BY created_at ASC")
            ).mappings().all()
        return [dict(r) for r in rows]

    db = mongo_get_db()
    c = Collections()
    return list(db[c.jobs].find({"status": "queued"}, sort=[("created_at", 1)]))
