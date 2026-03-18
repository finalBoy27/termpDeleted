from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from .config import Config
from .db import ensure_indexes as mongo_ensure_indexes, get_db as mongo_get_db, Collections
from .pg import ensure_schema as pg_ensure_schema, get_engine
from .utils import normalize_username


_JOBS_VALID_COLUMNS: frozenset[str] = frozenset({
    "status", "username_norm", "username_display",
    "started_at", "finished_at", "inserted", "matched_posts",
    "page", "total_pages", "batch",
    "range_newer_than", "range_older_than",
    "title_only", "error",
})
_UPSERT_BATCH_SIZE = 500


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def init_storage() -> None:
    if Config.DB_BACKEND == "postgres":
        if not Config.DATABASE_URL:
            Config.DB_BACKEND = "mongo"
            mongo_ensure_indexes()
            return
        pg_ensure_schema()
        return
    mongo_ensure_indexes()


# ──────────────────────────────── Jobs ────────────────────────────────────────

def job_create(username_display: str) -> str:
    uname_norm = normalize_username(username_display)
    job_id = f"job_{uname_norm}_{int(utc_now().timestamp())}_{uuid4().hex[:6]}"
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("""
                INSERT INTO jobs(job_id, status, username_norm, username_display, created_at)
                VALUES (:job_id, 'queued', :un, :ud, :ca)
                ON CONFLICT (job_id) DO UPDATE SET
                  status='queued',
                  username_norm=EXCLUDED.username_norm,
                  username_display=EXCLUDED.username_display
            """), {"job_id": job_id, "un": uname_norm, "ud": username_display, "ca": utc_now()})
        return job_id
    db = mongo_get_db(); c = Collections()
    db[c.jobs].update_one(
        {"_id": job_id},
        {"$set": {"_id": job_id, "status": "queued",
                  "username_display": username_display,
                  "username_norm": uname_norm, "created_at": utc_now()}},
        upsert=True,
    )
    return job_id


def job_get(job_id: str) -> dict | None:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(text("SELECT * FROM jobs WHERE job_id=:id"), {"id": job_id}).mappings().first()
            return dict(row) if row else None
    db = mongo_get_db(); c = Collections()
    return db[c.jobs].find_one({"_id": job_id})


def job_patch(job_id: str, patch: dict[str, Any]) -> None:
    if Config.DB_BACKEND == "postgres":
        safe_patch = {k: v for k, v in patch.items() if k in _JOBS_VALID_COLUMNS}
        unknown = set(patch.keys()) - _JOBS_VALID_COLUMNS
        if unknown:
            import logging
            logging.getLogger("scrape_queue").warning(
                "job_patch: ignoring unknown column(s) %s for job %s", unknown, job_id)
        if not safe_patch:
            return
        cols = [f"{k} = :{k}" for k in safe_patch]
        params: dict[str, Any] = {"id": job_id, **safe_patch}
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text(f"UPDATE jobs SET {', '.join(cols)} WHERE job_id=:id"), params)
        return
    db = mongo_get_db(); c = Collections()
    db[c.jobs].update_one({"_id": job_id}, {"$set": patch}, upsert=True)


def job_cancel_request(job_id: str) -> None:
    if Config.DB_BACKEND == "postgres":
        job_patch(job_id, {"status": "cancel_requested"})
    else:
        db = mongo_get_db(); c = Collections()
        db[c.jobs].update_one({"_id": job_id}, {"$set": {"status": "cancel_requested"}}, upsert=False)


def job_is_cancel_requested(job_id: str) -> bool:
    j = job_get(job_id)
    return bool(j and j.get("status") in ("cancel_requested", "paused"))


def reset_stale_running_jobs() -> int:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            res = conn.execute(text(
                "UPDATE jobs SET status='queued', started_at=NULL, error='reset after restart' WHERE status='running'"
            ))
            return int(res.rowcount or 0)
    db = mongo_get_db(); c = Collections()
    res = db[c.jobs].update_many(
        {"status": "running"},
        {"$set": {"status": "queued", "started_at": None, "error": "reset after restart"}},
    )
    return int(res.modified_count)


# ──────────────────────────── Users cache ─────────────────────────────────────

def user_is_cached(username_display: str, newer_than: str, older_than: str,
                   title_only: int = 0) -> bool:
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(text("""
                SELECT 1 FROM users
                WHERE username_norm=:u AND title_only=:t
                  AND cached_newer_than=:n AND cached_older_than=:o
            """), {"u": uname_norm, "t": title_only, "n": newer_than, "o": older_than}).first()
            return row is not None
    db = mongo_get_db(); c = Collections()
    doc = db[c.users].find_one(
        {"username_norm": uname_norm, "title_only": title_only},
        projection={"_id": 0, "cached_range": 1},
    )
    if not doc:
        return False
    cr = doc.get("cached_range")
    return isinstance(cr, dict) and cr.get("newer_than") == newer_than and cr.get("older_than") == older_than


def user_mark_cached(username_display: str, newer_than: str, older_than: str,
                     last_scraped_at: datetime, title_only: int = 0,
                     display_name: str = "") -> None:
    uname_norm = normalize_username(username_display)
    display = display_name or username_display
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("""
                INSERT INTO users(username_norm, username_display,
                                  cached_newer_than, cached_older_than,
                                  last_scraped_at, title_only)
                VALUES (:u, :d, :n, :o, :t, :to)
                ON CONFLICT (username_norm, title_only) DO UPDATE SET
                  username_display  = EXCLUDED.username_display,
                  cached_newer_than = EXCLUDED.cached_newer_than,
                  cached_older_than = EXCLUDED.cached_older_than,
                  last_scraped_at   = EXCLUDED.last_scraped_at
            """), {"u": uname_norm, "d": display, "n": newer_than, "o": older_than,
                   "t": last_scraped_at, "to": title_only})
        return
    db = mongo_get_db(); c = Collections()
    db[c.users].update_one(
        {"username_norm": uname_norm, "title_only": title_only},
        {"$set": {
            "username_norm": uname_norm, "username_display": display,
            "cached_range": {"newer_than": newer_than, "older_than": older_than},
            "last_scraped_at": last_scraped_at, "title_only": title_only,
        }},
        upsert=True,
    )


def user_delete(username_display: str) -> dict:
    """Deletes ALL data for this username across BOTH title_only modes."""
    uname_norm = normalize_username(username_display)
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            r1 = conn.execute(text("DELETE FROM media WHERE username_norm=:u"), {"u": uname_norm})
            r2 = conn.execute(text("DELETE FROM users WHERE username_norm=:u"), {"u": uname_norm})
        return {"ok": True, "deleted_media": r1.rowcount or 0, "deleted_user_meta": r2.rowcount or 0}
    db = mongo_get_db(); c = Collections()
    mr = db[c.media].delete_many({"username_norm": uname_norm})
    ur = db[c.users].delete_many({"username_norm": uname_norm})
    return {"ok": True, "deleted_media": mr.deleted_count, "deleted_user_meta": ur.deleted_count}


# ──────────────────────────── Media write ─────────────────────────────────────

def media_upsert_many(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    total_inserted = 0
    num_batches = math.ceil(len(rows) / _UPSERT_BATCH_SIZE)
    batches = [rows[i * _UPSERT_BATCH_SIZE:(i + 1) * _UPSERT_BATCH_SIZE] for i in range(num_batches)]

    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        stmt = text("""
            INSERT INTO media(username_norm, username_display, post_date,
                              media_url, media_type, created_at, title_only)
            VALUES (:u, :d, :p, :url, :t, :c, :to)
            ON CONFLICT (username_norm, title_only, media_url) DO NOTHING
        """)
        for batch in batches:
            payload = []
            for r in batch:
                d = (r.get("username_display") or "").strip()
                payload.append({
                    "u": normalize_username(d), "d": d,
                    "p": r.get("post_date"), "url": r.get("media_url"),
                    "t": r.get("media_type"), "c": r.get("created_at"),
                    "to": int(r.get("title_only") or 0),
                })
            with eng.begin() as conn:
                res = conn.execute(stmt, payload)
                total_inserted += int(res.rowcount or 0)
        return total_inserted

    db = mongo_get_db(); c = Collections()
    from pymongo import UpdateOne
    for batch in batches:
        ops = []
        for r in batch:
            d = (r.get("username_display") or "").strip()
            un = normalize_username(d)
            to = int(r.get("title_only") or 0)
            ops.append(UpdateOne(
                {"username_norm": un, "title_only": to, "media_url": r.get("media_url")},
                {"$setOnInsert": {
                    "username_norm": un, "username_display": d,
                    "post_date": r.get("post_date"), "media_url": r.get("media_url"),
                    "media_type": r.get("media_type"), "created_at": r.get("created_at"),
                    "title_only": to,
                }},
                upsert=True,
            ))
        try:
            result = db[c.media].bulk_write(ops, ordered=False)
            total_inserted += int(result.upserted_count or 0)
        except Exception:
            pass
    return total_inserted


# ──────────────────────────── Media read ──────────────────────────────────────

def media_count(username_norms: list[str], *, media_type: str | None = None,
                year: str | None = None, title_only: int | None = None) -> int:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = ["username_norm = ANY(:u)"]
        params: dict[str, Any] = {"u": username_norms}
        if title_only is not None:
            where.append("title_only = :to"); params["to"] = title_only
        if media_type:
            where.append("media_type = :mt"); params["mt"] = media_type
        if year:
            where.append("post_date LIKE :y"); params["y"] = f"{year}%"
        with eng.begin() as conn:
            return int(conn.execute(
                text("SELECT COUNT(*) FROM media WHERE " + " AND ".join(where)), params
            ).scalar() or 0)
    db = mongo_get_db(); c = Collections()
    f: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if title_only is not None:
        f["title_only"] = title_only
    if media_type:
        f["media_type"] = media_type
    if year:
        f["post_date"] = {"$regex": f"^{year}"}
    return int(db[c.media].count_documents(f))


def media_count_per_user(username_norms: list[str],
                         title_only: int | None = None) -> dict[str, int]:
    if not username_norms:
        return {}
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = "username_norm = ANY(:u)"
        params: dict[str, Any] = {"u": username_norms}
        if title_only is not None:
            where += " AND title_only = :to"; params["to"] = title_only
        with eng.begin() as conn:
            rows = conn.execute(
                text(f"SELECT username_norm, COUNT(*)::bigint AS c FROM media WHERE {where} GROUP BY username_norm"),
                params,
            ).all()
        return {norm: int(cnt) for norm, cnt in rows}
    db = mongo_get_db(); c = Collections()
    match: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if title_only is not None:
        match["title_only"] = title_only
    result: dict[str, int] = {}
    for row in db[c.media].aggregate([
        {"$match": match}, {"$group": {"_id": "$username_norm", "c": {"$sum": 1}}}
    ]):
        result[row["_id"]] = int(row["c"])
    return result


def media_page(username_norms: list[str], *, media_type: str | None, year: str | None,
               page: int, ipp: int, title_only: int | None = None) -> list[dict]:
    skip = (page - 1) * ipp
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = ["username_norm = ANY(:u)"]
        params: dict[str, Any] = {"u": username_norms, "limit": ipp, "offset": skip}
        if title_only is not None:
            where.append("title_only = :to"); params["to"] = title_only
        if media_type:
            where.append("media_type = :mt"); params["mt"] = media_type
        if year:
            where.append("post_date LIKE :y"); params["y"] = f"{year}%"
        q = ("SELECT media_url, media_type, post_date FROM media WHERE "
             + " AND ".join(where)
             + " ORDER BY post_date DESC, id DESC LIMIT :limit OFFSET :offset")
        with eng.begin() as conn:
            rows = conn.execute(text(q), params).mappings().all()
            return [{"src": r["media_url"], "type": r["media_type"], "date": r["post_date"]} for r in rows]
    db = mongo_get_db(); c = Collections()
    f: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if title_only is not None:
        f["title_only"] = title_only
    if media_type:
        f["media_type"] = media_type
    if year:
        f["post_date"] = {"$regex": f"^{year}"}
    cur = (db[c.media].find(f, projection={"_id": 0, "media_url": 1, "media_type": 1, "post_date": 1})
           .sort("post_date", -1).skip(skip).limit(ipp))
    return [{"src": r.get("media_url", ""), "type": r.get("media_type", "images"),
             "date": r.get("post_date", "")} for r in cur]


def media_type_counts(username_norms: list[str],
                      title_only: int | None = None) -> dict[str, int]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = "username_norm = ANY(:u)"
        params: dict[str, Any] = {"u": username_norms}
        if title_only is not None:
            where += " AND title_only = :to"; params["to"] = title_only
        with eng.begin() as conn:
            rows = conn.execute(
                text(f"SELECT media_type, COUNT(*)::bigint AS c FROM media WHERE {where} GROUP BY media_type"),
                params,
            ).all()
        out = {"images": 0, "videos": 0, "gifs": 0}
        for mt, cnt in rows:
            if mt in out:
                out[mt] = int(cnt)
        return out
    db = mongo_get_db(); c = Collections()
    match: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if title_only is not None:
        match["title_only"] = title_only
    out = {"images": 0, "videos": 0, "gifs": 0}
    for row in db[c.media].aggregate([{"$match": match}, {"$group": {"_id": "$media_type", "c": {"$sum": 1}}}]):
        k = row["_id"] or "images"
        if k in out:
            out[k] = int(row["c"])
    return out


def media_year_counts(username_norms: list[str],
                      title_only: int | None = None) -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        where = "username_norm = ANY(:u)"
        params: dict[str, Any] = {"u": username_norms}
        if title_only is not None:
            where += " AND title_only = :to"; params["to"] = title_only
        with eng.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT SUBSTRING(post_date, 1, 4) AS y, COUNT(*)::bigint AS c
                FROM media WHERE {where} GROUP BY y ORDER BY y DESC
            """), params).all()
        return [{"year": y, "count": int(c)} for (y, c) in rows
                if isinstance(y, str) and len(y) == 4 and y.isdigit()]
    db = mongo_get_db(); c = Collections()
    match: dict[str, Any] = {"username_norm": {"$in": username_norms}}
    if title_only is not None:
        match["title_only"] = title_only
    out = []
    for row in db[c.media].aggregate([
        {"$match": match},
        {"$project": {"y": {"$substrBytes": ["$post_date", 0, 4]}}},
        {"$group": {"_id": "$y", "c": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
    ]):
        y = row["_id"]
        if isinstance(y, str) and len(y) == 4 and y.isdigit():
            out.append({"year": y, "count": int(row["c"])})
    return out


# ──────────────────────── Username listing ────────────────────────────────────

def get_all_usernames() -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(text("""
                SELECT u.username_display, u.username_norm,
                       COALESCE(u.title_only, 0) AS title_only,
                       COALESCE(m.cnt, 0)::bigint AS media_count
                FROM users u
                LEFT JOIN (
                    SELECT username_norm, title_only, COUNT(*) AS cnt
                    FROM media GROUP BY username_norm, title_only
                ) m ON u.username_norm = m.username_norm
                  AND COALESCE(u.title_only, 0) = COALESCE(m.title_only, 0)
                ORDER BY u.username_display, u.title_only
            """)).mappings().all()
        return [{
            "username_display": r["username_display"],
            "username_norm":    r["username_norm"],
            "media_count":      int(r["media_count"]),
            "title_only":       int(r["title_only"]),
        } for r in rows]
    db = mongo_get_db(); c = Collections()
    users = list(db[c.users].find(
        {}, projection={"_id": 0, "username_display": 1, "username_norm": 1, "title_only": 1}
    ))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        to   = int(u.get("title_only") or 0)
        cnt  = db[c.media].count_documents({"username_norm": norm, "title_only": to})
        result.append({
            "username_display": u.get("username_display", norm),
            "username_norm": norm, "media_count": cnt, "title_only": to,
        })
    result.sort(key=lambda x: (x.get("username_display", "").lower(), x["title_only"]))
    return result


def get_users_with_latest_date() -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(text("""
                SELECT u.username_display, u.username_norm,
                       COALESCE(u.title_only, 0) AS title_only,
                       MAX(m.post_date) AS latest_date
                FROM users u
                LEFT JOIN media m
                  ON u.username_norm = m.username_norm
                 AND COALESCE(u.title_only, 0) = COALESCE(m.title_only, 0)
                GROUP BY u.username_display, u.username_norm, u.title_only
                ORDER BY u.username_display, u.title_only
            """)).mappings().all()
        return [{
            "username_display": r["username_display"],
            "username_norm":    r["username_norm"],
            "title_only":       int(r["title_only"]),
            "latest_date":      r["latest_date"],
        } for r in rows]
    db = mongo_get_db(); c = Collections()
    users = list(db[c.users].find(
        {}, projection={"_id": 0, "username_display": 1, "username_norm": 1, "title_only": 1}
    ))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        to   = int(u.get("title_only") or 0)
        latest = db[c.media].find_one(
            {"username_norm": norm, "title_only": to},
            sort=[("post_date", -1)], projection={"post_date": 1}
        )
        result.append({
            "username_display": u.get("username_display", norm),
            "username_norm": norm, "title_only": to,
            "latest_date": latest["post_date"] if latest else None,
        })
    return result


def search_usernames(query: str) -> list[dict[str, Any]]:
    query = (query or "").strip().lower()
    if not query:
        return get_all_usernames()
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(text("""
                SELECT u.username_display, u.username_norm,
                       COALESCE(u.title_only, 0) AS title_only,
                       COALESCE(m.cnt, 0)::bigint AS media_count
                FROM users u
                LEFT JOIN (
                    SELECT username_norm, title_only, COUNT(*) AS cnt
                    FROM media GROUP BY username_norm, title_only
                ) m ON u.username_norm = m.username_norm
                  AND COALESCE(u.title_only, 0) = COALESCE(m.title_only, 0)
                WHERE u.username_norm ILIKE :q
                ORDER BY u.username_display, u.title_only
            """), {"q": f"%{query}%"}).mappings().all()
        return [{
            "username_display": r["username_display"],
            "username_norm":    r["username_norm"],
            "media_count":      int(r["media_count"]),
            "title_only":       int(r["title_only"]),
        } for r in rows]
    db = mongo_get_db(); c = Collections()
    import re as _re
    pattern = _re.compile(_re.escape(query), _re.IGNORECASE)
    users = list(db[c.users].find(
        {"username_norm": {"$regex": pattern}},
        projection={"_id": 0, "username_display": 1, "username_norm": 1, "title_only": 1}
    ))
    result = []
    for u in users:
        norm = u.get("username_norm", "")
        to   = int(u.get("title_only") or 0)
        cnt  = db[c.media].count_documents({"username_norm": norm, "title_only": to})
        result.append({
            "username_display": u.get("username_display", norm),
            "username_norm": norm, "media_count": cnt, "title_only": to,
        })
    result.sort(key=lambda x: (x.get("username_display", "").lower(), x["title_only"]))
    return result


def get_queued_jobs() -> list[dict[str, Any]]:
    if Config.DB_BACKEND == "postgres":
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(
                text("SELECT * FROM jobs WHERE status='queued' ORDER BY created_at ASC")
            ).mappings().all()
        return [dict(r) for r in rows]
    db = mongo_get_db(); c = Collections()
    return list(db[c.jobs].find({"status": "queued"}, sort=[("created_at", 1)]))
