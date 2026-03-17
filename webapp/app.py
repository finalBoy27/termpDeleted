from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

import json

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

from .auth import check_credentials, login_user, logout_user, require_role
from .config import Config
from .storage import (
    init_storage,
    job_create,
    job_get,
    job_cancel_request,
    user_is_cached,
    user_delete,
    media_page,
    media_count,
    media_type_counts,
    media_year_counts,
)
from .scraper import scrape_user_to_mongo
from .utils import normalize_username, split_usernames

# reuse gallery generator exactly (same design)
from g2 import _CSS, clean_url  # type: ignore


app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = Config.SECRET_KEY

_executor = ThreadPoolExecutor(max_workers=2)


def _run_scrape_in_thread(username: str, job_id: str, newer_than: str, older_than: str, title_only: int) -> None:
    asyncio.run(
        scrape_user_to_mongo(
            username,
            job_id=job_id,
            newer_than=newer_than,
            older_than=older_than,
            title_only=title_only,
        )
    )


init_storage()


@app.get("/")
def home():
    return render_template("home.html")


@app.get("/logout")
def logout():
    logout_user()
    return redirect(url_for("home"))


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if check_credentials("admin", u, p):
            login_user("admin")
            return redirect(request.args.get("next") or url_for("admin_panel"))
        flash("Invalid admin credentials", "error")
    return render_template("login.html", role="admin", default_user=Config.ADMIN_USER)


@app.route("/client/login", methods=["GET", "POST"])
def client_login():
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if check_credentials("client", u, p):
            login_user("client")
            return redirect(request.args.get("next") or url_for("client_panel"))
        flash("Invalid client credentials", "error")
    return render_template("login.html", role="client", default_user=Config.CLIENT_USER)


@app.get("/admin")
@require_role("admin")
def admin_panel():
    # For postgres, recent jobs are shown via /admin/job/<id> JSON; keep table simple for now.
    # We'll fetch recent jobs from whichever backend using a lightweight query.
    recent_jobs = []
    if True:
        # postgres
        if True:
            try:
                from .config import Config
                if Config.DB_BACKEND == "postgres":
                    from .pg import get_engine
                    from sqlalchemy import text
                    eng = get_engine()
                    with eng.begin() as conn:
                        recent_jobs = [dict(r) for r in conn.execute(text("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 25")).mappings().all()]
                else:
                    from .db import get_db, Collections
                    db = get_db(); c = Collections()
                    recent_jobs = list(db[c.jobs].find({}, sort=[("created_at", -1)], limit=25))
            except Exception:
                recent_jobs = []
    return render_template(
        "admin.html",
        newer_than=Config.NEWER_THAN,
        older_than=Config.OLDER_THAN,
        recent_jobs=recent_jobs,
    )


@app.post("/admin/scrape")
@require_role("admin")
def admin_scrape():
    username = (request.form.get("username") or "").strip()
    force = request.form.get("force") == "1"
    title_only = 1 if (request.form.get("title_only") == "1") else 0
    newer_than = (request.form.get("newer_than") or Config.NEWER_THAN).strip()
    older_than = (request.form.get("older_than") or Config.OLDER_THAN).strip()
    if not username:
        flash("Username is required", "error")
        return redirect(url_for("admin_panel"))

    if (not force) and user_is_cached(username, newer_than, older_than):
        flash("Already cached in DB for selected range. Use Force to rescrape.", "info")
        return redirect(url_for("admin_panel"))

    job_id = job_create(username)
    _executor.submit(_run_scrape_in_thread, username, job_id, newer_than, older_than, title_only)
    flash(f"Scrape started. Job: {job_id}", "ok")
    return redirect(url_for("admin_panel"))


@app.get("/admin/job/<job_id>")
@require_role("admin")
def admin_job(job_id: str):
    j = job_get(job_id)
    if not j:
        return jsonify({"ok": False, "error": "job not found"}), 404
    if "_id" in j:
        j["_id"] = str(j["_id"])
    if "job_id" in j:
        j["_id"] = j["job_id"]
    return jsonify({"ok": True, "job": j})

@app.post("/admin/job/<job_id>/cancel")
@require_role("admin")
def admin_cancel_job(job_id: str):
    job_cancel_request(job_id)
    flash(f"Cancel requested for {job_id}", "ok")
    return redirect(url_for("admin_panel"))


@app.post("/admin/user/delete")
@require_role("admin")
def admin_delete_user():
    username = (request.form.get("username") or "").strip()
    if not username:
        flash("Username required to delete", "error")
        return redirect(url_for("admin_panel"))
    res = user_delete(username)
    flash(f"Deleted {res['deleted_media']} media items for {normalize_username(username)}", "ok")
    return redirect(url_for("admin_panel"))


@app.get("/client")
@require_role("client")
def client_panel():
    return render_template("client.html", newer_than=Config.NEWER_THAN, older_than=Config.OLDER_THAN)


def _load_media_for_username(username_display: str) -> dict:
    """
    Builds the same structure g2.create_html_gallery expects:
    media_by_date_per_username[username][type][date] = [url, ...]
    """
    db = get_db()
    c = Collections()
    uname_norm = normalize_username(username_display)
    media_by_date: dict[str, dict[str, dict[str, list[str]]]] = {
        username_display: {"images": {}, "videos": {}, "gifs": {}}
    }

    cursor = db[c.media].find(
        {"username_norm": uname_norm},
        projection={"_id": 0, "post_date": 1, "media_url": 1, "media_type": 1, "username_display": 1},
    )
    for row in cursor:
        date = row.get("post_date") or ""
        url = clean_url(row.get("media_url") or "")
        typ = row.get("media_type") or "images"
        if not (isinstance(date, str) and len(date) >= 10):
            continue
        if not url.startswith(("http://", "https://")):
            continue
        if typ not in ("images", "videos", "gifs"):
            typ = "images"
        bucket = media_by_date[username_display][typ].setdefault(date, [])
        bucket.append(url)
    return media_by_date


@app.get("/gallery")
@require_role("client")
def gallery():
    raw = (request.args.get("username") or "").strip()
    usernames = split_usernames(raw)
    if not usernames:
        flash("Enter a username to search.", "error")
        return redirect(url_for("client_panel"))

    # Fast gallery: server-side pagination via /api/media (no huge inline JSON)
    title = f"{', '.join(usernames)} — Media Gallery"
    return render_template(
        "gallery_shell.html",
        title=title,
        css=_CSS,
        usernames_json=json.dumps(usernames),
        title_json=json.dumps(title),
    )


@app.get("/api/media")
@require_role("client")
def api_media():
    raw = (request.args.get("usernames") or "").strip()
    usernames = split_usernames(raw)
    selected = (request.args.get("selected") or "").strip()
    media_type = (request.args.get("mediaType") or "all").strip()
    year = (request.args.get("year") or "all").strip()
    page = max(1, int(request.args.get("page") or 1))
    ipp = max(1, min(2000, int(request.args.get("ipp") or 200)))

    if not usernames:
        return jsonify({"ok": False, "error": "usernames required"}), 400

    # Determine which usernames are currently being viewed
    if selected:
        query_names = [selected]
    else:
        query_names = usernames

    norms = [normalize_username(u) for u in query_names]
    mt = media_type if media_type in ("images", "videos", "gifs") else None
    yr = year if (year != "all" and len(year) == 4 and year.isdigit()) else None
    total_filtered = media_count(norms, media_type=mt, year=yr)
    items = media_page(norms, media_type=mt, year=yr, page=page, ipp=ipp)
    items = [{"type": i["type"], "src": clean_url(i["src"]), "date": i["date"]} for i in items]

    # meta for buttons and year dropdown (computed on the full requested usernames list)
    all_norms = [normalize_username(u) for u in usernames]
    total_all = media_count(all_norms)
    type_counts = media_type_counts(all_norms)
    year_counts = media_year_counts(all_norms)

    # per-user counts (labels use the raw provided names)
    user_counts = []
    for u in usernames:
        n = normalize_username(u)
        cnt = media_count([n])
        user_counts.append({"label": u, "value": u, "count": cnt})

    return jsonify(
        {
            "ok": True,
            "items": items,
            "meta": {
                "requested_usernames": usernames,
                "selected": selected,
                "total": total_all,
                "total_filtered": total_filtered,
                "type_counts": type_counts,
                "year_counts": year_counts,
                "user_counts": user_counts,
            },
        }
    )


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}


if __name__ == "__main__":
    ensure_indexes()
    app.run(host="0.0.0.0", port=5000, debug=True)
