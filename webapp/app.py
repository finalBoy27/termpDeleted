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
    media_count_per_user,
    media_type_counts,
    media_year_counts,
)
from .scraper import scrape_user_to_mongo, _CSS, clean_url
from .utils import normalize_username, split_usernames


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
    recent_jobs = []
    try:
        if Config.DB_BACKEND == "postgres":
            from .pg import get_engine
            from sqlalchemy import text
            eng = get_engine()
            with eng.begin() as conn:
                recent_jobs = [dict(r) for r in conn.execute(
                    text("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 25")
                ).mappings().all()]
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
    username   = (request.form.get("username") or "").strip()
    force      = request.form.get("force") == "1"
    title_only = 1 if (request.form.get("title_only") == "1") else 0
    newer_than = (request.form.get("newer_than") or Config.NEWER_THAN).strip()
    older_than = (request.form.get("older_than") or Config.OLDER_THAN).strip()

    if not username:
        flash("Username is required", "error")
        return redirect(url_for("admin_panel"))

    # FIX BUG 1: pass title_only so cache check is per-dataset, not just per-username
    if (not force) and user_is_cached(username, newer_than, older_than, title_only=title_only):
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


@app.get("/gallery")
@require_role("client")
def gallery():
    raw        = (request.args.get("username") or "").strip()
    # FIX BUG 2: read title_only from query param so the correct dataset is opened
    title_only = int(request.args.get("title_only") or 0)
    to_filter: int | None = title_only if title_only in (0, 1) else None

    usernames = split_usernames(raw)
    if not usernames:
        flash("Enter a username to search.", "error")
        return redirect(url_for("client_panel"))

    title_label = " [T]" if to_filter == 1 else ""
    title = f"{', '.join(usernames)}{title_label} — Media Gallery"
    return render_template(
        "gallery_shell.html",
        title=title,
        css=_CSS,
        usernames_json=json.dumps(usernames),
        title_json=json.dumps(title),
        title_only_json=json.dumps(to_filter),   # passed to JS → fetchPage
    )


@app.get("/api/media")
@require_role("client")
def api_media():
    raw        = (request.args.get("usernames") or "").strip()
    usernames  = split_usernames(raw)
    selected   = (request.args.get("selected") or "").strip()
    media_type = (request.args.get("mediaType") or "all").strip()
    year       = (request.args.get("year") or "all").strip()
    page       = max(1, int(request.args.get("page") or 1))
    ipp        = max(1, min(2000, int(request.args.get("ipp") or 200)))
    # FIX BUG 3: read title_only so queries hit the correct separate dataset
    _to_raw    = request.args.get("title_only")
    to_filter: int | None = None
    if _to_raw is not None:
        try:
            _to_int = int(_to_raw)
            if _to_int in (0, 1):
                to_filter = _to_int
        except (ValueError, TypeError):
            pass

    if not usernames:
        return jsonify({"ok": False, "error": "usernames required"}), 400

    query_names = [selected] if selected else usernames
    norms       = [normalize_username(u) for u in query_names]
    mt          = media_type if media_type in ("images", "videos", "gifs") else None
    yr          = year if (year != "all" and len(year) == 4 and year.isdigit()) else None

    total_filtered = media_count(norms, media_type=mt, year=yr, title_only=to_filter)
    items          = media_page(norms, media_type=mt, year=yr, page=page, ipp=ipp, title_only=to_filter)
    items          = [{"type": i["type"], "src": clean_url(i["src"]), "date": i["date"]} for i in items]

    all_norms   = [normalize_username(u) for u in usernames]
    total_all   = media_count(all_norms, title_only=to_filter)
    type_counts = media_type_counts(all_norms, title_only=to_filter)
    year_counts = media_year_counts(all_norms, title_only=to_filter)

    per_user_counts = media_count_per_user(all_norms, title_only=to_filter)
    user_counts = [
        {"label": u, "value": u, "count": per_user_counts.get(normalize_username(u), 0)}
        for u in usernames
    ]

    return jsonify({
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
    })


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}


if __name__ == "__main__":
    from .db import ensure_indexes
    ensure_indexes()
    app.run(host="0.0.0.0", port=5000, debug=True)
