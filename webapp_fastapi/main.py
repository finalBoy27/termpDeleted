from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.templating import Jinja2Templates

from webapp.auth import check_credentials
from webapp.config import Config
from webapp.scraper import scrape_user_to_mongo, _CSS, clean_url  # FIX: import from scraper, not from g2
from webapp.storage import (
    init_storage,
    reset_stale_running_jobs,
    job_create,
    job_get,
    job_patch,
    job_cancel_request,
    user_is_cached,
    user_delete,
    media_page,
    media_count,
    media_count_per_user,   # FIX: new batched per-user count function
    media_type_counts,
    media_year_counts,
    get_all_usernames,
    search_usernames,
    get_queued_jobs,
    get_users_with_latest_date,
)
from webapp.utils import normalize_username, split_usernames

import pathlib

_BASE = pathlib.Path(__file__).parent.parent

logger = logging.getLogger("scrape_queue")
logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=Config.SECRET_KEY)
templates = Jinja2Templates(directory=str(_BASE / "webapp" / "templates"))

static_dir = _BASE / "webapp" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ==================== Sequential Queue System ====================

class ScrapeQueue:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._running_job_id: str | None = None
        self._task: asyncio.Task | None = None

    async def enqueue(self, username: str, job_id: str, newer_than: str, older_than: str, title_only: int):
        await self._try_start_next()

    async def _try_start_next(self):
        async with self._lock:
            if self._running_job_id is not None:
                return
            queued = await asyncio.to_thread(get_queued_jobs)
            if not queued:
                return
            job = queued[0]
            jid = job.get("job_id") or str(job.get("_id", ""))
            self._running_job_id = jid
            self._task = asyncio.create_task(self._run_job(job))

    async def _run_job(self, job: dict):
        jid = job.get("job_id") or str(job.get("_id", ""))
        username = job.get("username_display") or job.get("username_norm") or ""
        newer = job.get("range_newer_than") or Config.NEWER_THAN
        older = job.get("range_older_than") or Config.OLDER_THAN
        title_only = int(job.get("title_only") or 0)
        logger.info(f"[QUEUE] Starting job {jid} for '{username}' ({newer} → {older}) title_only={title_only}")
        try:
            result = await scrape_user_to_mongo(
                username,
                job_id=jid,
                newer_than=newer,
                older_than=older,
                title_only=title_only,
            )
            logger.info(f"[QUEUE] Job {jid} completed: {result}")
        except Exception as e:
            logger.error(f"[QUEUE] Job {jid} EXCEPTION: {type(e).__name__}: {e}", exc_info=True)
            try:
                await asyncio.to_thread(
                    job_patch, jid,
                    {"status": "failed", "error": f"{type(e).__name__}: {str(e)[:450]}", "finished_at": datetime.now(timezone.utc)}
                )
            except Exception:
                pass
        finally:
            async with self._lock:
                self._running_job_id = None
                self._task = None
            await self._try_start_next()

    async def pause_job(self, job_id: str):
        j = await asyncio.to_thread(job_get, job_id)
        if not j:
            return
        status = j.get("status", "")
        if status in ("running", "queued"):
            await asyncio.to_thread(job_patch, job_id, {"status": "paused"})

    async def resume_job(self, job_id: str):
        j = await asyncio.to_thread(job_get, job_id)
        if not j:
            return
        if j.get("status") == "paused":
            await asyncio.to_thread(job_patch, job_id, {"status": "queued"})
            await self._try_start_next()


_queue = ScrapeQueue()


# ==================== Auto Updater Loop ====================

async def auto_update_loop():
    """
    Background task that runs at UTC midnight to update all users with a 1-day buffer.
    Uses UTC time consistently. Staggers job creation to avoid flooding the job table.
    Preserves the title_only setting from the user's last scrape.
    """
    while True:
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)
        next_midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=timezone.utc)
        sleep_seconds = (next_midnight - now).total_seconds()

        logger.info(f"[AUTO-UPDATE] Sleeping for {sleep_seconds:.0f} seconds until UTC midnight.")
        await asyncio.sleep(sleep_seconds)

        logger.info("[AUTO-UPDATE] UTC midnight reached! Queuing auto-updates for all cached users.")
        try:
            users_info = await asyncio.to_thread(get_users_with_latest_date)

            today_dt = datetime.now(timezone.utc)
            future_dt = today_dt + timedelta(days=1)
            older_than_str = future_dt.strftime("%Y-%m-%d")

            for u in users_info:
                uname = u["username_display"]
                latest_date = u.get("latest_date")
                # Preserve the title_only setting from when this user was last scraped
                title_only_val = int(u.get("title_only") or 0)

                if latest_date:
                    try:
                        latest_dt = datetime.strptime(latest_date[:10], "%Y-%m-%d")
                        past_dt = latest_dt - timedelta(days=1)
                        newer_than_str = past_dt.strftime("%Y-%m-%d")
                    except Exception:
                        newer_than_str = latest_date
                else:
                    newer_than_str = Config.NEWER_THAN

                job_id = await asyncio.to_thread(job_create, uname)
                await asyncio.to_thread(job_patch, job_id, {
                    "range_newer_than": newer_than_str,
                    "range_older_than": older_than_str,
                    "title_only": title_only_val,
                })
                logger.info(f"[AUTO-UPDATE] Queuing {uname} from {newer_than_str} to {older_than_str} title_only={title_only_val}")
                await _queue.enqueue(uname, job_id, newer_than_str, older_than_str, title_only_val)

                # Stagger to avoid flooding the job table with all users at once
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"[AUTO-UPDATE] Daily update error: {e}", exc_info=True)


@app.on_event("startup")
async def on_startup():
    init_storage()
    reset_count = await asyncio.to_thread(reset_stale_running_jobs)
    if reset_count:
        logger.info(f"[STARTUP] Reset {reset_count} stale 'running' job(s) back to 'queued'.")
    await _queue._try_start_next()
    asyncio.create_task(auto_update_loop())


# ==================== Auth helpers ====================

def _role(req: Request) -> str | None:
    r = req.session.get("role")
    return r if isinstance(r, str) else None


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def require_role(role: str, req: Request) -> Response | None:
    if _role(req) == role:
        return None
    next_url = str(req.url.path)
    if req.url.query:
        next_url += "?" + req.url.query
    if role == "admin":
        return _redirect(f"/admin/login?next={next_url}")
    return _redirect(f"/client/login?next={next_url}")


# Cap flash messages at 10 to prevent session cookie overflow
_FLASH_MAX = 10

def flash(req: Request, msg: str) -> None:
    req.session.setdefault("flashes", [])
    flashes = req.session["flashes"]
    if len(flashes) >= _FLASH_MAX:
        flashes.pop(0)
    flashes.append(msg)
    req.session["flashes"] = flashes


def pop_flashes(req: Request) -> list[str]:
    msgs = req.session.pop("flashes", [])
    return msgs if isinstance(msgs, list) else []


# ==================== Routes ====================

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/logout")
def logout(request: Request):
    request.session.pop("role", None)
    return _redirect("/")


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_get(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "role": "admin", "default_user": Config.ADMIN_USER, "messages": pop_flashes(request)},
    )


@app.post("/admin/login")
def admin_login_post(request: Request, username: str = Form(""), password: str = Form("")):
    if check_credentials("admin", username, password):
        request.session["role"] = "admin"
        return _redirect(request.query_params.get("next") or "/admin")
    flash(request, "Invalid admin credentials")
    return _redirect("/admin/login")


@app.get("/client/login", response_class=HTMLResponse)
def client_login_get(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "role": "client", "default_user": Config.CLIENT_USER, "messages": pop_flashes(request)},
    )


@app.post("/client/login")
def client_login_post(request: Request, username: str = Form(""), password: str = Form("")):
    if check_credentials("client", username, password):
        request.session["role"] = "client"
        return _redirect(request.query_params.get("next") or "/client")
    flash(request, "Invalid client credentials")
    return _redirect("/client/login")


# ==================== Admin ====================

@app.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request):
    gate = require_role("admin", request)
    if gate:
        return gate

    recent_jobs: list[dict[str, Any]] = []
    try:
        if Config.DB_BACKEND == "postgres":
            from webapp.pg import get_engine
            from sqlalchemy import text
            eng = get_engine()
            with eng.begin() as conn:
                recent_jobs = [dict(r) for r in conn.execute(text("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50")).mappings().all()]
                for j in recent_jobs:
                    j["_id"] = j.get("job_id", j.get("_id"))
        else:
            from webapp.db import get_db, Collections
            db = get_db()
            c = Collections()
            recent_jobs = list(db[c.jobs].find({}, sort=[("created_at", -1)], limit=50))
    except Exception:
        recent_jobs = []

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "newer_than": Config.NEWER_THAN,
            "older_than": Config.OLDER_THAN,
            "recent_jobs": recent_jobs,
            "messages": pop_flashes(request),
        },
    )


@app.post("/admin/scrape")
async def admin_scrape(
    request: Request,
    username: str = Form(""),
    force: str | None = Form(None),
    title_only: str | None = Form(None),
    newer_than: str = Form(""),
    older_than: str = Form(""),
):
    gate = require_role("admin", request)
    if gate:
        return gate

    raw = (username or "").strip()
    if not raw:
        flash(request, "Username is required")
        return _redirect("/admin")

    force_bool      = (force == "1")
    title_only_int  = 1 if (title_only == "1") else 0
    newer_than      = (newer_than or Config.NEWER_THAN).strip()
    older_than      = (older_than or Config.OLDER_THAN).strip()

    usernames    = split_usernames(raw)
    created_jobs = []
    skipped      = []

    for uname in usernames:
        uname = uname.strip()
        if not uname:
            continue
        # FIX: pass title_only to cache check — same username but different title_only
        # = different dataset, so it must NOT be considered cached.
        if (not force_bool) and user_is_cached(uname, newer_than, older_than, title_only=title_only_int):
            skipped.append(uname)
            continue
        job_id = job_create(uname)
        job_patch(job_id, {"range_newer_than": newer_than, "range_older_than": older_than, "title_only": title_only_int})
        created_jobs.append(job_id)

    if skipped:
        flash(request, f"Already cached (use Force): {', '.join(skipped)}")
    if created_jobs:
        flash(request, f"Queued {len(created_jobs)} job(s): {', '.join(created_jobs)}")
        await _queue.enqueue("", "", newer_than, older_than, title_only_int)
    elif not skipped:
        flash(request, "No valid usernames provided")

    return _redirect("/admin")


@app.get("/admin/job/{job_id}")
def admin_job(request: Request, job_id: str):
    gate = require_role("admin", request)
    if gate:
        return gate
    j = job_get(job_id)
    if not j:
        return JSONResponse({"ok": False, "error": "job not found"}, status_code=404)
    if "_id" not in j and "job_id" in j:
        j["_id"] = j["job_id"]
    for k, v in j.items():
        if isinstance(v, datetime):
            j[k] = v.isoformat()
    return JSONResponse({"ok": True, "job": j})


@app.get("/admin/jobs")
def admin_jobs_api(request: Request, page: int = 1, per_page: int = 10, status: str = "all"):
    gate = require_role("admin", request)
    if gate:
        return gate
    page     = max(1, page)
    per_page = max(1, min(100, per_page))
    offset   = (page - 1) * per_page
    status_filter = status if status != "all" else None

    recent_jobs: list[dict[str, Any]] = []
    total_count = 0
    try:
        if Config.DB_BACKEND == "postgres":
            from webapp.pg import get_engine
            from sqlalchemy import text
            eng = get_engine()
            with eng.begin() as conn:
                count_q      = "SELECT COUNT(*) FROM jobs"
                count_params: dict[str, Any] = {}
                if status_filter:
                    count_q += " WHERE status=:st"
                    count_params["st"] = status_filter
                total_count = int(conn.execute(text(count_q), count_params).scalar() or 0)

                q      = "SELECT * FROM jobs"
                params: dict[str, Any] = {"limit": per_page, "offset": offset}
                if status_filter:
                    q += " WHERE status=:st"
                    params["st"] = status_filter
                q += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
                recent_jobs = [dict(r) for r in conn.execute(text(q), params).mappings().all()]
                for j in recent_jobs:
                    j["_id"] = j.get("job_id", j.get("_id"))
        else:
            from webapp.db import get_db, Collections
            db = get_db()
            c  = Collections()
            filt = {"status": status_filter} if status_filter else {}
            total_count = db[c.jobs].count_documents(filt)
            recent_jobs = list(db[c.jobs].find(filt, sort=[("created_at", -1)], skip=offset, limit=per_page))
    except Exception:
        recent_jobs = []
        total_count = 0

    for j in recent_jobs:
        for k, v in list(j.items()):
            if isinstance(v, datetime):
                j[k] = v.isoformat()
        if "_id" in j and not isinstance(j["_id"], str):
            j["_id"] = str(j["_id"])

    import math
    total_pages = max(1, math.ceil(total_count / per_page))

    return JSONResponse({
        "ok": True,
        "jobs": recent_jobs,
        "page": page,
        "per_page": per_page,
        "total_count": total_count,
        "total_pages": total_pages,
    })


@app.post("/admin/job/{job_id}/cancel")
async def admin_cancel(request: Request, job_id: str):
    gate = require_role("admin", request)
    if gate:
        return gate
    job_cancel_request(job_id)
    flash(request, f"Cancel requested for {job_id}")
    return _redirect("/admin")


@app.post("/admin/job/{job_id}/pause")
async def admin_pause(request: Request, job_id: str):
    gate = require_role("admin", request)
    if gate:
        return gate
    await _queue.pause_job(job_id)
    flash(request, f"Pause requested for {job_id}. The job will stop at the next page boundary and can be resumed from the start.")
    return _redirect("/admin")


@app.post("/admin/job/{job_id}/resume")
async def admin_resume(request: Request, job_id: str):
    gate = require_role("admin", request)
    if gate:
        return gate
    await _queue.resume_job(job_id)
    flash(request, f"Resumed {job_id}")
    return _redirect("/admin")


@app.post("/admin/user/delete")
def admin_delete_user(request: Request, username: str = Form("")):
    gate = require_role("admin", request)
    if gate:
        return gate
    username = (username or "").strip()
    if not username:
        flash(request, "Username required to delete")
        return _redirect("/admin")
    res = user_delete(username)
    flash(request, f"Deleted {res['deleted_media']} media items for {normalize_username(username)}")
    return _redirect("/admin")


# ==================== Client ====================

@app.get("/client", response_class=HTMLResponse)
def client_panel(request: Request):
    gate = require_role("client", request)
    if gate:
        return gate
    return templates.TemplateResponse(
        "client.html",
        {"request": request, "newer_than": Config.NEWER_THAN, "older_than": Config.OLDER_THAN, "messages": pop_flashes(request)},
    )


@app.get("/api/usernames")
def api_usernames(request: Request, q: str = ""):
    gate = require_role("client", request)
    if gate:
        return gate
    q = (q or "").strip()
    if q:
        results = search_usernames(q)
    else:
        results = get_all_usernames()
    return JSONResponse({"ok": True, "usernames": results})


# ==================== Gallery ====================

@app.get("/gallery", response_class=HTMLResponse)
def gallery(request: Request, username: str = ""):
    gate = require_role("client", request)
    if gate:
        return gate
    raw = (username or "").strip()
    usernames = split_usernames(raw)
    if not usernames:
        flash(request, "Enter a username to search.")
        return _redirect("/client")

    # FIX: batch normalize then do a single per-user count query instead of N individual calls
    norms  = [normalize_username(u) for u in usernames]
    counts = media_count_per_user(norms)

    final_usernames = []
    for u, norm in zip(usernames, norms):
        if counts.get(norm, 0) > 0:
            final_usernames.append(u)
        else:
            matches = search_usernames(u)
            if matches:
                for m in matches:
                    if m["username_display"] not in final_usernames:
                        final_usernames.append(m["username_display"])

    if not final_usernames:
        flash(request, f"No results found for: {raw}")
        return _redirect("/client")

    title = f"{', '.join(final_usernames)} — Media Gallery"
    return templates.TemplateResponse(
        "gallery_shell.html",
        {"request": request, "title": title, "css": _CSS, "usernames_json": json.dumps(final_usernames), "title_json": json.dumps(title)},
    )


@app.get("/api/media")
def api_media(
    request: Request,
    usernames: str = "",
    selected: str = "",
    mediaType: str = "all",
    year: str = "all",
    page: int = 1,
    ipp: int = 200,
):
    gate = require_role("client", request)
    if gate:
        return gate

    usernames_list = split_usernames((usernames or "").strip())
    if not usernames_list:
        return JSONResponse({"ok": False, "error": "usernames required"}, status_code=400)

    page = max(1, int(page or 1))
    ipp  = max(1, min(2000, int(ipp or 200)))
    mt   = mediaType if mediaType in ("images", "videos", "gifs") else None
    yr   = year if (year != "all" and isinstance(year, str) and len(year) == 4 and year.isdigit()) else None

    query_names    = [selected] if (selected or "").strip() else usernames_list
    norms          = [normalize_username(u) for u in query_names]
    total_filtered = media_count(norms, media_type=mt, year=yr)
    items          = media_page(norms, media_type=mt, year=yr, page=page, ipp=ipp)
    items          = [{"type": i["type"], "src": clean_url(i["src"]), "date": i["date"]} for i in items]

    all_norms   = [normalize_username(u) for u in usernames_list]
    total_all   = media_count(all_norms)
    type_counts = media_type_counts(all_norms)
    year_counts = media_year_counts(all_norms)

    # FIX: use a single batched DB query for per-user counts instead of N individual calls
    per_user_counts = media_count_per_user(all_norms)
    user_counts = [
        {"label": u, "value": u, "count": per_user_counts.get(normalize_username(u), 0)}
        for u in usernames_list
    ]

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "meta": {
                "requested_usernames": usernames_list,
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
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}
