from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

# We reuse the proven parsing/extraction logic from g2.py
from g2 import (  # type: ignore
    _HTTP2,
    BASE_URL,
    DEFAULT_NEWER_THAN,
    DEFAULT_OLDER_THAN,
    DELAY_BETWEEN_PAGES,
    INITIAL_SEARCH_ID,
    MAX_CONCURRENT_THREADS,
    build_page_from_canonical,
    classify_media,
    clean_url,
    extract_media_from_html,
    extract_threads,
    fetch_page,
    filter_media,
    find_view_older_link,
    get_total_pages,
    process_threads_concurrent,
    _make_client,
)

from .config import Config
from .utils import normalize_username
from .storage import (
    utc_now as storage_utc_now,
    job_patch,
    job_is_cancel_requested,
    user_is_cached,
    user_mark_cached,
    job_create,
    job_get,
    user_delete,
    media_upsert_many,
)


def _compile_patterns(username: str) -> list[re.Pattern]:
    tokens = [t for t in re.split(r"[,\s]+", username) if t]
    phrase = " ".join(tokens)
    patterns: list[re.Pattern] = []
    if phrase:
        patterns.append(re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE))
    for tok in tokens:
        patterns.append(re.compile(r"\b" + re.escape(tok) + r"\b", re.IGNORECASE))
    return patterns


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _build_search_url_dates(search_id: str, query: str, newer_than_date: str, older_than_date: str, *, page: int | None = None, title_only: int = 0) -> str:
    """
    Build desifakes search URL with full dates (YYYY-MM-DD).
    g2.build_search_url only supports year boundaries; admin wants day/month/year selection.
    """
    base = f"{BASE_URL}/search/{search_id}/"
    params: dict[str, str | int] = {"q": query, "o": "date"}
    params["c[newer_than]"] = newer_than_date
    params["c[older_than]"] = older_than_date
    if title_only == 1:
        params["c[title_only]"] = 1
    if page:
        params["page"] = page
    return f"{base}?{urlencode(params)}"


def request_cancel(job_id: str) -> None:
    job_patch(job_id, {"status": "cancel_requested"})


def is_cancel_requested(job_id: str) -> bool:
    return job_is_cancel_requested(job_id)


def is_user_cached(username: str, newer_than: str, older_than: str) -> bool:
    """
    Simple cache rule: if we already have at least one item for the username and
    the stored meta says the year range matches, we skip by default.
    """
    return user_is_cached(username, newer_than, older_than)


def mark_user_cached(username: str, newer_than: str, older_than: str, last_scraped_at: datetime, *, display_name: str | None = None) -> None:
    user_mark_cached(display_name or username, newer_than, older_than, last_scraped_at)


async def scrape_user_to_mongo(
    username: str,
    *,
    title_only: int = 0,
    newer_than: str | None = None,
    older_than: str | None = None,
    job_id: str | None = None,
) -> dict:
    newer_than = newer_than or Config.NEWER_THAN or DEFAULT_NEWER_THAN
    older_than = older_than or Config.OLDER_THAN or DEFAULT_OLDER_THAN

    username_display = username.strip()
    uname_norm = normalize_username(username_display)

    patterns = _compile_patterns(username)
    search_display = "+".join(username.split())

    seen_global: set[str] = set()
    inserted = 0
    matched_posts = 0
    started_at = _utc_now()

    async def job_update(patch: dict) -> None:
        if not job_id:
            return
        mapped = dict(patch)
        if "range" in mapped and isinstance(mapped["range"], dict):
            r = mapped.pop("range")
            mapped["range_newer_than"] = r.get("newer_than")
            mapped["range_older_than"] = r.get("older_than")
        # job_patch can be sync (postgres/mongo), so do it off the event loop
        await asyncio.to_thread(job_patch, job_id, mapped)

    await job_update(
        {
            "status": "running",
            "username_display": username_display,
            "username_norm": uname_norm,
            "started_at": started_at,
            "inserted": 0,
            "matched_posts": 0,
            "page": None,
            "total_pages": None,
            "batch": 0,
            "error": None,
            "range": {"newer_than": newer_than, "older_than": older_than},
        }
    )

    # Accept either "YYYY" or "YYYY-MM-DD" for env/form.
    newer_than_date = newer_than if "-" in newer_than else f"{newer_than}-01-01"
    older_than_date = older_than if "-" in older_than else f"{older_than}-12-31"

    current_url = _build_search_url_dates(INITIAL_SEARCH_ID, search_display, newer_than_date, older_than_date, title_only=title_only)
    batch_num = 0

    async with _make_client() as client:
        while current_url:
            if job_id and is_cancel_requested(job_id):
                # Preserve 'paused' status if job was paused (don't overwrite to cancelled)
                _jcheck = await asyncio.to_thread(job_get, job_id)
                _jstatus = (_jcheck or {}).get("status", "")
                if _jstatus != "paused":
                    await job_update({"status": "cancelled", "finished_at": _utc_now()})
                return {"username": username_display, "status": _jstatus or "cancelled"}

            batch_num += 1
            await job_update({"batch": batch_num})

            resp = await fetch_page(client, current_url)
            if not resp["ok"]:
                await job_update({"status": "failed", "error": f"Batch failed: {current_url[:200]}"})
                break

            current_url = resp["final_url"]
            total_pages = get_total_pages(resp["html"])
            last_page_html = resp["html"]
            page_html = resp["html"]

            for page_num in range(1, total_pages + 1):
                if job_id and is_cancel_requested(job_id):
                    _jcheck2 = await asyncio.to_thread(job_get, job_id)
                    _jstatus2 = (_jcheck2 or {}).get("status", "")
                    if _jstatus2 != "paused":
                        await job_update({"status": "cancelled", "finished_at": _utc_now()})
                    return {"username": username_display, "status": _jstatus2 or "cancelled"}

                if page_num > 1:
                    pg = await fetch_page(client, build_page_from_canonical(current_url, page_num))
                    if not pg["ok"]:
                        continue
                    page_html = last_page_html = pg["html"]

                await job_update({"page": page_num, "total_pages": total_pages, "inserted": inserted})

                threads = extract_threads(page_html)
                if not threads:
                    await asyncio.sleep(DELAY_BETWEEN_PAGES)
                    continue

                articles = await process_threads_concurrent(threads, patterns, client)
                if articles:
                    matched_posts += sum(1 for a in articles if a.get("matched"))

                batch_rows: list[dict[str, Any]] = []
                for article in articles:
                    post_date = article.get("post_date") or datetime.now().strftime("%Y-%m-%d")
                    raw_html = article.get("article_html", "")
                    media_urls = filter_media(extract_media_from_html(raw_html), seen_global)
                    for url in media_urls:
                        url = clean_url(url)
                        if not url.startswith(("http://", "https://")):
                            continue
                        media_type = classify_media(url)
                        batch_rows.append(
                            {
                                "username_display": username_display,
                                "post_date": post_date,
                                "media_url": url,
                                "media_type": media_type,
                                "created_at": _utc_now(),
                            }
                        )

                if batch_rows:
                    # Bulk insert off the event loop (postgres uses sync engine)
                    inserted += int(await asyncio.to_thread(media_upsert_many, batch_rows))

                await asyncio.sleep(DELAY_BETWEEN_PAGES)

            older_url = find_view_older_link(last_page_html, title_only)
            if not older_url:
                break
            redir = await fetch_page(client, older_url)
            if not redir["ok"]:
                break
            current_url = redir["final_url"]

    finished_at = _utc_now()
    await asyncio.to_thread(mark_user_cached, username_display, newer_than, older_than, finished_at, display_name=username_display)
    await job_update({"status": "done", "finished_at": finished_at, "inserted": inserted, "matched_posts": matched_posts})
    return {
        "username": username_display,
        "status": "done",
        "inserted": inserted,
        "matched_posts": matched_posts,
        "started_at": started_at,
        "finished_at": finished_at,
        "cached_range": {"newer_than": newer_than, "older_than": older_than},
    }


def start_job(username: str) -> str:
    """
    Create a job doc and return its string _id (we use string ids for simplicity).
    """
    return job_create(username)


def delete_user_data(username: str) -> dict:
    return user_delete(username)


def get_job(job_id: str) -> dict | None:
    j = job_get(job_id)
    if not j:
        return None
    if "_id" not in j and "job_id" in j:
        j["_id"] = j["job_id"]
    return j
