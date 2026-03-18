from __future__ import annotations

# ─── stdlib ───────────────────────────────────────────────────────────────────
import asyncio
import json
import random
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl, urlunparse
from typing import Any

# ─── third-party ──────────────────────────────────────────────────────────────
import httpx

# ─── selectolax → bs4 → bare shim (mirrors g2.py fallback chain) ─────────────
try:
    from selectolax.parser import HTMLParser
    _PARSER = "selectolax"
except ImportError:
    _PARSER = None
    try:
        from bs4 import BeautifulSoup as _BS4
        _PARSER = "bs4"
    except ImportError:
        pass

    class HTMLParser:  # type: ignore[no-redef]
        def __init__(self, html: str):
            if _PARSER == "bs4":
                try:
                    self._soup = _BS4(html, "lxml")
                except Exception:
                    self._soup = _BS4(html, "html.parser")
            else:
                self._html = html
                self._soup = None
        def css_first(self, selector: str):
            if self._soup:
                el = self._soup.select_one(selector)
                return _BS4Node(el) if el else None
            return None
        def css(self, selector: str):
            return [_BS4Node(el) for el in self._soup.select(selector)] if self._soup else []
        @property
        def html(self) -> str:
            return str(self._soup) if self._soup else getattr(self, "_html", "")

    class _BS4Node:
        def __init__(self, tag):
            self._tag = tag
        @property
        def attributes(self) -> dict:
            return dict(self._tag.attrs) if self._tag else {}
        @property
        def html(self) -> str:
            return str(self._tag) if self._tag else ""
        def text(self, strip: bool = False) -> str:
            t = self._tag.get_text() if self._tag else ""
            return t.strip() if strip else t
        def css_first(self, selector: str):
            el = self._tag.select_one(selector) if self._tag else None
            return _BS4Node(el) if el else None
        def css(self, selector: str):
            return [_BS4Node(el) for el in (self._tag.select(selector) if self._tag else [])]

# ─── HTTP/2 availability check ────────────────────────────────────────────────
try:
    import h2  # noqa: F401
    _HTTP2 = True
except ImportError:
    _HTTP2 = False

# ─── webapp imports ───────────────────────────────────────────────────────────
from .config import Config
from .utils import normalize_username
from .storage import (
    job_patch,
    job_is_cancel_requested,
    user_is_cached,
    user_mark_cached,
    job_create,
    job_get,
    user_delete,
    media_upsert_many,
)


# =============================================================================
# CONSTANTS  (inlined from g2.py — no import needed)
# =============================================================================

BASE_URL            = "https://desifakes.com"
INITIAL_SEARCH_ID   = "46509052"
DEFAULT_NEWER_THAN  = "2019"
DEFAULT_OLDER_THAN  = "2026"
TIMEOUT             = [8.0, 12.0, 20.0]
DELAY_BETWEEN_PAGES = 0.25
MAX_CONCURRENT_THREADS = 20
MAX_RETRIES         = 3
RETRY_DELAY         = [1.0, 2.0, 3.0]

VALID_EXTS   = {"jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"}
EXCLUDE_PATS = {"/data/avatars/", "/data/assets/", "/data/addonflare/"}

# WAF bypass — rotate user agents to avoid IP ban / Cloudflare tracking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]


# =============================================================================
# UTILITIES  (inlined from g2.py)
# =============================================================================

def _pw(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr, flush=True)


def clean_url(url: str) -> str:
    url = url.strip()
    if url.startswith("%22") and url.endswith("%22"):
        url = url[3:-3]
    if url.startswith('"') and url.endswith('"'):
        url = url[1:-1]
    return url


def classify_media(url: str) -> str:
    low = url.lower()
    if "sendvid.com" in low:
        return "videos"
    if "vh/dl?url" in low:
        return "videos"
    if "vh/dli?" in low:
        return "images"
    if ".mp4" in low or ".mov" in low or ".avi" in low or ".mkv" in low or ".webm" in low:
        return "videos"
    if ".gif" in low:
        return "gifs"
    return "images"


def build_page_from_canonical(canonical_url: str, page: int) -> str:
    parsed = urlparse(canonical_url)
    params = dict(parse_qsl(parsed.query))
    if page == 1:
        params.pop("page", None)
    else:
        params["page"] = str(page)
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def find_view_older_link(html_str: str, title_only: int = 0) -> str | None:
    tree = HTMLParser(html_str)
    link_node = tree.css_first("div.block-footer a")
    if not link_node:
        for a in tree.css("a[href]"):
            href = a.attributes.get("href", "")
            if "/older" in href:
                link_node = a
                break
    if not link_node:
        return None
    href = link_node.attributes.get("href", "")
    if not href or "/older" not in href:
        return None
    full = urljoin(BASE_URL, href)
    if title_only == 1 and "title_only" not in full:
        full += ("&" if "?" in full else "?") + "c[title_only]=1"
    return full


def get_total_pages(html_str: str) -> int:
    tree = HTMLParser(html_str)
    nav = tree.css_first("ul.pageNav-main")
    if nav:
        pages = [
            int(a.text(strip=True))
            for a in nav.css("li.pageNav-page a")
            if a.text(strip=True).isdigit()
        ]
        if pages:
            return max(pages)
    simple = tree.css_first(".pageNavSimple-el--current")
    if simple:
        txt = simple.text(strip=True)
        m = re.search(r"of\s+(\d+)", txt, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return 1


def extract_threads(html_str: str) -> list[str]:
    tree = HTMLParser(html_str)
    seen: set[str] = set()
    result: list[str] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        if "threads/" in href and not href.startswith("#") and "page-" not in href:
            full = urljoin(BASE_URL, href)
            if full not in seen:
                seen.add(full)
                result.append(full)
    return result


def extract_media_from_html(raw_html: str) -> list[str]:
    import html as html_module
    if not raw_html:
        return []

    html_content = html_module.unescape(raw_html)
    tree = HTMLParser(html_content)
    urls: set[str] = set()

    # sendvid via data-s9e-mediaembed-iframe JSON
    for container in tree.css('span[data-s9e-mediaembed="sendvid"]'):
        for node in container.css("span[data-s9e-mediaembed-iframe]"):
            raw = node.attributes.get("data-s9e-mediaembed-iframe", "")
            if not raw:
                continue
            try:
                attrs = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                if isinstance(attrs, list) and "src" in attrs:
                    idx = attrs.index("src")
                    if idx + 1 < len(attrs):
                        src: str = attrs[idx + 1]
                        src = ("https:" + src) if src.startswith("//") else src
                        src = src.replace("/embed/", "/")
                        urls.add(src)
            except Exception:
                pass

    # sendvid via actual <iframe src>
    for node in tree.css('span[data-s9e-mediaembed="sendvid"] iframe[src]'):
        src = node.attributes.get("src", "").strip()
        if src:
            src = ("https:" + src) if src.startswith("//") else src
            src = src.replace("/embed/", "/")
            urls.add(src)

    # sendvid via regex on data attributes
    for m in re.findall(
        r'data-s9e-mediaembed-iframe=["\'][^"\']*["\']src["\'][,\s]*["\']([^"\']+sendvid\.com[^"\']+)["\']',
        html_content,
    ):
        sv = m.replace("\\/", "/")
        sv = ("https:" + sv) if sv.startswith("//") else sv
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    # sendvid in plain iframe src
    for m in re.findall(
        r'src=["\']((?://|https?://)sendvid\.com/(?:embed/)?[^"\']+)["\']',
        html_content,
    ):
        sv = ("https:" + m) if m.startswith("//") else m
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    # generic src attributes
    for node in tree.css("*[src]"):
        src = node.attributes.get("src", "").strip()
        if src:
            src = src.replace("/vh/dli?", "/vh/dl?")
            urls.add(src)

    # data-src
    for node in tree.css("*[data-src]"):
        ds = node.attributes.get("data-src", "").strip()
        if ds:
            urls.add(ds)

    # data-video
    for node in tree.css("*[data-video]"):
        dv = node.attributes.get("data-video", "").strip()
        if dv:
            urls.add(dv)

    # <video> / <source>
    for node in tree.css("video, video source"):
        src = node.attributes.get("src", "").strip()
        if src:
            urls.add(src)

    # CSS background-image
    for node in tree.css("*[style]"):
        style = node.attributes.get("style") or ""
        for m in re.findall(r"url\((.*?)\)", style):
            m = m.strip("\"' ")
            if m:
                urls.add(m)

    # bare https URLs in raw text
    for m in re.findall(r"https?://[^\s\"'<>]+", html_content):
        urls.add(m.strip())

    media: list[str] = []
    for u in urls:
        if not u:
            continue
        low = u.lower()
        if "sendvid.com" in low:
            media.append(u)
        elif ("encoded$" in low and ".mp4" in low) or any(
            f".{ext}" in low for ext in VALID_EXTS
        ):
            full = urljoin(BASE_URL, u) if u.startswith("/") else u
            media.append(full)

    return list(dict.fromkeys(media))


def filter_media(media_list: list[str], seen_global: set[str]) -> list[str]:
    out: list[str] = []
    seen_local: set[str] = set()
    for url in media_list:
        if any(bad in url for bad in EXCLUDE_PATS):
            continue
        if url not in seen_local and url not in seen_global:
            seen_local.add(url)
            seen_global.add(url)
            out.append(url)
    return out


# =============================================================================
# HTTP LAYER  (inlined from g2.py + WAF-bypass enhancements from scraper.py)
# =============================================================================

def _make_client() -> httpx.AsyncClient:
    """
    Secure async HTTP client with randomized User-Agent and WAF-bypass headers.
    Replaces both g2._make_client() and the old get_secure_client().
    """
    lim = httpx.Limits(max_keepalive_connections=20, max_connections=50)
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    return httpx.AsyncClient(
        limits=lim,
        http2=_HTTP2,
        follow_redirects=True,
        headers=headers,
        timeout=30.0,
    )


async def fetch_page(client: httpx.AsyncClient, url: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.get(url, timeout=TIMEOUT[min(attempt - 1, 2)])
            return {"ok": r.status_code == 200, "html": r.text, "final_url": str(r.url)}
        except Exception as exc:
            _pw(f"fetch {attempt}/{MAX_RETRIES} [{url[:60]}]: {exc}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY[attempt - 1])
    return {"ok": False, "html": "", "final_url": url}


async def make_request(client: httpx.AsyncClient, url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.get(url, timeout=TIMEOUT[min(attempt - 1, 2)])
            r.raise_for_status()
            return r.text
        except Exception as exc:
            _pw(f"request {attempt}/{MAX_RETRIES} [{url[:60]}]: {exc}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY[attempt - 1])
    return ""


# =============================================================================
# ARTICLE / THREAD PROCESSOR  (inlined from g2.py)
# =============================================================================

def _article_matches(article, patterns: list) -> bool:
    try:
        text = article.text(separator=" ").strip().lower()
    except Exception:
        text = (article.html or "").lower()
    return any(p.search(text) for p in patterns)


async def _process_thread(
    client: httpx.AsyncClient,
    post_url: str,
    patterns: list,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    async with semaphore:
        html_str = await make_request(client, post_url)
        if not html_str:
            return []

        tree = HTMLParser(html_str)
        articles = tree.css("article.message--post")
        results: list[dict] = []

        thread_m = re.search(r"/threads/([^/]+)\.(\d+)/?", post_url)
        slug = thread_m.group(1) if thread_m else ""
        tid  = thread_m.group(2) if thread_m else ""

        for article in articles:
            post_id = article.attributes.get("data-content", "").replace("post-", "").strip()
            if not post_id:
                continue

            post_url_full = (
                f"{BASE_URL}/threads/{slug}.{tid}/post-{post_id}"
                if thread_m
                else post_url
            )

            post_date = datetime.now().strftime("%Y-%m-%d")
            dt_tag = article.css_first("time.u-dt")
            if dt_tag and "datetime" in dt_tag.attributes:
                try:
                    post_date = datetime.strptime(
                        dt_tag.attributes["datetime"], "%Y-%m-%dT%H:%M:%S%z"
                    ).strftime("%Y-%m-%d")
                except Exception:
                    pass

            results.append(
                {
                    "url":          post_url_full,
                    "post_id":      post_id,
                    "matched":      _article_matches(article, patterns),
                    "post_date":    post_date,
                    "article_html": article.html,
                }
            )

        matched = [a for a in results if a["matched"]]
        return matched if matched else results


async def process_threads_concurrent(
    thread_urls: list[str],
    patterns: list,
    client: httpx.AsyncClient,
) -> list[dict]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_THREADS)
    tasks = [_process_thread(client, url, patterns, semaphore) for url in thread_urls]
    batches = await asyncio.gather(*tasks, return_exceptions=True)
    out: list[dict] = []
    for b in batches:
        if isinstance(b, Exception):
            _pw(f"Thread batch error: {b}")
            continue
        out.extend(b)
    return out


# =============================================================================
# SCRAPER INTERNALS
# =============================================================================

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


def _build_search_url_dates(
    search_id: str,
    query: str,
    newer_than_date: str,
    older_than_date: str,
    *,
    page: int | None = None,
    title_only: int = 0,
) -> str:
    """Build desifakes search URL with full YYYY-MM-DD date boundaries."""
    base = f"{BASE_URL}/search/{search_id}/"
    params: dict[str, str | int] = {"q": query, "o": "date"}
    params["c[newer_than]"] = newer_than_date
    params["c[older_than]"] = older_than_date
    if title_only == 1:
        params["c[title_only]"] = 1
    if page:
        params["page"] = page
    return f"{base}?{urlencode(params)}"


# =============================================================================
# PUBLIC API — thin wrappers used by storage / main
# =============================================================================

def request_cancel(job_id: str) -> None:
    job_patch(job_id, {"status": "cancel_requested"})


def is_cancel_requested(job_id: str) -> bool:
    return job_is_cancel_requested(job_id)


def is_user_cached(username: str, newer_than: str, older_than: str) -> bool:
    return user_is_cached(username, newer_than, older_than)


def mark_user_cached(
    username: str,
    newer_than: str,
    older_than: str,
    last_scraped_at: datetime,
    *,
    display_name: str | None = None,
) -> None:
    user_mark_cached(display_name or username, newer_than, older_than, last_scraped_at)


def start_job(username: str) -> str:
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


# =============================================================================
# MAIN SCRAPE COROUTINE
# =============================================================================

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
        # job_patch uses sync DB drivers; offload to thread pool to avoid blocking the event loop
        await asyncio.to_thread(job_patch, job_id, mapped)

    await job_update(
        {
            "status":        "running",
            "username_display": username_display,
            "username_norm":    uname_norm,
            "started_at":       started_at,
            "inserted":         0,
            "matched_posts":    0,
            "page":             None,
            "total_pages":      None,
            "batch":            0,
            "error":            None,
            "range": {"newer_than": newer_than, "older_than": older_than},
        }
    )

    # Accept either "YYYY" or "YYYY-MM-DD" for env/form input
    newer_than_date = newer_than if "-" in newer_than else f"{newer_than}-01-01"
    older_than_date = older_than if "-" in older_than else f"{older_than}-12-31"

    current_url = _build_search_url_dates(
        INITIAL_SEARCH_ID, search_display, newer_than_date, older_than_date, title_only=title_only
    )
    batch_num = 0

    async with _make_client() as client:
        while current_url:
            # ── cooperative cancel / pause check ──────────────────────────────
            if job_id and is_cancel_requested(job_id):
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

            current_url    = resp["final_url"]
            total_pages    = get_total_pages(resp["html"])
            last_page_html = resp["html"]
            page_html      = resp["html"]

            for page_num in range(1, total_pages + 1):
                # ── cooperative cancel / pause check inside page loop ─────────
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
                    # Anti-bot jitter on empty/error pages to avoid WAF detection
                    await asyncio.sleep(DELAY_BETWEEN_PAGES + random.uniform(0.5, 1.5))
                    continue

                # Process all threads concurrently (semaphore-limited)
                articles = await process_threads_concurrent(threads, patterns, client)
                if articles:
                    matched_posts += sum(1 for a in articles if a.get("matched"))

                batch_rows: list[dict[str, Any]] = []
                for article in articles:
                    post_date = article.get("post_date") or datetime.now().strftime("%Y-%m-%d")
                    raw_html  = article.get("article_html", "")
                    # O(1) dedup via seen_global set
                    media_urls = filter_media(extract_media_from_html(raw_html), seen_global)
                    for url in media_urls:
                        url = clean_url(url)
                        if not url.startswith(("http://", "https://")):
                            continue
                        batch_rows.append(
                            {
                                "username_display": username_display,
                                "post_date":        post_date,
                                "media_url":        url,
                                "media_type":       classify_media(url),
                                "created_at":       _utc_now(),
                            }
                        )

                if batch_rows:
                    # Bulk upsert off the event loop (storage uses sync drivers)
                    inserted += int(await asyncio.to_thread(media_upsert_many, batch_rows))

                # Anti-bot jitter — randomized human-like delay to bypass Cloudflare
                jitter = random.uniform(0.5, 2.0)
                await asyncio.sleep(DELAY_BETWEEN_PAGES + jitter)

            older_url = find_view_older_link(last_page_html, title_only)
            if not older_url:
                break

            redir = await fetch_page(client, older_url)
            if not redir["ok"]:
                break
            current_url = redir["final_url"]

    finished_at = _utc_now()
    await asyncio.to_thread(
        mark_user_cached,
        username_display, newer_than, older_than, finished_at,
        display_name=username_display,
    )
    await job_update(
        {
            "status":        "done",
            "finished_at":   finished_at,
            "inserted":      inserted,
            "matched_posts": matched_posts,
        }
    )

    return {
        "username":      username_display,
        "status":        "done",
        "inserted":      inserted,
        "matched_posts": matched_posts,
        "started_at":    started_at,
        "finished_at":   finished_at,
        "cached_range":  {"newer_than": newer_than, "older_than": older_than},
    }
