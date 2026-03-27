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

# ─── selectolax → bs4 → bare shim ─────────────────────────────────────────────
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
# CONSTANTS
# =============================================================================

# BASE_URL               = "https://desifakes.com"
BASE_URL               = "https://sexbaba.co"
INITIAL_SEARCH_ID      = "46509052"
DEFAULT_NEWER_THAN     = "2019"
DEFAULT_OLDER_THAN     = "2026"
TIMEOUT                = [8.0, 12.0, 20.0]
DELAY_BETWEEN_PAGES    = 0.25
MAX_CONCURRENT_THREADS = 20
MAX_RETRIES            = 3
RETRY_DELAY            = [1.0, 2.0, 3.0]

VALID_EXTS   = {"jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"}
EXCLUDE_PATS = {"/data/avatars/", "/data/assets/", "/data/addonflare/"}

# WAF bypass — rotate user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

# FIX: Gallery CSS inlined here so webapp_fastapi/main.py no longer needs
# `from g2 import _CSS` — removing the tight coupling to the CLI script.
_CSS = """
<style>
  *, *::before, *::after { box-sizing: border-box; }
  :root {
    --bg:      #0d0d0d;
    --surface: #1a1a2e;
    --card:    #16213e;
    --accent:  #0f3460;
    --blue:    #e94560;
    --text:    #e0e0e0;
    --muted:   #888;
    --radius:  10px;
  }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 16px; }
  h1   { text-align: center; font-size: 1.8rem; margin-bottom: 18px;
         background: linear-gradient(90deg,#e94560,#0f3460); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
  .stats-bar { text-align:center; margin-bottom:14px; color: var(--muted); font-size:.9rem; }
  .controls  { display:flex; flex-wrap:wrap; justify-content:center; gap:10px; margin-bottom:18px; }
  .filter-button, .media-type-select, .number-input {
    padding: 10px 18px; border-radius: var(--radius); border: 1px solid #333;
    background: var(--card); color: var(--text); font-size: 15px; cursor: pointer;
    transition: background .25s, border-color .25s;
  }
  .filter-button:hover { background: var(--accent); border-color: var(--blue); }
  .filter-button.active { background: var(--blue); border-color: var(--blue); color: #fff; font-weight: 600; }
  .number-input { width: 80px; text-align: center; }
  .pagination { display:flex; justify-content:center; flex-wrap:wrap; gap:6px; margin:16px 0; }
  .pagination-button {
    padding: 8px 16px; border-radius: var(--radius); border: 1px solid #333;
    background: var(--card); color: var(--text); font-size:14px; cursor:pointer;
    transition: background .2s, transform .1s;
  }
  .pagination-button:hover  { background: var(--accent); transform: scale(1.06); }
  .pagination-button.active { background: var(--blue); font-weight:700; border-color: var(--blue); }
  .pagination-button:disabled { opacity:.4; cursor:not-allowed; transform:none; }
  .masonry { display:flex; gap:10px; align-items:flex-start; }
  .column  { flex:1; display:flex; flex-direction:column; gap:10px; }
  .media-item { position:relative; border-radius: var(--radius); overflow:hidden;
                background: var(--card); border: 1px solid #2a2a4a; transition: transform .2s, box-shadow .2s; }
  .media-item:hover { transform: scale(1.015); box-shadow: 0 4px 24px rgba(233,69,96,.25); }
  .media-item img, .media-item video { width:100%; display:block; border-radius: var(--radius) var(--radius) 0 0; }
  .embed-wrap { width:100%; aspect-ratio:16/9; }
  .embed-wrap iframe { width:100%; height:100%; border:none; }
  .media-date { font-size:.72rem; color:var(--muted); padding:4px 8px; background: var(--surface); }
  @media (max-width:768px) {
    .masonry { flex-direction:column; }
    .filter-button, .media-type-select { font-size:13px; padding:8px 12px; }
    .number-input { width:65px; }
  }
</style>
"""


# =============================================================================
# UTILITIES
# =============================================================================

def _pw(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr, flush=True)


def clean_url(url: str) -> str:
    """
    FIX: also inlined here so webapp_fastapi/main.py can import it from
    webapp.scraper instead of from g2, removing the g2 import entirely.
    """
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

    for node in tree.css('span[data-s9e-mediaembed="sendvid"] iframe[src]'):
        src = node.attributes.get("src", "").strip()
        if src:
            src = ("https:" + src) if src.startswith("//") else src
            src = src.replace("/embed/", "/")
            urls.add(src)

    for m in re.findall(
        r'data-s9e-mediaembed-iframe=["\'][^"\']*["\']src["\'][,\s]*["\']([^"\']+sendvid\.com[^"\']+)["\']',
        html_content,
    ):
        sv = m.replace("\\/", "/")
        sv = ("https:" + sv) if sv.startswith("//") else sv
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    for m in re.findall(
        r'src=["\']((?://|https?://)sendvid\.com/(?:embed/)?[^"\']+)["\']',
        html_content,
    ):
        sv = ("https:" + m) if m.startswith("//") else m
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    for node in tree.css("*[src]"):
        src = node.attributes.get("src", "").strip()
        if src:
            src = src.replace("/vh/dli?", "/vh/dl?")
            urls.add(src)

    for node in tree.css("*[data-src]"):
        ds = node.attributes.get("data-src", "").strip()
        if ds:
            urls.add(ds)

    for node in tree.css("*[data-video]"):
        dv = node.attributes.get("data-video", "").strip()
        if dv:
            urls.add(dv)

    for node in tree.css("video, video source"):
        src = node.attributes.get("src", "").strip()
        if src:
            urls.add(src)

    for node in tree.css("*[style]"):
        style = node.attributes.get("style") or ""
        for m in re.findall(r"url\((.*?)\)", style):
            m = m.strip("\"' ")
            if m:
                urls.add(m)

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
# HTTP LAYER
# =============================================================================

def _make_client() -> httpx.AsyncClient:
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
    """
    FIX: Handles 429 (rate-limit) and 5xx server errors with explicit backoff
    and respects Retry-After headers, instead of silently failing after 3 tries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.get(url, timeout=TIMEOUT[min(attempt - 1, 2)])
            status = r.status_code

            # Rate-limited or transient server error: honour Retry-After and retry
            if status in (429, 502, 503, 504) and attempt < MAX_RETRIES:
                wait = int(r.headers.get("Retry-After", RETRY_DELAY[attempt - 1] * 3))
                _pw(f"HTTP {status} [{url[:60]}] — retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                await asyncio.sleep(wait)
                continue

            return {"ok": status == 200, "html": r.text, "final_url": str(r.url)}
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
# ARTICLE / THREAD PROCESSOR
# =============================================================================

def _article_matches(article, patterns: list) -> bool:
    # FIX: renamed local variable from `text` to `article_text` to prevent
    # accidentally shadowing any outer `text` name (e.g. sqlalchemy.text).
    try:
        article_text = article.text(separator=" ").strip().lower()
    except Exception:
        article_text = (article.html or "").lower()
    return any(p.search(article_text) for p in patterns)


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

            # FIX: use UTC-aware datetime instead of naive local datetime.now()
            post_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
# PUBLIC API
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
    title_only: int = 0,
) -> None:
    user_mark_cached(
        display_name or username,
        newer_than,
        older_than,
        last_scraped_at,
        title_only=title_only,
    )


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
        await asyncio.to_thread(job_patch, job_id, mapped)

    await job_update(
        {
            "status":           "running",
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

    newer_than_date = newer_than if "-" in newer_than else f"{newer_than}-01-01"
    older_than_date = older_than if "-" in older_than else f"{older_than}-12-31"

    current_url = _build_search_url_dates(
        INITIAL_SEARCH_ID, search_display, newer_than_date, older_than_date, title_only=title_only
    )
    batch_num = 0

    async with _make_client() as client:
        while current_url:
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
                    await asyncio.sleep(DELAY_BETWEEN_PAGES + random.uniform(0.5, 1.5))
                    continue

                articles = await process_threads_concurrent(threads, patterns, client)
                if articles:
                    matched_posts += sum(1 for a in articles if a.get("matched"))

                batch_rows: list[dict[str, Any]] = []
                for article in articles:
                    # FIX: UTC-aware fallback date
                    post_date = article.get("post_date") or _utc_now().strftime("%Y-%m-%d")
                    raw_html  = article.get("article_html", "")
                    media_urls = filter_media(extract_media_from_html(raw_html), seen_global)
                    for url in media_urls:
                        url = clean_url(url)
                        if not url.startswith(("http://", "https://")):
                            continue
                        batch_rows.append({
                            "username_display": username_display,
                            "post_date": post_date,
                            "media_url": url,
                            "media_type": classify_media(url),
                            "created_at": _utc_now(),
                            "title_only": title_only,      # <-- ADD THIS
                        })

                if batch_rows:
                    inserted += int(await asyncio.to_thread(media_upsert_many, batch_rows))

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
        title_only=title_only,         # <-- ADD THIS
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
