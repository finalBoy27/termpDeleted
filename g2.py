#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║   DesiFakes Scraper — Termux Edition  (fast, no-bloat)              ║
╚══════════════════════════════════════════════════════════════════════╝

── Termux one-time setup ────────────────────────────────────────────
  pkg update && pkg upgrade -y
  pkg install python python-pip libxml2 libxslt -y
  pip install aiosqlite httpx selectolax
  pip install h2       # optional — enables HTTP/2
  pip install orjson   # optional — faster JSON
  # If selectolax fails:  pip install lxml beautifulsoup4

Usage:  python scraper_termux.py
"""

# ─── stdlib ────────────────────────────────────────────────────────────────
import asyncio
import gc
import html as html_module
import json
import math
import re
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl, urlunparse

# ─── orjson → stdlib fallback ─────────────────────────────────────────────
try:
    import orjson as _orjson
    def _json_dumps(obj: object) -> str:
        return _orjson.dumps(obj).decode()
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False
    def _json_dumps(obj: object) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

# ─── selectolax → bs4 → bare shim ─────────────────────────────────────────
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

# ─── third-party ──────────────────────────────────────────────────────────
import aiosqlite
import httpx

# ─── HTTP/2 availability check ────────────────────────────────────────────
try:
    import h2  # noqa: F401
    _HTTP2 = True
except ImportError:
    _HTTP2 = False

# ─── minimal print helpers (no rich/loguru) ───────────────────────────────
def _pw(msg: str): print(f"[WARN] {msg}", file=sys.stderr, flush=True)
def _pe(msg: str): print(f"[ERR]  {msg}", file=sys.stderr, flush=True)

# ─── DIRECTORIES ──────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
OUTPUT_DIR   = BASE_DIR / "output"
SCRAPING_DIR = BASE_DIR / "Scraping"
OUTPUT_DIR.mkdir(exist_ok=True)
SCRAPING_DIR.mkdir(exist_ok=True)

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
BASE_URL              = "https://desifakes.com"
INITIAL_SEARCH_ID     = "46509052"
ORDER                 = "date"
DEFAULT_NEWER_THAN    = "2019"
DEFAULT_OLDER_THAN    = "2026"
TIMEOUT               = [8.0, 12.0, 20.0]
DELAY_BETWEEN_PAGES   = 0.25
MAX_CONCURRENT_THREADS= 20
MAX_RETRIES           = 3
RETRY_DELAY           = [1.0, 2.0, 3.0]
BATCH_SIZE_DB         = 10_000
MAX_FILE_SIZE_MB      = 200
MAX_PAGINATION_RANGE  = 150

VALID_EXTS    = {"jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"}
EXCLUDE_PATS  = {"/data/avatars/", "/data/assets/", "/data/addonflare/"}

# ─── UPLOAD HOSTS ─────────────────────────────────────────────────────────────
HOSTS: list[dict] = [
    {
        "name":  "HTML-Hosting-1",
        "url":   "https://html-hosting.tirev71676.workers.dev/api/upload",
        "field": "file",
        "type":  "html_hosting",
    },
    {
        "name":  "HTML-Hosting-2",
        "url":   "https://directproxy.tirev71676.workers.dev/api/upload",
        "field": "file",
        "type":  "html_hosting",
    },
    {
        "name":  "Litterbox (72h)",
        "url":   "https://litterbox.catbox.moe/resources/internals/api.php",
        "field": "fileToUpload",
        "type":  "catbox",
        "data":  {"reqtype": "fileupload", "time": "72h"},
    },
    {
        "name":  "Catbox (permanent)",
        "url":   "https://catbox.moe/user/api.php",
        "field": "fileToUpload",
        "type":  "catbox",
        "data":  {"reqtype": "fileupload"},
    },
]

# ─── UTILITIES ────────────────────────────────────────────────────────────────

def mem_mb() -> float:
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1_048_576
    except ImportError:
        return 0.0


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


def extract_search_id(url: str) -> str | None:
    m = re.search(r"/search/(\d+)/", url)
    return m.group(1) if m else None


def build_search_url(
    search_id: str,
    query: str,
    newer_than: str,
    older_than: str,
    page: int | None = None,
    older_than_ts: str | None = None,
    title_only: int = 0,
) -> str:
    base = f"{BASE_URL}/search/{search_id}/"
    params: dict = {"q": query, "o": ORDER}
    if older_than_ts:
        params["c[older_than]"] = older_than_ts
    else:
        params["c[newer_than]"] = f"{newer_than}-01-01"
        params["c[older_than]"] = f"{older_than}-12-31"
    if title_only == 1:
        params["c[title_only]"] = 1
    if page:
        params["page"] = page
    return f"{base}?{urlencode(params)}"


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


# ─── MEDIA EXTRACTION ────────────────────────────────────────────────────────

def extract_media_from_html(raw_html: str) -> list[str]:
    if not raw_html:
        return []

    html_content = html_module.unescape(raw_html)
    tree = HTMLParser(html_content)
    urls: set[str] = set()

    # ── sendvid via data-s9e-mediaembed-iframe JSON ──
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

    # ── sendvid via actual <iframe src> ──
    for node in tree.css('span[data-s9e-mediaembed="sendvid"] iframe[src]'):
        src = node.attributes.get("src", "").strip()
        if src:
            src = ("https:" + src) if src.startswith("//") else src
            src = src.replace("/embed/", "/")
            urls.add(src)

    # ── sendvid via regex on data attributes ──
    for m in re.findall(
        r'data-s9e-mediaembed-iframe=["\'][^"\']*["\']src["\'][,\s]*["\']([^"\']+sendvid\.com[^"\']+)["\']',
        html_content,
    ):
        sv = m.replace("\\/", "/")
        sv = ("https:" + sv) if sv.startswith("//") else sv
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    # ── sendvid in plain iframe src ──
    for m in re.findall(
        r'src=["\']((?://|https?://)sendvid\.com/(?:embed/)?[^"\']+)["\']',
        html_content,
    ):
        sv = ("https:" + m) if m.startswith("//") else m
        sv = sv.replace("/embed/", "/")
        urls.add(sv)

    # ── generic src attributes ──
    for node in tree.css("*[src]"):
        src = node.attributes.get("src", "").strip()
        if src:
            src = src.replace("/vh/dli?", "/vh/dl?")
            urls.add(src)

    # ── data-src ──
    for node in tree.css("*[data-src]"):
        ds = node.attributes.get("data-src", "").strip()
        if ds:
            urls.add(ds)

    # ── data-video ──
    for node in tree.css("*[data-video]"):
        dv = node.attributes.get("data-video", "").strip()
        if dv:
            urls.add(dv)

    # ── <video> / <source> ──
    for node in tree.css("video, video source"):
        src = node.attributes.get("src", "").strip()
        if src:
            urls.add(src)

    # ── CSS background-image ──
    for node in tree.css("*[style]"):
        style = node.attributes.get("style") or ""
        for m in re.findall(r"url\((.*?)\)", style):
            m = m.strip("\"' ")
            if m:
                urls.add(m)

    # ── bare https URLs in raw text ──
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


# ─── HTTP LAYER ───────────────────────────────────────────────────────────────

def _make_client(limits: httpx.Limits | None = None) -> httpx.AsyncClient:
    lim = limits or httpx.Limits(max_keepalive_connections=30, max_connections=100)
    return httpx.AsyncClient(
        limits=lim, http2=_HTTP2, follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/122.0.0.0 Mobile Safari/537.36"},
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


# ─── ARTICLE PROCESSOR ────────────────────────────────────────────────────────

def _article_matches(article, patterns: list) -> bool:
    try:
        text = article.text(separator=" ").strip().lower()
    except Exception:
        text = (article.html or "").lower()
    return any(p.search(text) for p in patterns)


async def process_thread(
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
        slug  = thread_m.group(1) if thread_m else ""
        tid   = thread_m.group(2) if thread_m else ""

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
    tasks = [process_thread(client, url, patterns, semaphore) for url in thread_urls]
    batches = await asyncio.gather(*tasks, return_exceptions=True)
    out: list[dict] = []
    for b in batches:
        if isinstance(b, Exception):
            _pw(f"Thread batch error: {b}")
            continue
        out.extend(b)
    return out


# ─── DATABASE ─────────────────────────────────────────────────────────────────

async def init_db(db: aiosqlite.Connection) -> None:
    await db.execute(
        """CREATE TABLE IF NOT EXISTS media (
            id         INTEGER PRIMARY KEY,
            username   TEXT    NOT NULL,
            post_date  TEXT    NOT NULL,
            media_url  TEXT    UNIQUE NOT NULL,
            media_type TEXT    NOT NULL
        )"""
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_user ON media(username)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_date ON media(post_date)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_type ON media(media_type)")
    await db.commit()


# ─── SCRAPE ONE USER ─────────────────────────────────────────────────────────

async def scrape_user(
    username: str,
    title_only: int,
    newer_than: str,
    older_than: str,
    db_path: str,
) -> int:
    print(f"  Scraping: {username!r}", flush=True)

    search_display = "+".join(username.split())
    tokens  = [t for t in re.split(r"[,\s]+", username) if t]
    phrase  = " ".join(tokens)
    patterns: list[re.Pattern] = []
    if phrase:
        patterns.append(re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE))
    for tok in tokens:
        patterns.append(re.compile(r"\b" + re.escape(tok) + r"\b", re.IGNORECASE))

    seen_global: set[str] = set()
    inserted_count = 0

    async with aiosqlite.connect(db_path) as db:
        await init_db(db)
        current_url = build_search_url(
            INITIAL_SEARCH_ID, search_display, newer_than, older_than, title_only=title_only
        )
        batch_num = 0

        async with _make_client() as client:
            while current_url:
                batch_num += 1
                resp = await fetch_page(client, current_url)
                if not resp["ok"]:
                    _pe(f"Batch {batch_num} failed: {current_url[:80]}")
                    break

                current_url    = resp["final_url"]
                total_pages    = get_total_pages(resp["html"])
                last_page_html = resp["html"]
                page_html      = resp["html"]

                for page_num in range(1, total_pages + 1):
                    if page_num > 1:
                        pg = await fetch_page(client, build_page_from_canonical(current_url, page_num))
                        if not pg["ok"]:
                            continue
                        page_html = last_page_html = pg["html"]

                    threads = extract_threads(page_html)
                    if not threads:
                        continue

                    articles = await process_threads_concurrent(threads, patterns, client)
                    rows: list[tuple] = []
                    for article in articles:
                        for url in filter_media(extract_media_from_html(article.get("article_html", "")), seen_global):
                            url = clean_url(url)
                            if url.startswith(("http://", "https://")):
                                rows.append((username, article["post_date"], url, classify_media(url)))

                    if rows:
                        await db.executemany(
                            "INSERT OR IGNORE INTO media (username, post_date, media_url, media_type) VALUES (?, ?, ?, ?)",
                            rows,
                        )
                        await db.commit()
                        inserted_count += len(rows)

                    print(f"\r  {username}: batch {batch_num}  page {page_num}/{total_pages}  media={inserted_count}  ",
                          end="", flush=True)
                    await asyncio.sleep(DELAY_BETWEEN_PAGES)

                older_url = find_view_older_link(last_page_html, title_only)
                if not older_url:
                    break
                redir = await fetch_page(client, older_url)
                if not redir["ok"]:
                    break
                current_url = redir["final_url"]

    print()  # newline after \r progress
    print(f"  done: {username!r}  media={inserted_count}", flush=True)
    return inserted_count


# ─── UPLOAD FUNCTIONS ────────────────────────────────────────────────────────

async def _upload_to_host(client, host, file_bytes, filename) -> tuple[str, str]:
    name, url, field = host["name"], host["url"], host["field"]
    try:
        r = await client.post(url, files={field: (filename, file_bytes, "text/html")},
                              data=host.get("data", {}), timeout=60.0)
        if r.status_code not in (200, 201):
            return name, f"HTTP {r.status_code}"
        if host.get("type") == "html_hosting":
            try:
                j = r.json()
            except Exception:
                return name, f"Bad JSON"
            return (name, str(j["url"])) if j.get("success") and j.get("url") else (name, f"Error: {j.get('error','?')}")
        t = r.text.strip()
        if t.startswith("https://"):
            if name.startswith("Litterbox") and "files.catbox.moe" in t:
                t = "https://litterbox.catbox.moe/" + t.split("/")[-1]
            return name, t
        return name, f"Bad response: {t[:80]}"
    except httpx.TimeoutException:
        return name, "Timeout"
    except Exception as exc:
        return name, f"Error: {exc}"


async def upload_html(file_path: Path, hosts: list[dict]) -> list[tuple[str, str]]:
    if not file_path.exists():
        return [(h["name"], "File not found") for h in hosts]
    file_bytes = file_path.read_bytes()
    print(f"  Uploading {file_path.name} ({len(file_bytes)/1_048_576:.1f} MB) to {len(hosts)} hosts...", flush=True)
    lim = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    async with httpx.AsyncClient(limits=lim, follow_redirects=True, timeout=90.0, http2=_HTTP2) as client:
        results = await asyncio.gather(*[_upload_to_host(client, h, file_bytes, file_path.name) for h in hosts],
                                       return_exceptions=True)
    return [(hosts[i]["name"], f"Error: {r}") if isinstance(r, Exception) else r for i, r in enumerate(results)]


# ─── HTML GALLERY GENERATOR ───────────────────────────────────────────────────

_JS = r"""
<script>
(function(){
  const mediaData     = __MEDIA_DATA__;
  const usernames     = __USERNAMES__;
  const yearCounts    = __YEAR_COUNTS__;
  const totalItems    = __TOTAL_ITEMS__;
  const defPerPage    = __DEF_PER_PAGE__;

  const masonry           = document.getElementById("masonry");
  const pagination        = document.getElementById("pagination");
  const mediaTypeSelect   = document.getElementById("mediaType");
  const yearSelect        = document.getElementById("yearSelect");
  const itemsPerUserInput = document.getElementById("itemsPerUser");
  const itemsPerPageInput = document.getElementById("itemsPerPage");
  const buttons           = document.querySelectorAll(".filter-button");

  let selectedUsername = "";
  let currentPage      = 1;

  // ── populate year dropdown ──────────────────────────────────────────────────
  yearSelect.innerHTML =
    '<option value="all">All (' +
    Object.values(yearCounts).reduce((a, b) => a + b, 0) +
    ")</option>";
  Object.keys(yearCounts)
    .sort((a, b) => b - a)
    .forEach(y => {
      const opt = document.createElement("option");
      opt.value = y;
      opt.textContent = y + " (" + yearCounts[y] + ")";
      yearSelect.appendChild(opt);
    });

  // ── helpers ─────────────────────────────────────────────────────────────────
  function getOrderedMedia(mediaType, ipu, ipp, page, yearFilter) {
    let allMedia = [];
    if (selectedUsername === "") {
      const mediaByUser = {};
      let maxRounds = 0;
      usernames.forEach(u => {
        let um = (mediaData[u] || []).slice();
        if (mediaType !== "all") um = um.filter(i => i.type === mediaType);
        if (yearFilter !== "all") um = um.filter(i => i.date.startsWith(yearFilter));
        um.sort((a, b) => (b.date > a.date ? 1 : -1));
        mediaByUser[u] = um;
        maxRounds = Math.max(maxRounds, Math.ceil(um.length / ipu));
      });
      for (let r = 0; r < maxRounds; r++) {
        usernames.forEach(u => {
          allMedia = allMedia.concat(mediaByUser[u].slice(r * ipu, r * ipu + ipu));
        });
      }
    } else {
      let um = (mediaData[selectedUsername] || []).slice();
      if (mediaType !== "all") um = um.filter(i => i.type === mediaType);
      if (yearFilter !== "all") um = um.filter(i => i.date.startsWith(yearFilter));
      um.sort((a, b) => (b.date > a.date ? 1 : -1));
      allMedia = um;
    }
    const start = (page - 1) * ipp;
    return { media: allMedia.slice(start, start + ipp), total: allMedia.length };
  }

  function updatePagination(total, ipp) {
    pagination.innerHTML = "";
    const totalPages = Math.ceil(total / ipp);
    if (totalPages <= 1) return;

    const maxBtns = 7;
    let s = Math.max(1, currentPage - Math.floor(maxBtns / 2));
    let e = Math.min(totalPages, s + maxBtns - 1);
    if (e - s + 1 < maxBtns) s = Math.max(1, e - maxBtns + 1);

    const mkBtn = (label, page, disabled, active) => {
      const b = document.createElement("button");
      b.className = "pagination-button" + (active ? " active" : "");
      b.textContent = label;
      b.disabled = disabled;
      if (!disabled) b.addEventListener("click", () => { currentPage = page; renderMedia(); });
      return b;
    };

    pagination.appendChild(mkBtn("«First", 1, currentPage === 1, false));
    pagination.appendChild(mkBtn("‹Prev",  currentPage - 1, currentPage === 1, false));
    for (let i = s; i <= e; i++)
      pagination.appendChild(mkBtn(i, i, false, i === currentPage));
    pagination.appendChild(mkBtn("Next›", currentPage + 1, currentPage === totalPages, false));
    pagination.appendChild(mkBtn("Last»", totalPages,      currentPage === totalPages, false));
  }

  function makeSendvidEmbed(url) {
    const el = document.createElement("div");
    el.className = "embed-wrap";
    const iframe = document.createElement("iframe");
    let embed = url;
    if (!embed.includes("/embed/")) embed = embed.replace("sendvid.com/", "sendvid.com/embed/");
    if (!embed.startsWith("http"))  embed = "https://" + embed.replace(/^\/\//, "");
    iframe.src             = embed;
    iframe.allowFullscreen = true;
    iframe.loading         = "lazy";
    iframe.setAttribute("data-vtype", "sendvid");
    el.appendChild(iframe);
    return el;
  }

  function makeVideo(url) {
    const el = document.createElement("video");
    el.src        = url;
    el.controls   = true;
    el.loading    = "lazy";
    el.preload    = "metadata";
    el.playsInline = true;
    el.setAttribute("data-vtype", "standard");
    el.onerror = () => el.remove();
    return el;
  }

  function makeImage(url, alt) {
    const el = document.createElement("img");
    el.src     = url;
    el.alt     = alt;
    el.loading = "lazy";
    el.onerror = () => el.remove();
    return el;
  }

  function renderMedia() {
    masonry.innerHTML = "";
    const mediaType  = mediaTypeSelect.value;
    const yearFilter = yearSelect.value;
    const ipu        = Math.max(1, parseInt(itemsPerUserInput.value) || 2);
    const ipp        = Math.max(1, parseInt(itemsPerPageInput.value) || defPerPage);
    const { media: allMedia, total } = getOrderedMedia(mediaType, ipu, ipp, currentPage, yearFilter);

    updatePagination(total, ipp);

    const cols    = 3;
    const columns = [];
    for (let c = 0; c < cols; c++) {
      const col = document.createElement("div");
      col.className = "column";
      masonry.appendChild(col);
      columns.push(col);
    }

    allMedia.forEach((item, idx) => {
      const row    = Math.floor(idx / cols);
      const col    = idx % cols;
      const actual = row % 2 === 0 ? col : cols - 1 - col;
      let el;
      if (item.type === "videos") {
        el = item.src.includes("sendvid.com") ? makeSendvidEmbed(item.src) : makeVideo(item.src);
      } else {
        el = makeImage(item.src, item.type);
      }
      const wrapper = document.createElement("div");
      wrapper.className = "media-item";
      wrapper.setAttribute("data-date", item.date);
      const lbl = document.createElement("div");
      lbl.className = "media-date";
      lbl.textContent = item.date;
      wrapper.appendChild(el);
      wrapper.appendChild(lbl);
      columns[actual].appendChild(wrapper);
    });

    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  // ── pause all other videos when one plays ────────────────────────────────
  document.addEventListener("play", e => {
    document.querySelectorAll("video").forEach(v => { if (v !== e.target) v.pause(); });
  }, true);

  // ── event bindings ─────────────────────────────────────────────────────────
  buttons.forEach(btn => {
    btn.addEventListener("click", () => {
      if (btn.classList.contains("active")) return;
      buttons.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      selectedUsername = btn.getAttribute("data-usernames");
      currentPage = 1;
      renderMedia();
    });
  });

  [mediaTypeSelect, yearSelect].forEach(el =>
    el.addEventListener("change", () => { currentPage = 1; renderMedia(); })
  );
  [itemsPerUserInput, itemsPerPageInput].forEach(el =>
    el.addEventListener("input", () => { currentPage = 1; renderMedia(); })
  );

  // ── initial render ─────────────────────────────────────────────────────────
  try { renderMedia(); }
  catch(e) {
    console.error("Initial render failed:", e);
    masonry.innerHTML = '<p style="color:red;text-align:center">Render error — open DevTools console</p>';
  }
})();
</script>
"""

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


def create_html_gallery(
    media_by_date_per_username: dict,
    usernames: list[str],
) -> str | None:
    usernames_str = ", ".join(usernames)
    title = f"{usernames_str} — Media Gallery"
    

    media_data: dict   = {}
    total_items        = 0
    media_counts: dict = {}
    type_counts        = {"images": 0, "videos": 0, "gifs": 0}

    for uname in usernames:
        user_bucket = media_by_date_per_username.get(
            uname, {"images": {}, "videos": {}, "gifs": {}}
        )
        media_list: list[dict] = []
        count = 0

        for mtype in ("images", "videos", "gifs"):
            for date in sorted(user_bucket[mtype], reverse=True):
                for url in user_bucket[mtype][date]:
                    url = clean_url(url)
                    if not url.startswith(("http://", "https://")):
                        continue
                    safe_src = url.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "").replace("\r", "")
                    media_list.append({"type": mtype, "src": safe_src, "date": date})
                    count += 1
                    type_counts[mtype] += 1

        media_list.sort(key=lambda x: x["date"], reverse=True)
        safe_key = uname.replace(" ", "_")
        media_data[safe_key] = media_list
        media_counts[uname]  = count
        total_items += count

    if total_items == 0:
        
        return None

    try:
        media_json = _json_dumps(media_data)
        json_mb    = len(media_json) / 1_048_576
        
        if json_mb > MAX_FILE_SIZE_MB:
            
            return None
    except Exception as exc:
        
        return None

    year_counts: dict[str, int] = {}
    for items in media_data.values():
        for item in items:
            y = item["date"][:4]
            year_counts[y] = year_counts.get(y, 0) + 1
    year_counts_json = _json_dumps(year_counts)

    unames_js    = _json_dumps([u.replace(" ", "_") for u in usernames])
    # Default items-per-page requested: 200 (user can change in UI)
    def_per_page = 200

    filter_btns = [
        '<button class="filter-button active" data-usernames="" '
        f'data-original-text="All">All ({total_items})</button>'
    ]
    for uname in usernames:
        safe  = html_module.escape(uname.replace(" ", "_"))
        label = html_module.escape(uname)
        count = media_counts[uname]
        filter_btns.append(
            f'<button class="filter-button" data-usernames="{safe}" '
            f'data-original-text="{label} ({count})">{label} ({count})</button>'
        )

    js_body = (
        _JS
        .replace("__MEDIA_DATA__",   media_json)
        .replace("__USERNAMES__",    unames_js)
        .replace("__YEAR_COUNTS__",  year_counts_json)
        .replace("__TOTAL_ITEMS__",  str(total_items))
        .replace("__DEF_PER_PAGE__", str(def_per_page))
    )

    html_out = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html_module.escape(title)}</title>
  {_CSS}
</head>
<body>
  <h1>{html_module.escape(title)}</h1>
  <div class="stats-bar">
    Total: {total_items} &nbsp;|&nbsp;
    Images: {type_counts['images']} &nbsp;|&nbsp;
    Videos: {type_counts['videos']} &nbsp;|&nbsp;
    GIFs: {type_counts['gifs']}
  </div>
  <div class="controls">
    <select id="mediaType" class="media-type-select">
      <option value="all">All ({total_items})</option>
      <option value="images">Images ({type_counts['images']})</option>
      <option value="videos">Videos ({type_counts['videos']})</option>
      <option value="gifs">GIFs ({type_counts['gifs']})</option>
    </select>
    <select id="yearSelect" class="media-type-select"><option value="all">All Years</option></select>
    <input type="number" id="itemsPerUser" class="number-input" min="1" value="2" title="Items per user per round">
    <input type="number" id="itemsPerPage" class="number-input" min="1" value="{def_per_page}" title="Items per page">
    {"".join(filter_btns)}
  </div>
  <div class="pagination" id="pagination"></div>
  <div class="masonry" id="masonry"></div>
  {js_body}
</body>
</html>"""

    size_mb = len(html_out) / 1_048_576
    
    return html_out


# ─── CONSOLE UI + MAIN ────────────────────────────────────────────────────

def _ask(prompt: str, default: str = "") -> str:
    try:
        val = input(f"{prompt} [{default}]: ").strip()
        return val if val else default
    except (EOFError, KeyboardInterrupt):
        print(); sys.exit(0)

def _ask_yn(prompt: str, default: bool = True) -> bool:
    d = "Y/n" if default else "y/N"
    try:
        val = input(f"{prompt} [{d}]: ").strip().lower()
        return default if not val else val.startswith("y")
    except (EOFError, KeyboardInterrupt):
        print(); sys.exit(0)


async def run(usernames, title_only, newer_than, older_than, do_upload) -> None:
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_path = str(SCRAPING_DIR / f"media_{ts}.db")
    print(f"DB: {db_path}\n", flush=True)

    total_scraped: dict[str, int] = {}
    for user in usernames:
        for attempt in range(1, 4):
            count = await scrape_user(user, title_only, newer_than, older_than, db_path)
            if count > 0:
                break
            if attempt < 3:
                print(f"  Retry {attempt}/2 for {user!r}...", flush=True)
                await asyncio.sleep(2)
        total_scraped[user] = count

    print("\n── Scrape Results ──")
    grand = 0
    for u, c in total_scraped.items():
        print(f"  {u}: {c}")
        grand += c
    print(f"  TOTAL: {grand}")

    if grand == 0:
        _pe("No media found."); _cleanup(db_path); return

    print("\nLoading media from database...", flush=True)
    media_by_date: dict = defaultdict(lambda: {"images": {}, "videos": {}, "gifs": {}})
    async with aiosqlite.connect(db_path) as db:
        cursor   = await db.execute("SELECT COUNT(*) FROM media")
        total_db = (await cursor.fetchone())[0]
        offset = 0
        while offset < total_db:
            cursor = await db.execute(
                "SELECT username, post_date, media_url, media_type FROM media LIMIT ? OFFSET ?",
                (BATCH_SIZE_DB, offset),
            )
            rows = await cursor.fetchall()
            for uname, date, url, typ in rows:
                url = clean_url(url)
                if not url.startswith(("http://", "https://")): continue
                if date not in media_by_date[uname][typ]: media_by_date[uname][typ][date] = []
                media_by_date[uname][typ][date].append(url)
            offset += BATCH_SIZE_DB

    try:
        html_content = create_html_gallery(media_by_date, usernames)
    except Exception as exc:
        _pe(f"Gallery build error: {exc}\n{traceback.format_exc()}")
        html_content = None
    del media_by_date

    if not html_content:
        _pe("Gallery build failed."); _cleanup(db_path); return

    safe_names  = "_".join(u.replace(" ", "_") for u in usernames)[:60]
    output_file = OUTPUT_DIR / f"{safe_names}_{ts}.html"
    output_file.write_text(html_content, encoding="utf-8")
    size_mb = output_file.stat().st_size / 1_048_576
    del html_content

    print(f"\nGallery saved: {output_file}  ({size_mb:.2f} MB)", flush=True)

    if do_upload:
        try:
            upload_results = await upload_html(output_file, HOSTS)
            print("\n── Upload Results ──")
            for name, result in upload_results:
                icon = "OK" if result.startswith("https://") else "FAIL"
                print(f"  [{icon}] {name}: {result}")
        except Exception as exc:
            _pe(f"Upload error: {exc}")

    _cleanup(db_path)


def _cleanup(db_path: str) -> None:
    try: Path(db_path).unlink(missing_ok=True)
    except Exception: pass
    try:
        d = Path(db_path).parent
        if d.is_dir() and not any(d.iterdir()): d.rmdir()
    except Exception: pass


def main() -> None:
    print("╔══════════════════════════════════════════╗")
    print("║  DesiFakes Scraper — Termux Edition      ║")
    print("╚══════════════════════════════════════════╝")
    print(f"  Parser:{_PARSER or 'shim'}  HTTP/2:{'yes' if _HTTP2 else 'no'}  orjson:{'yes' if _HAS_ORJSON else 'no'}\n")

    raw_users  = _ask("Usernames (comma-separated)", "tanu jain")
    usernames  = [u.strip() for u in raw_users.split(",") if u.strip()]
    title_only = 1 if _ask("Title-only search (0/1)", "0") == "1" else 0
    newer_than = _ask("Newer than year", DEFAULT_NEWER_THAN)
    older_than = _ask("Older than year", DEFAULT_OLDER_THAN)
    do_upload  = _ask_yn("Upload HTML to hosting services?", True)

    print(f"\n  Users : {', '.join(usernames)}")
    print(f"  Years : {newer_than}–{older_than}")
    print(f"  Upload: {'yes' if do_upload else 'no'}")
    if not _ask_yn("\nStart scraping?", True):
        print("Aborted."); return

    try:
        asyncio.run(run(usernames, title_only, newer_than, older_than, do_upload))
    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as exc:
        _pe(f"Fatal: {exc}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
