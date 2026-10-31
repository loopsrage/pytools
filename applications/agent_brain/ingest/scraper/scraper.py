import ast
import asyncio
import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from playwright.async_api import async_playwright

from superlative_times.superlative_times import SuperlativeTimes
from rapidfuzzlib.rapidfuzzlib import fuzz_one_against_choices

from applications.agent_brain.core.settings import get


def _clean_text(html: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_rss_text(text: str) -> str:
    """Clean RSS item text (may contain HTML entities and tags)."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"&#\d+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_rss(xml_text: str) -> list[dict]:
    """Parse an RSS feed into a list of job item dicts."""
    items: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
        # Handle both <channel><item> and <feed><entry> (Atom)
        channel = root.find("channel")
        if channel is not None:
            item_els = channel.findall("item")
        else:
            item_els = root.findall("entry")

        for el in item_els:
            def get_text(tag: str) -> str:
                child = el.find(tag)
                if child is not None and child.text:
                    return child.text.strip()
                return ""

            title = get_text("title")
            link = get_text("link")
            if not link:
                # Atom <link href="..."/>
                link_el = el.find("link")
                if link_el is not None:
                    link = link_el.get("href", "")
            description = _clean_rss_text(get_text("description") or get_text("summary") or get_text("content"))
            pub_date = get_text("pubDate") or get_text("published") or get_text("updated")
            guid = get_text("guid") or link
            category = [c.text for c in el.findall("category") if c.text]

            items.append({
                "title": title,
                "link": link,
                "description": description,
                "pub_date": pub_date,
                "guid": guid,
                "category": category,
            })
    except ET.ParseError:
        pass
    return items


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


_JOB_LINK_PATTERNS = [
    r"/job[s]?/",
    r"/career[s]?/",
    r"/position/",
    r"/role/",
    r"/opening/",
    r"/apply/",
    r"/join-us/",
    r"/work-here/",
    r"[?&]job_id=",
    r"[?&]position_id=",
    r"[?&]req_id=",
]

_JOB_LINK_RE = re.compile("|".join(_JOB_LINK_PATTERNS), re.I)


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def _is_job_link(url: str) -> bool:
    path = urlparse(url).path.lower()
    return bool(_JOB_LINK_RE.search(path))


def discover_links(html: str, base_url: str,
                   max_links: int = 50,
                   job_only: bool = True) -> list[str]:
    """Extract links from HTML, optionally filtering to job-related links."""
    links: list[str] = []
    seen: set[str] = set()

    for m in re.finditer(
        r'<a\s+[^>]*href\s*=\s*["\']([^"\']+)["\']',
        html,
        re.I,
    ):
        href = m.group(1)
        abs_url = _normalize_url(urljoin(base_url, href))
        parsed = urlparse(abs_url)
        if parsed.scheme not in ("http", "https"):
            continue
        if abs_url in seen:
            continue
        if job_only and not _is_job_link(abs_url):
            continue
        seen.add(abs_url)
        links.append(abs_url)
        if len(links) >= max_links:
            break

    return links


class WebScraper:
    def __init__(self, store, index: SuperlativeTimes,
                 headless: bool | None = None,
                 timeout_seconds: int | None = None,
                 user_agent: str | None = None):
        self.store = store
        self.index = index
        self._previous: list[dict] = []
        self.headless = headless if headless is not None else get(
            "scraper.headless", True)
        self.timeout_seconds = int(timeout_seconds or get(
            "scraper.timeout_seconds", 30)) * 1000
        self.user_agent = user_agent or get(
            "scraper.user_agent", "agent-brain-bot")
        self._load_previous()

    def _load_previous(self) -> None:
        try:
            files = self.store.list_files("scrapes")
        except Exception:
            return
        for path in files:
            if not path.endswith(".meta"):
                continue
            try:
                meta = ast.literal_eval(self.store.read_text(path))
            except Exception:
                continue
            txt_path = path.replace(".meta", ".txt")
            try:
                text = self.store.read_text(txt_path)
            except Exception:
                text = ""
            scrape_id = Path(path).stem.replace(".meta", "")
            self._previous.append(
                {
                    "scrape_id": scrape_id,
                    "url": meta.get("url", ""),
                    "text": text,
                    "hash": meta.get("hash", ""),
                    "title": meta.get("title"),
                    "created_at": meta.get("created_at", ""),
                    "is_duplicate": meta.get("is_duplicate", False),
                    "similarity": meta.get("similarity"),
                }
            )

    def _fetch_with_urllib(self, url: str, timeout: int = 15) -> dict | None:
        """Fetch a page using urllib (no JavaScript rendering). Fast and reliable."""
        try:
            req = Request(url, headers={"User-Agent": self.user_agent})
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                status_code = resp.status
                content_type = resp.headers.get("Content-Type", "")

            if "application/json" in content_type or url.endswith(".json"):
                import json as _json
                data = _json.loads(raw.decode("utf-8", errors="replace"))
                # Flatten JSON into searchable text
                text = _json.dumps(data, indent=2)[:50000]
                return {"html": text, "title": url.split("/")[-1] or "json",
                        "status_code": status_code, "is_json": True,
                        "rss_items": None}

            if ("application/xml" in content_type or "application/rss" in content_type
                    or "application/atom" in content_type or url.endswith((".xml", ".rss", ".atom"))):
                items = _parse_rss(raw.decode("utf-8", errors="replace"))
                text = "\n".join(
                    f"{i.get('title','')} | {i.get('link','')} | {i.get('description','')}"
                    for i in items
                )[:50000]
                title = items[0].get("title", "") if items else url.split("/")[-1] or "rss"
                return {"html": text, "title": title,
                        "status_code": status_code, "is_json": False,
                        "rss_items": items}

            html = raw.decode("utf-8", errors="replace")
            title_match = re.search(
                r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
            title = title_match.group(1).strip() if title_match else ""
            return {"html": html, "title": title,
                    "status_code": status_code, "is_json": False,
                    "rss_items": None}
        except (URLError, HTTPError, Exception):
            return None

    async def scrape(self, url: str, wait_until: str = "networkidle") -> dict:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=self.user_agent)
            try:
                page = await context.new_page()
                response = await page.goto(
                    url, wait_until=wait_until,
                    timeout=self.timeout_seconds)
                title = await page.title()
                html = await page.content()
                text = _clean_text(html)
                status_code = response.status if response else None
            except Exception as exc:
                await browser.close()
                raise exc
            await browser.close()

        content_hash = _content_hash(text)
        now = datetime.utcnow()
        self.index.set_times(now)

        is_duplicate = False
        similarity = None
        if self._previous:
            choices = [item["text"] for item in self._previous]
            scores = fuzz_one_against_choices(text, choices, threshold=0.0)
            if scores:
                best_match = max(scores.items(), key=lambda kv: kv[1])
                similarity = best_match[1] / 100.0
                if similarity >= 0.85:
                    is_duplicate = True

        record = {
            "scrape_id": None,  # set by caller
            "url": url,
            "text": text,
            "hash": content_hash,
            "title": title,
            "status_code": status_code,
            "created_at": now,
            "is_duplicate": is_duplicate,
            "similarity": similarity,
        }
        self._previous.append(record)
        return record

    async def crawl(self, seed_url: str, wait_until: str = "networkidle",
                    max_pages: int = 20,
                    job_only: bool = True) -> list[dict]:
        """Crawl a site starting from seed_url, discovering job links.

        Uses urllib (no JS rendering) for fast, reliable page fetching.
        Returns a list of scrape records, one per discovered page.
        """
        import asyncio as _asyncio

        records: list[dict] = []
        visited: set[str] = set()
        queue: list[str] = [seed_url]

        loop = _asyncio.get_event_loop()

        while queue and len(visited) < max_pages:
            url = queue.pop(0)
            norm = _normalize_url(url)
            if norm in visited:
                continue
            visited.add(norm)

            # Fetch via urllib in a thread executor to avoid blocking
            try:
                fetch_result = await loop.run_in_executor(
                    None, self._fetch_with_urllib, norm, 15)
            except Exception:
                continue

            if fetch_result is None:
                continue

            html = fetch_result["html"]
            title = fetch_result["title"]
            status_code = fetch_result["status_code"]

            text = _clean_text(html)

            content_hash = _content_hash(text)
            now = datetime.utcnow()
            self.index.set_times(now)

            is_duplicate = False
            similarity = None
            if self._previous:
                choices = [item["text"] for item in self._previous]
                scores = fuzz_one_against_choices(
                    text, choices, threshold=0.0)
                if scores:
                    best_match = max(
                        scores.items(), key=lambda kv: kv[1])
                    similarity = best_match[1] / 100.0
                    if similarity >= 0.85:
                        is_duplicate = True

            record = {
                "scrape_id": None,
                "url": norm,
                "text": text,
                "hash": content_hash,
                "title": title,
                "status_code": status_code,
                "created_at": now,
                "is_duplicate": is_duplicate,
                "similarity": similarity,
            }
            records.append(record)
            self._previous.append(record)

            # Discover job links from this page for the next round
            if len(visited) < max_pages and not fetch_result.get("is_json"):
                try:
                    new_links = discover_links(
                        html, norm, max_links=15,
                        job_only=job_only)
                    for link in new_links:
                        if link not in visited and link not in queue:
                            queue.append(link)
                except Exception:
                    pass

        return records
