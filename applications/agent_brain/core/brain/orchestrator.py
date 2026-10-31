import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from applications.agent_brain.ingest.scraper.models import ScrapeRequest
from applications.agent_brain.ingest.scraper.store import ScrapeStore
from applications.agent_brain.ingest.scraper.scraper import WebScraper
from superlative_times.superlative_times import SuperlativeTimes

from applications.agent_brain.core.settings import get
from applications.agent_brain.core.brain.store import BrainStore
from applications.agent_brain.core.brain.models import (
    Topic, Page, Artifact, TopicStatus, url_hash, content_hash,
)
from applications.agent_brain.core.brain.robots import RobotsChecker


class BrainOrchestrator:
    """Orchestrates scrape → ETL → document pipeline for each topic (site).

    Periodically scrapes configured sites, cleans the data via ETL,
    and organizes results into artifacts grouped by topic.

    Dual hash layer:
      * url_hash  - deterministic SHA-256 of the URL (page identity, 1 scrape/day)
      * content_hash - SHA-256 of page text (skip Qdrant upsert if unchanged)
    """

    SCRAPE_COOLDOWN = timedelta(hours=24)

    def __init__(self, brain_store: BrainStore, scrape_store: ScrapeStore,
                 scraper: WebScraper, index: SuperlativeTimes,
                 robots: RobotsChecker):
        self.brain = brain_store
        self.scrape_store = scrape_store
        self.scraper = scraper
        self.index = index
        self.robots = robots

    def list_topics(self) -> list[Topic]:
        topics = []
        try:
            for path in self.brain.list_files("topics"):
                if path.endswith(".json"):
                    data = self.brain.read_json(path)
                    topics.append(Topic(**data))
        except Exception:
            pass
        return topics

    def _recent_page_for_url(self, url: str) -> Optional[Page]:
        """Return the most recent page for a URL if scraped within the cooldown."""
        uh = url_hash(url)
        now = datetime.now(timezone.utc)
        best: Optional[Page] = None
        try:
            for path in self.brain.list_files("pages"):
                if not path.endswith(".json"):
                    continue
                data = self.brain.read_json(path)
                if data.get("url_hash") != uh:
                    continue
                page = Page(**data)
                if page.scraped_at:
                    scraped = page.scraped_at
                    if scraped.tzinfo is None:
                        scraped = scraped.replace(tzinfo=timezone.utc)
                    if now - scraped > self.SCRAPE_COOLDOWN:
                        continue
                if best is None or page.scraped_at > best.scraped_at:
                    best = page
        except Exception:
            pass
        return best

    async def register_topic(self, name: str, url: str,
                             scrape_interval_seconds: int | None = None,
                             crawl: bool = True,
                             max_pages: int = 20,
                             ignore_robots: bool = False) -> Topic:
        respect_robots = get("scraper.respect_robots_txt", True) and not ignore_robots
        allowed = self.robots.is_allowed(url) if respect_robots else True
        status = TopicStatus.ACTIVE if allowed else TopicStatus.BLOCKED
        interval = scrape_interval_seconds or get(
            "scraper.default_interval_seconds", 3600)
        topic = Topic(
            name=name,
            url=url,
            status=status,
            scrape_interval_seconds=interval,
            robots_txt_allowed=allowed,
            metadata={"crawl": crawl, "max_pages": max_pages,
                      "ignore_robots": ignore_robots},
        )
        self.brain.save_json(f"topics/{topic.topic_id}.json", topic.model_dump())
        return topic

    async def crawl_topic(self, topic: Topic) -> dict:
        """Crawl a topic site: discover job links and scrape each page."""
        meta = topic.metadata or {}
        respect_robots = get("scraper.respect_robots_txt", True) and not meta.get(
            "ignore_robots", False)
        allowed = self.robots.is_allowed(str(topic.url)) if respect_robots else True
        if not allowed:
            return {"topic": topic.name, "skipped": True, "reason": "robots.txt blocked"}
        crawl_enabled = meta.get("crawl", True)
        max_pages = int(meta.get("max_pages", 20))

        if not crawl_enabled:
            return await self.scrape_topic(topic)

        delay = self.robots.crawl_delay(str(topic.url))
        if delay:
            await asyncio.sleep(delay)

        records = await self.scraper.crawl(
            str(topic.url), max_pages=max_pages, job_only=True)

        results = []
        for record in records:
            if record.get("is_duplicate"):
                results.append({
                    "topic": topic.name,
                    "url": record["url"],
                    "skipped": True,
                    "reason": "duplicate content",
                })
                continue

            text = record.get("text") or ""
            ch = content_hash(text)

            uh = url_hash(record["url"])
            existing = self._recent_page_for_url(record["url"])
            if existing is not None and existing.content_hash == ch:
                results.append({
                    "topic": topic.name,
                    "url": record["url"],
                    "skipped": True,
                    "reason": "content unchanged",
                    "url_hash": uh,
                    "content_hash": ch,
                })
                continue

            page = Page(
                topic_id=topic.topic_id,
                url=record["url"],
                url_hash=uh,
                title=record.get("title"),
                content_hash=ch,
                text=text,
                stored_path=None,
            )
            self.brain.save_json(f"pages/{page.page_id}.json", page.model_dump())

            artifact = await self._run_etl(topic, page, record)
            results.append({
                "topic": topic.name,
                "page_id": page.page_id,
                "url": record["url"],
                "url_hash": uh,
                "content_hash": ch,
                "title": record.get("title"),
                "artifact": artifact,
            })

        topic.last_scraped = datetime.now(timezone.utc)
        self.brain.save_json(f"topics/{topic.topic_id}.json", topic.model_dump())

        return {
            "topic": topic.name,
            "pages_scraped": len(results),
            "results": results,
        }

    async def scrape_topic(self, topic: Topic) -> dict:
        """Scrape a single topic, store page data, run ETL + doc pipeline."""
        meta = topic.metadata or {}
        respect_robots = get("scraper.respect_robots_txt", True) and not meta.get(
            "ignore_robots", False)
        allowed = self.robots.is_allowed(str(topic.url)) if respect_robots else True
        if not allowed:
            return {"topic": topic.name, "skipped": True, "reason": "robots.txt blocked"}

        uh = url_hash(str(topic.url))
        recent = self._recent_page_for_url(str(topic.url))
        if recent is not None:
            return {
                "topic": topic.name,
                "skipped": True,
                "reason": "scraped within last 24 hours",
                "url_hash": uh,
                "page_id": recent.page_id,
            }

        delay = self.robots.crawl_delay(str(topic.url))
        if delay:
            await asyncio.sleep(delay)

        req = ScrapeRequest(
            url=topic.url,
            wait_until=get("scraper.wait_until", "networkidle"))
        result = await self.scraper.scrape(str(req.url), wait_until=req.wait_until)

        text = result.get("text") or ""
        ch = content_hash(text)

        # Content-change detection: if an existing page for this URL has the same
        # content hash, nothing changed — skip ETL and Qdrant upsert.
        existing = self._recent_page_for_url(str(topic.url))
        if existing is not None and existing.content_hash == ch:
            return {
                "topic": topic.name,
                "skipped": True,
                "reason": "content unchanged",
                "url_hash": uh,
                "content_hash": ch,
            }

        page = Page(
            topic_id=topic.topic_id,
            url=result["url"],
            url_hash=uh,
            title=result.get("title"),
            content_hash=ch,
            text=text,
            stored_path=result.get("stored_path"),
        )
        self.brain.save_json(f"pages/{page.page_id}.json", page.model_dump())

        topic.last_scraped = datetime.now(timezone.utc)
        self.brain.save_json(f"topics/{topic.topic_id}.json", topic.model_dump())

        artifact = await self._run_etl(topic, page, result)

        return {
            "topic": topic.name,
            "page_id": page.page_id,
            "url_hash": uh,
            "content_hash": ch,
            "status": result.get("status"),
            "title": result.get("title"),
            "is_duplicate": result.get("is_duplicate"),
            "similarity": result.get("similarity_to_existing"),
            "artifact": artifact,
        }

    async def _run_etl(self, topic: Topic, page: Page,
                       scrape_result: dict) -> dict | None:
        """Clean scraped content via ETL and store as artifact."""
        text = scrape_result.get("text") or ""
        if not text:
            return None

        lines = [l for l in text.splitlines() if l.strip()]
        rows = []
        for i, line in enumerate(lines):
            rows.append({
                "line_number": i + 1,
                "content": line,
                "topic": topic.name,
                "page_id": page.page_id,
            })

        csv_content = "line_number,content,topic,page_id\n"
        for r in rows:
            escaped = r["content"].replace('"', '""')
            csv_content += f'{r["line_number"]},"{escaped}",{r["topic"]},{r["page_id"]}\n'

        artifact_key = f"artifacts/{topic.topic_id}/{page.page_id}.csv"
        self.brain.save_bytes(artifact_key, csv_content.encode("utf-8"))

        artifact = Artifact(
            topic_id=topic.topic_id,
            page_id=page.page_id,
            kind="csv",
            stored_path=artifact_key,
            row_count=len(rows),
        )
        self.brain.save_json(f"artifacts/{artifact.artifact_id}.json", artifact.model_dump())
        return {
            "artifact_id": artifact.artifact_id,
            "kind": artifact.kind,
            "stored_path": artifact.stored_path,
            "row_count": artifact.row_count,
        }

    async def scrape_all_due(self) -> list[dict]:
        """Scrape all active topics that are due.

        Topics with crawl=True in metadata are crawled (discover job links)
        instead of single-page scraped.

        Per-topic errors are caught so one slow/blocked site doesn't
        prevent the rest from being scraped.
        """
        results = []
        for topic in self.list_topics():
            if topic.status != TopicStatus.ACTIVE:
                results.append({"topic": topic.name, "skipped": True,
                                "reason": f"status={topic.status.value}"})
                continue
            try:
                if (topic.metadata or {}).get("crawl", True):
                    results.append(await self.crawl_topic(topic))
                else:
                    results.append(await self.scrape_topic(topic))
            except Exception as exc:
                results.append({
                    "topic": topic.name,
                    "skipped": True,
                    "reason": f"scrape error: {exc}",
                })
        return results
