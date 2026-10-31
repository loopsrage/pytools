import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams,
)

from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.helpers import (
    link_pipeline,
    new_controller,
    start_pipeline,
    stop_pipeline,
)
from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from service_controller.service_controller import ServiceController
from thread_safe.index import Index

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.ingest.doc_pipeline.chunk import chunk_text
from applications.agent_brain.ingest.doc_pipeline.embed import TfidfEmbedder
from applications.agent_brain.ingest.doc_pipeline.structured_extractor import (
    extract_structured_fields,
)
from applications.agent_brain.core.settings import get
from applications.agent_brain.core.brain.models import url_hash, content_hash


@dataclass
class QdrantIngestConfig:
    qdrant_path: str = "data/.qdrant_brain"
    qdrant_collection: str = "agent_brain_chunks"
    vector_size: int = 384
    scan_interval_seconds: float = 60.0
    chunk_size: int = 500
    chunk_overlap: int = 100
    brain_base_dir: str = "data/brain"
    user_agent: str = "agent-brain-bot"


class QdrantIngestPipeline:
    """Periodic FS-to-Qdrant ingestion pipeline.

    Architecture:
        PeriodicProducer (FS scan every N seconds)
            -> QueueController (chunk text via doc_pipeline.chunk)
            -> QueueController (embed via TF-IDF + upsert to Qdrant)

    Managed by ServiceController for clean start/stop lifecycle.
    """

    def __init__(self, config: QdrantIngestConfig | None = None):
        self.config = config or QdrantIngestConfig()
        self._client: Optional[QdrantClient] = None
        self._embedder = TfidfEmbedder()
        self._service: Optional[ServiceController] = None
        self._tg: Optional[asyncio.TaskGroup] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._producer: Optional[PeriodicProducer] = None
        self._chunk_queue: Optional[QueueController] = None
        self._qdrant_queue: Optional[QueueController] = None
        self._scanned_ids: set[str] = set()
        self._stats = {
            "scans": 0,
            "items_scanned": 0,
            "chunks_created": 0,
            "points_upserted": 0,
            "content_unchanged_skips": 0,
            "errors": 0,
        }

    # ------------------------------------------------------------------
    # Qdrant client
    # ------------------------------------------------------------------
    def _ensure_client(self) -> QdrantClient:
        if self._client is None:
            qdrant_path = Path(self.config.qdrant_path)
            qdrant_path.parent.mkdir(parents=True, exist_ok=True)
            self._client = QdrantClient(path=str(qdrant_path))
            self._ensure_collection()
        return self._client

    def _ensure_collection(self) -> None:
        client = self._ensure_client()
        try:
            client.get_collection(self.config.qdrant_collection)
        except Exception:
            client.create_collection(
                collection_name=self.config.qdrant_collection,
                vectors_config=VectorParams(
                    size=self.config.vector_size,
                    distance=Distance.COSINE,
                ),
            )

    # ------------------------------------------------------------------
    # FS scan action (called by PeriodicProducer)
    # ------------------------------------------------------------------
    def scan_brain_fs(self) -> Optional[list[QueueData]]:
        """Walk the brain filesystem and emit QueueData for each unscanned page/topic."""
        results: list[QueueData] = []
        brain_dir = Path(self.config.brain_base_dir)
        if not brain_dir.exists():
            return None

        # Scan topics
        topics_dir = brain_dir / "topics"
        if topics_dir.exists():
            for topic_file in sorted(topics_dir.glob("*.json")):
                try:
                    data = json.loads(topic_file.read_text())
                    topic_id = data.get("topic_id")
                    if not topic_id or topic_id in self._scanned_ids:
                        continue
                    self._scanned_ids.add(topic_id)
                    results.append(
                        QueueData(
                            type="topic",
                            topic_id=topic_id,
                            topic_name=data.get("name", ""),
                            url=data.get("url", ""),
                            text=f"Topic: {data.get('name', '')} | {data.get('url', '')}",
                            source_path=str(topic_file),
                        )
                    )
                    self._stats["items_scanned"] += 1
                except Exception:
                    self._stats["errors"] += 1
                    continue

        # Scan pages
        pages_dir = brain_dir / "pages"
        if pages_dir.exists():
            for page_file in sorted(pages_dir.glob("*.json")):
                try:
                    data = json.loads(page_file.read_text())
                    page_id = data.get("page_id")
                    if not page_id or page_id in self._scanned_ids:
                        continue
                    self._scanned_ids.add(page_id)
                    text = data.get("text") or ""
                    uh = data.get("url_hash") or url_hash(data.get("url", ""))
                    ch = data.get("content_hash") or content_hash(text)
                    results.append(
                        QueueData(
                            type="page",
                            page_id=page_id,
                            topic_id=data.get("topic_id"),
                            topic_name=data.get("topic_name", ""),
                            url=data.get("url", ""),
                            url_hash=uh,
                            content_hash=ch,
                            title=data.get("title", ""),
                            text=text,
                            source_path=str(page_file),
                        )
                    )
                    self._stats["items_scanned"] += 1
                except Exception:
                    self._stats["errors"] += 1
                    continue

        self._stats["scans"] += 1
        return results if results else None

    # ------------------------------------------------------------------
    # Chunker action (QueueController worker)
    # ------------------------------------------------------------------
    async def _chunk_action(self, qd: QueueData) -> Optional[QueueData]:
        text = qd.attribute("text") or ""
        if not text:
            qd.set_attribute("chunks", [])
            qd.set_attribute("chunk_count", 0)
            return qd

        chunks = chunk_text(
            text,
            size=self.config.chunk_size,
            overlap=self.config.chunk_overlap,
        )
        qd.set_attribute("chunks", chunks)
        qd.set_attribute("chunk_count", len(chunks))
        self._stats["chunks_created"] += len(chunks)
        return qd

    # ------------------------------------------------------------------
    # Qdrant upsert action (QueueController worker)
    # ------------------------------------------------------------------
    async def _qdrant_action(self, qd: QueueData) -> Optional[QueueData]:
        chunks = qd.attribute("chunks") or []
        if not chunks:
            return qd

        client = self._ensure_client()

        # Embed all chunks with TF-IDF
        vectors = self._embedder.embed_many(chunks)

        url = qd.attribute("url") or ""
        uh = qd.attribute("url_hash") or url_hash(url)
        ch = qd.attribute("content_hash") or content_hash(" ".join(chunks))

        # Extract structured job fields from the full page text for filtering
        full_text = qd.attribute("text") or ""
        structured = extract_structured_fields(full_text)

        # Content-change detection: scroll for existing points with this URL hash
        # and skip the upsert if the content hash is unchanged.
        try:
            scroll_result, _ = client.scroll(
                collection_name=self.config.qdrant_collection,
                scroll_filter=Filter(
                    must=[FieldCondition(key="url_hash", match=MatchValue(value=uh))]
                ),
                limit=1,
                with_payload=True,
            )
            if scroll_result and scroll_result[0].payload.get("content_hash") == ch:
                qd.set_attribute("qdrant_upserted", 0)
                qd.set_attribute("content_unchanged", True)
                self._stats["content_unchanged_skips"] += 1
                return qd
        except Exception:
            # Point may not exist yet; proceed with upsert.
            pass

        # Deterministic point IDs derived from the URL hash so re-scraping the
        # same URL upserts (replaces) rather than creating duplicates.
        point_structs: list[PointStruct] = []
        for i, (chunk, vec) in enumerate(zip(chunks, vectors)):
            vec_arr = np.asarray(vec, dtype=np.float64)
            if vec_arr.size < self.config.vector_size:
                padded = np.pad(
                    vec_arr, (0, self.config.vector_size - vec_arr.size)
                )
            else:
                padded = vec_arr[: self.config.vector_size]

            point_id = f"{uh}:{i}"
            point_structs.append(
                PointStruct(
                    id=hashlib.md5(point_id.encode("utf-8")).hexdigest(),
                    vector=padded.tolist(),
                    payload={
                        "type": qd.attribute("type"),
                        "topic_id": qd.attribute("topic_id"),
                        "page_id": qd.attribute("page_id"),
                        "url_hash": uh,
                        "content_hash": ch,
                        "title": qd.attribute("title"),
                        "url": qd.attribute("url"),
                        "chunk_index": i,
                        "text": chunk,
                        "source_path": qd.attribute("source_path"),
                        "salary_min_usd": structured.get("salary_min_usd"),
                        "location": structured.get("location"),
                        "remote": structured.get("remote"),
                        "drug_testing": structured.get("drug_testing"),
                        "portfolio_required": structured.get("portfolio_required"),
                    },
                )
            )

        if point_structs:
            client.upsert(
                collection_name=self.config.qdrant_collection,
                points=point_structs,
            )
            self._stats["points_upserted"] += len(point_structs)

        qd.set_attribute("qdrant_upserted", len(point_structs))
        qd.set_attribute("content_unchanged", False)
        return qd

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the periodic ingestion pipeline (FS -> chunks -> Qdrant)."""
        if self._service is not None:
            return  # already running

        # Build the pipeline: chunk queue -> qdrant queue
        self._chunk_queue = new_controller(
            identity="chunk_queue",
            action=self._chunk_action,
            worker_count=3,
        )
        self._qdrant_queue = new_controller(
            identity="qdrant_queue",
            action=self._qdrant_action,
            worker_count=2,
        )
        link_pipeline([self._chunk_queue, self._qdrant_queue])

        # Periodic producer scans the FS and enqueues to the chunk queue
        async def _enqueue_scan_results() -> list[Any] | None:
            items = self.scan_brain_fs()
            if not items:
                return None
            tasks = [
                self._chunk_queue.enqueue(it) for it in items
            ]
            if tasks:
                asyncio.get_event_loop().run_until_complete(
                    asyncio.gather(*tasks)
                )
            return items

        self._producer = PeriodicProducer(
            action=_enqueue_scan_results,
            queue=self._chunk_queue,
            interval=self.config.scan_interval_seconds,
            start_now=True,
        )

        # ServiceController manages lifecycle
        self._stop_event = asyncio.Event()
        self._service = ServiceController()
        self._service.controllers = [self._producer]
        self._service.queues = [self._chunk_queue, self._qdrant_queue]

        # Note: ServiceController.init expects a running TaskGroup.
        # Callers must wrap start() inside `async with asyncio.TaskGroup() as tg:`.
        # See QdrantIngestPipeline.start_async() for the async entry point.

    async def start_async(self) -> None:
        """Async entry point: starts the pipeline inside a TaskGroup."""
        if self._service is not None:
            return

        self._chunk_queue = new_controller(
            identity="chunk_queue",
            action=self._chunk_action,
            worker_count=3,
        )
        self._qdrant_queue = new_controller(
            identity="qdrant_queue",
            action=self._qdrant_action,
            worker_count=2,
        )
        link_pipeline([self._chunk_queue, self._qdrant_queue])

        async def _enqueue_scan_results() -> list[Any] | None:
            items = self.scan_brain_fs()
            if not items:
                return None
            await asyncio.gather(
                *[self._chunk_queue.enqueue(it) for it in items]
            )
            return items

        self._producer = PeriodicProducer(
            action=_enqueue_scan_results,
            queue=self._chunk_queue,
            interval=self.config.scan_interval_seconds,
            start_now=True,
        )

        self._stop_event = asyncio.Event()
        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()

        self._service = ServiceController()
        self._service.controllers = [self._producer]
        self._service.queues = [self._chunk_queue, self._qdrant_queue]
        self._service.init(self._stop_event, self._tg)

        # Ensure Qdrant client is initialized so search works even if
        # _qdrant_action hasn't run yet
        self._ensure_client()

    async def stop(self) -> None:
        """Stop the pipeline and clean up."""
        if self._stop_event is not None:
            self._stop_event.set()

        if self._service is not None:
            await self._service.close()

        if self._producer is not None:
            self._producer.close()

        if self._tg is not None:
            try:
                await self._tg.__aexit__(None, None, None)
            except Exception:
                pass

    def stats(self) -> dict:
        return dict(self._stats)
