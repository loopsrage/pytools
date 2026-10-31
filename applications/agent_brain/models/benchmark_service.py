from __future__ import annotations

import hashlib
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any

from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

from benchmark_ledger import (
    BenchmarkLedger,
    BenchmarkQdrantDocument,
    BenchmarkSaveRequest,
    derive_friendly_model_title,
)
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.datasource_index.datasource_index import Datasource
from indexes.informational_index.fs_crud_service import FSBaseCrudService
from indexes.informational_index.informational_index import (
    INFORMATIONAL_NAMESPACE_UUID,
    _deterministic_384d_vector,
)
from service_controller.service_controller import ServiceController
from simple_functions.simple_functions import uuid5_from_args
from stats_collector.stats_collector import new_stats
from superlative_times.superlative_times import SuperlativeTimes
from thread_safe.index import Index

from applications.agent_brain.core.brain.qdrant_chunk_provider import (
    MODEL_DIMENSIONS,
    QDRANT_NAMESPACE_UUID,
    QdrantChunkClientProvider,
    chunk_document_text,
    encode_texts_cpu,
    rejoin_chunks,
)

_DEFAULT_BENCHMARK_LEDGER = (
    Path(__file__).resolve().parents[1] / "data" / "benchmark_history_ledger.json"
)
_CANONICAL_SEED_BENCHMARK_LEDGER = (
    Path(__file__).resolve().parent / "benchmark_history_ledger.json"
)


class NeuralBenchmarkService(ServiceController, Datasource):
    """
    Connects the neural_networks benchmark leaderboards, git commit history, and AST-extracted
    PyTorch model architectures to Qdrant vector storage (`portfolio_neural_benchmarks`) and
    the Agent Brain control panel.
    """

    def __init__(
        self,
        qdrant_graph: QdrantChunkClientProvider,
        repo_path: str | Path = "/Users/jarek/IdeaProjects/neural_networks",
        ledger_path: str | Path = _DEFAULT_BENCHMARK_LEDGER,
        collection_name: str = "portfolio_neural_benchmarks",
        watch_files: bool = True,
        refresh_interval_seconds: int = 30,
        staging_root: str | Path | None = None,
    ) -> None:
        super().__init__()
        self.controllers = []
        self.queues = []
        self.qdrant_graph = qdrant_graph
        self.watch_files = bool(watch_files)
        self.refresh_interval_seconds = max(0, int(refresh_interval_seconds))
        self.repo_path = Path(repo_path).resolve()
        self.promoted_dir = (self.repo_path / "promoted").resolve()
        self.staging_root = (
            Path(staging_root).resolve()
            if staging_root is not None
            else (self.repo_path.parent / "qdrant_staging").resolve()
        )
        self.ledger_path = Path(ledger_path)
        if (
            not self.ledger_path.exists()
            and self.ledger_path.resolve() == _DEFAULT_BENCHMARK_LEDGER.resolve()
            and _CANONICAL_SEED_BENCHMARK_LEDGER.is_file()
        ):
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(_CANONICAL_SEED_BENCHMARK_LEDGER, self.ledger_path)
        self.collection_name = collection_name
        self.conversations_collection_name = "promoted_model_conversations"

        # Solidify promoted path and staging workspace with FSBase
        self.promoted_fs = FSBase("file")
        self.promoted_fs.make_dirs(str(self.promoted_dir), exist_ok=True)
        if self.promoted_dir.is_dir():
            self.promoted_fs.index(str(self.promoted_dir))

        self.fs_crud = FSBaseCrudService(
            promoted_root=self.promoted_dir,
            staging_root=self.staging_root,
            idea_projects_root=self.repo_path.parent,
        )

        self.ledger = BenchmarkLedger(repo_path=self.repo_path, ledger_path=self.ledger_path)
        self._lock = threading.RLock()
        self._index = Index()
        self._index.new("DOCUMENTS")
        self._index.new("CHUNKS_BY_DOC")
        self._index.new("STATE")

        self.stats = new_stats()
        self.times = SuperlativeTimes()
        self._initialized = False
        self._last_source_sig: str = ""
        self._last_sig_check_mono: float = 0.0
        self._qdrant_state_generation: int = 0
        self._doc_hashes: dict[str, str] = {}
        self._promoted_harness: Any = None
        self._promoted_harness_class: str = ""
        self._sample_gen_cache: tuple[Any, dict[str, Any], str] | None = None
        self._nn_corpus_record_cache: dict[tuple[str, int, str], dict[str, Any] | None] = {}
        self._selected_chat_model_id: str = ""
        self._selected_chat_model_meta: dict[str, Any] = {}
        self._active_conversation_id: str = "promoted_model_harness"
        self._conversations_by_id: dict[str, dict[str, Any]] = {}
        self._promoted_chat_messages: list[dict[str, Any]] = []
        self._load_conversations_from_staging_qdrant()

    @property
    def is_watcher_active(self) -> bool:
        return bool(
            self.watch_files
            and not os.environ.get("K_SERVICE")
            and self.refresh_interval_seconds > 0
        )

    def ensure_indexed(self, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if self._initialized and not force:
                cached = self._index.load_from_index("STATE", "STATUS")
                if cached is not None:
                    if not self.is_watcher_active:
                        return cached
                    now_mono = time.monotonic()
                    if (
                        self._last_sig_check_mono > 0.0
                        and (now_mono - self._last_sig_check_mono) < self.refresh_interval_seconds
                    ):
                        return cached
                    self._last_sig_check_mono = now_mono
                    current_sig = self.ledger.get_live_source_signature()
                    if current_sig == self._last_source_sig:
                        return cached
            return self.scrape_and_index(include_git_history=True, force_recreate=force)

    def scrape_and_index(
        self,
        include_git_history: bool = True,
        force_recreate: bool = False,
    ) -> dict[str, Any]:
        """
        Scrape benchmarks, git history, real-time benchmark streams, and PyTorch model source
        code from neural_networks, persist the canonical ledger via BenchmarkLedger, and
        incrementally index changed documents into Qdrant (`portfolio_neural_benchmarks`).
        """
        with self._lock:
            t0 = time.perf_counter()
            overview = self.ledger.scrape_and_build(include_git_history=include_git_history)
            docs: list[BenchmarkQdrantDocument] = self.ledger.build_qdrant_documents()

            model_name = str(
                getattr(
                    self.qdrant_graph,
                    "embedding_model",
                    "sentence-transformers/all-MiniLM-L6-v2",
                )
            )
            vector_dim = MODEL_DIMENSIONS.get(model_name, 384)
            chunk_size = int(getattr(self.qdrant_graph, "chunk_size", 380))
            chunk_overlap = int(getattr(self.qdrant_graph, "chunk_overlap", 64))

            client = self.qdrant_graph._ensure_qdrant_client()
            collection_exists = client.collection_exists(self.collection_name)
            if collection_exists and force_recreate:
                client.delete_collection(self.collection_name)
                collection_exists = False
                self._doc_hashes.clear()

            if not collection_exists:
                client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
                )
                self._doc_hashes.clear()

            if not self._doc_hashes:
                self._index.delete_index("DOCUMENTS")
                self._index.new("DOCUMENTS")
                self._index.delete_index("CHUNKS_BY_DOC")
                self._index.new("CHUNKS_BY_DOC")

            existing_qdrant_meta: dict[str, dict[str, Any]] = {}
            if collection_exists and not force_recreate:
                try:
                    scrolled_zero, _ = client.scroll(
                        collection_name=self.collection_name,
                        scroll_filter=Filter(
                            must=[FieldCondition(key="chunk_index", match=MatchValue(value=0))]
                        ),
                        limit=2000,
                        with_payload=True,
                        with_vectors=False,
                    )
                    for pt in scrolled_zero:
                        pl = pt.payload or {}
                        if pl.get("doc_id"):
                            existing_qdrant_meta[str(pl["doc_id"])] = dict(pl)
                except Exception:
                    existing_qdrant_meta = {}

            active_doc_ids = {doc.doc_id for doc in docs}
            stale_doc_ids = [d_id for d_id in existing_qdrant_meta if d_id not in active_doc_ids]
            for stale_id in stale_doc_ids:
                self._doc_hashes.pop(stale_id, None)
                try:
                    client.delete(
                        collection_name=self.collection_name,
                        points_selector=Filter(
                            must=[FieldCondition(key="doc_id", match=MatchValue(value=stale_id))]
                        ),
                    )
                except Exception:
                    pass

            changed_chunk_records: list[dict[str, Any]] = []
            embed_inputs: list[str] = []
            changed_docs: list[BenchmarkQdrantDocument] = []
            chunks_by_doc: dict[str, list[dict[str, Any]]] = {}

            for doc in docs:
                full_text = doc.markdown.strip()
                text_sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
                doc_friendly = (
                    getattr(doc, "friendly_title", "")
                    or str(doc.payload.get("friendly_title") or "")
                    or derive_friendly_model_title(doc.title, str(doc.payload.get("class_name") or ""))
                )
                doc_last_tested = str(getattr(doc, "last_tested_at", "") or "")
                prev_pl = existing_qdrant_meta.get(doc.doc_id)
                is_unchanged_in_qdrant = (
                    not force_recreate
                    and prev_pl is not None
                    and str(prev_pl.get("content_sha256") or prev_pl.get("text_sha256") or "") == text_sha256
                    and isinstance(prev_pl.get("doc_payload"), dict)
                    and bool(prev_pl.get("doc_payload"))
                    and str(prev_pl.get("last_tested_at") or "") == doc_last_tested
                    and str(prev_pl.get("friendly_title") or "") == doc_friendly
                )
                if is_unchanged_in_qdrant:
                    self._doc_hashes[doc.doc_id] = text_sha256
                    existing_chunks = (
                        self._index.load_from_index("CHUNKS_BY_DOC", doc.doc_id)
                        or self._fetch_doc_chunks_from_qdrant(doc.doc_id)
                    )
                    chunks_by_doc[doc.doc_id] = existing_chunks
                    continue

                changed_docs.append(doc)
                if doc.doc_id in self._doc_hashes or doc.doc_id in existing_qdrant_meta:
                    try:
                        client.delete(
                            collection_name=self.collection_name,
                            points_selector=Filter(
                                must=[FieldCondition(key="doc_id", match=MatchValue(value=doc.doc_id))]
                            ),
                        )
                    except Exception:
                        pass
                self._doc_hashes[doc.doc_id] = text_sha256

                raw_chunks = chunk_document_text(
                    full_text,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                )
                total_c = len(raw_chunks)
                point_ids = [
                    uuid5_from_args(
                        QDRANT_NAMESPACE_UUID,
                        self.collection_name,
                        doc.doc_id,
                        idx,
                        model_name,
                        chunk_size,
                        chunk_overlap,
                    )
                    for idx in range(total_c)
                ]

                doc_chunk_list: list[dict[str, Any]] = []
                for idx, c in enumerate(raw_chunks):
                    prev_id = point_ids[idx - 1] if idx > 0 else None
                    next_id = point_ids[idx + 1] if idx + 1 < total_c else None
                    rec: dict[str, Any] = {
                        "point_id": point_ids[idx],
                        "doc_id": doc.doc_id,
                        "doc_type": doc.doc_type,
                        "title": doc.title,
                        "friendly_title": doc_friendly,
                        "subtitle": doc.subtitle,
                        "category": doc.category,
                        "summary": doc.summary,
                        "source_file": doc.source_file,
                        "is_overall_best": doc.is_overall_best,
                        "is_promoted": getattr(doc, "is_promoted", False),
                        "overall_rank": doc.overall_rank,
                        "composite_score": doc.composite_score,
                        "wins_count": doc.wins_count,
                        "top3_count": doc.top3_count,
                        "mean_rank": doc.mean_rank,
                        "best_model_name": doc.best_model_name,
                        "last_tested_at": doc_last_tested,
                        "related_modules": doc.related_modules,
                        "related_kis": doc.related_kis,
                        "chunk_index": idx,
                        "total_chunks": total_c,
                        "char_start": c["char_start"],
                        "char_end": c["char_end"],
                        "core_start": c["core_start"],
                        "core_end": c["core_end"],
                        "overlap_prefix_len": c["overlap_prefix_len"],
                        "chunk_text": c["chunk_text"],
                        "non_overlapping_slice": c["non_overlapping_slice"],
                        "prev_chunk_id": prev_id,
                        "next_chunk_id": next_id,
                        "content_sha256": text_sha256,
                        "full_text_sha256": text_sha256,
                        "embedding_model": model_name,
                    }
                    if idx == 0:
                        payload_copy = dict(doc.payload)
                        payload_copy.setdefault("friendly_title", doc_friendly)
                        payload_copy["last_tested_at"] = doc_last_tested
                        rec["doc_payload"] = payload_copy
                        rec["code_snippet"] = doc.code_snippet
                        rec["markdown"] = doc.markdown
                    changed_chunk_records.append(rec)
                    doc_chunk_list.append(rec)
                    embed_inputs.append(
                        f"[{doc.doc_type}] {doc.title} ({doc.category}) — {c['chunk_text']}"
                    )
                chunks_by_doc[doc.doc_id] = doc_chunk_list

            if changed_chunk_records:
                vectors = encode_texts_cpu(embed_inputs, model_name=model_name)
                points: list[PointStruct] = [
                    PointStruct(
                        id=rec["point_id"],
                        vector=vec,
                        payload=rec,
                    )
                    for rec, vec in zip(changed_chunk_records, vectors)
                ]
                if points:
                    client.upsert(collection_name=self.collection_name, points=points)

            rejoin_verified_count = 0
            total_chunks_count = 0
            for doc in docs:
                doc_chunks = chunks_by_doc.get(doc.doc_id, [])
                total_chunks_count += len(doc_chunks)
                rejoined = rejoin_chunks(doc_chunks)
                if rejoined.get("integrity_verified"):
                    rejoin_verified_count += 1
                doc_dict = doc.model_dump()
                doc_dict["qdrant_chunk_count"] = len(doc_chunks)
                doc_dict["rejoin_verified"] = bool(rejoined.get("integrity_verified"))
                doc_dict["rejoined_sha256"] = str(rejoined.get("rejoined_sha256", ""))
                self._index.store_in_index("DOCUMENTS", doc.doc_id, doc_dict)
                self._index.store_in_index("CHUNKS_BY_DOC", doc.doc_id, doc_chunks)

            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            self._last_source_sig = self.ledger.get_live_source_signature()
            status = {
                "collection_name": self.collection_name,
                "repo_path": str(self.repo_path),
                "ledger_path": str(self.ledger_path),
                "embedding_model": model_name,
                "vector_dimensions": vector_dim,
                "total_documents": len(docs),
                "total_chunks_indexed": total_chunks_count,
                "incremental_updated_documents": len(changed_docs),
                "incremental_upserted_chunks": len(changed_chunk_records),
                "rejoin_verified_documents": rejoin_verified_count,
                "total_models": overview["total_models"],
                "total_benchmarks": overview["total_benchmarks"],
                "total_runs": overview["total_runs"],
                "best_model_name": overview["best_model"]["model_name"] if overview.get("best_model") else "",
                "last_indexed_ms": elapsed_ms,
                "last_scraped_at": overview["last_scraped_at"],
                "realtime_sync_active": self.is_watcher_active,
                "poll_interval_seconds": self.refresh_interval_seconds if self.is_watcher_active else 0,
            }
            self._last_sig_check_mono = time.monotonic()
            self._qdrant_state_generation += 1
            self._index.store_in_index("STATE", "QDRANT_OVERVIEW", None)
            self._index.store_in_index("STATE", "STATUS", status)
            self._initialized = True
            return status

    def _fetch_doc_chunks_from_qdrant(self, doc_id: str) -> list[dict[str, Any]]:
        """Fetch all indexed chunks for `doc_id` directly from Qdrant (`portfolio_neural_benchmarks`)."""
        client = self.qdrant_graph._ensure_qdrant_client()
        if not client.collection_exists(self.collection_name):
            return []
        try:
            scrolled, _ = client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(key="doc_id", match=MatchValue(value=doc_id))]
                ),
                limit=256,
                with_payload=True,
                with_vectors=False,
            )
            chunks = [dict(pt.payload) for pt in scrolled if pt.payload]
            chunks.sort(key=lambda c: int(c.get("chunk_index", 0)))
            return chunks
        except Exception:
            return []

    def _load_state_from_qdrant(self) -> dict[str, Any]:
        """
        Dynamically reconstruct all model lists, benchmark task leaderboards, run snapshots,
        documents, and `last_tested_at` timestamps directly from the benchmark results
        saved in Qdrant (`portfolio_neural_benchmarks`), caching the reconstructed view
        across read requests until the next index generation or source-signature change.
        """
        with self._lock:
            self.ensure_indexed(force=False)
            cache_key = (self._last_source_sig, self._qdrant_state_generation)
            cached_entry = self._index.load_from_index("STATE", "QDRANT_OVERVIEW")
            if (
                isinstance(cached_entry, tuple)
                and len(cached_entry) == 2
                and cached_entry[0] == cache_key
                and isinstance(cached_entry[1], dict)
            ):
                return dict(cached_entry[1])

            client = self.qdrant_graph._ensure_qdrant_client()
            scrolled_zero: list[Any] = []
            if client.collection_exists(self.collection_name):
                scrolled_zero, _ = client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=Filter(
                        must=[FieldCondition(key="chunk_index", match=MatchValue(value=0))]
                    ),
                    limit=2000,
                    with_payload=True,
                    with_vectors=False,
                )

            models_by_name: dict[str, dict[str, Any]] = {}
            tasks_list: list[dict[str, Any]] = []
            runs_list: list[dict[str, Any]] = []
            docs_by_id: dict[str, dict[str, Any]] = {}

            for pt in scrolled_zero:
                pl = dict(pt.payload or {})
                doc_id = str(pl.get("doc_id") or "")
                doc_type = str(pl.get("doc_type") or "")
                if not doc_id or not doc_type:
                    continue

                raw_doc_payload = pl.get("doc_payload")
                if not isinstance(raw_doc_payload, dict) or not raw_doc_payload:
                    cached_doc = self._index.load_from_index("DOCUMENTS", doc_id) or {}
                    raw_doc_payload = dict(cached_doc.get("payload") or {})
                else:
                    raw_doc_payload = dict(raw_doc_payload)

                markdown_text = str(pl.get("markdown") or "")
                code_snippet = str(pl.get("code_snippet") or "")
                if not markdown_text:
                    cached_doc = self._index.load_from_index("DOCUMENTS", doc_id) or {}
                    markdown_text = str(cached_doc.get("markdown") or pl.get("chunk_text") or "")
                    if not code_snippet:
                        code_snippet = str(cached_doc.get("code_snippet") or "")

                friendly_title = str(
                    pl.get("friendly_title")
                    or raw_doc_payload.get("friendly_title")
                    or (
                        derive_friendly_model_title(
                            str(pl.get("title") or ""),
                            str(raw_doc_payload.get("class_name") or ""),
                        )
                        if doc_type == "benchmark_model"
                        else str(pl.get("title") or "")
                    )
                )
                raw_doc_payload.setdefault("friendly_title", friendly_title)

                doc_entry: dict[str, Any] = {
                    "doc_id": doc_id,
                    "doc_type": doc_type,
                    "title": str(pl.get("title") or ""),
                    "friendly_title": friendly_title,
                    "subtitle": str(pl.get("subtitle") or ""),
                    "category": str(pl.get("category") or ""),
                    "summary": str(pl.get("summary") or ""),
                    "markdown": markdown_text,
                    "code_snippet": code_snippet,
                    "source_file": str(pl.get("source_file") or ""),
                    "is_overall_best": bool(pl.get("is_overall_best", False)),
                    "is_promoted": bool(pl.get("is_promoted", False)),
                    "overall_rank": int(pl.get("overall_rank", 999)),
                    "composite_score": float(pl.get("composite_score", 0.0)),
                    "wins_count": int(pl.get("wins_count", 0)),
                    "top3_count": int(pl.get("top3_count", 0)),
                    "mean_rank": float(pl.get("mean_rank", 99.0)),
                    "best_model_name": str(pl.get("best_model_name") or ""),
                    "last_tested_at": str(pl.get("last_tested_at") or raw_doc_payload.get("last_tested_at") or ""),
                    "related_modules": list(pl.get("related_modules") or []),
                    "related_kis": list(pl.get("related_kis") or []),
                    "payload": raw_doc_payload,
                    "qdrant_chunk_count": int(pl.get("total_chunks", 1)),
                    "rejoin_verified": True,
                    "rejoined_sha256": str(pl.get("content_sha256") or pl.get("text_sha256") or ""),
                }
                docs_by_id[doc_id] = doc_entry

                if doc_type == "benchmark_model":
                    m_name = str(raw_doc_payload.get("model_name") or pl.get("title") or doc_id)
                    raw_doc_payload.setdefault("model_id", doc_id)
                    raw_doc_payload.setdefault("model_name", m_name)
                    raw_doc_payload.setdefault("class_name", m_name.split(" (")[0].strip())
                    raw_doc_payload["friendly_title"] = friendly_title
                    models_by_name[m_name] = raw_doc_payload
                elif doc_type == "benchmark_task":
                    raw_doc_payload.setdefault("benchmark_id", doc_id)
                    tasks_list.append(raw_doc_payload)
                elif doc_type == "benchmark_run":
                    raw_doc_payload.setdefault("run_id", doc_id)
                    runs_list.append(raw_doc_payload)

            # Dynamically synthesize any model that appears in Qdrant benchmark_task leaderboards
            # or Qdrant benchmark_run results even if inserted directly into Qdrant without a model point
            discovered_entries_by_model: dict[str, list[dict[str, Any]]] = {}
            for t_obj in tasks_list:
                lb = t_obj.get("leaderboard") or []
                for row in lb:
                    if not isinstance(row, dict):
                        continue
                    r_model = str(row.get("model_name") or "").strip()
                    if r_model:
                        discovered_entries_by_model.setdefault(r_model, []).append(
                            {
                                "benchmark_id": str(t_obj.get("benchmark_id") or ""),
                                "benchmark_name": str(t_obj.get("name") or ""),
                                "benchmark_title": str(t_obj.get("title") or ""),
                                "category": str(t_obj.get("category") or "language"),
                                "rank": int(row.get("rank") or 99),
                                "total_models": len(lb),
                                "primary_metric": float(row.get("primary_metric") or 0.0),
                                "primary_metric_name": str(t_obj.get("primary_metric_name") or ""),
                                "secondary_metric": float(row.get("secondary_metric") or 0.0),
                                "secondary_metric_name": str(t_obj.get("secondary_metric_name") or ""),
                                "tertiary_metric": float(row.get("tertiary_metric") or 0.0),
                                "tertiary_metric_name": str(t_obj.get("tertiary_metric_name") or ""),
                                "higher_is_better": bool(t_obj.get("higher_is_better", False)),
                                "elapsed_sec": float(row.get("elapsed_sec") or 0.0),
                                "num_params": int(row.get("num_params") or 0),
                                "delta_vs_best_pct": 0.0,
                            }
                        )

            for r_obj in runs_list:
                for r_model in list((r_obj.get("model_mean_ranks") or {}).keys()) + list(
                    r_obj.get("tested_models") or []
                ):
                    r_model_str = str(r_model or "").strip()
                    if r_model_str and r_model_str not in models_by_name:
                        discovered_entries_by_model.setdefault(r_model_str, [])

            total_tasks_count = max(len(tasks_list), 1)
            total_known_models = max(len( set(models_by_name.keys()) | set(discovered_entries_by_model.keys()) ), 2)

            for disc_name, disc_entries in discovered_entries_by_model.items():
                if disc_name in models_by_name:
                    continue
                slug_chars = "".join(ch.lower() if ch.isalnum() else "_" for ch in disc_name)
                while "__" in slug_chars:
                    slug_chars = slug_chars.replace("__", "_")
                synth_id = f"model_{slug_chars.strip('_')}"
                cls_guess = disc_name.split(" (")[0].strip()
                friendly_guess = derive_friendly_model_title(disc_name, cls_guess)
                if disc_entries:
                    ranks = sorted(int(e["rank"]) for e in disc_entries)
                    wins_c = sum(1 for r in ranks if r == 1)
                    top3_c = sum(1 for r in ranks if r <= 3)
                    mean_r = round(sum(ranks) / len(ranks), 2)
                    med_r = float(ranks[len(ranks) // 2])
                    avg_p = int(round(sum(int(e["num_params"]) for e in disc_entries) / len(disc_entries)))
                    avg_el = round(sum(float(e["elapsed_sec"]) for e in disc_entries) / len(disc_entries), 3)
                    eval_c = len(disc_entries)
                else:
                    run_ranks = [
                        float((r.get("model_mean_ranks") or {})[disc_name])
                        for r in runs_list
                        if disc_name in (r.get("model_mean_ranks") or {})
                    ]
                    run_wins = [
                        int((r.get("model_win_counts") or {}).get(disc_name, 0))
                        for r in runs_list
                        if disc_name in (r.get("model_mean_ranks") or {})
                    ]
                    wins_c = max(run_wins, default=0)
                    top3_c = wins_c
                    mean_r = round(sum(run_ranks) / len(run_ranks), 2) if run_ranks else 25.0
                    med_r = mean_r
                    avg_p = 0
                    avg_el = 0.0
                    eval_c = total_tasks_count

                rank_comp = ((total_known_models - mean_r) / max(total_known_models - 1, 1)) * 65.0
                top3_comp = (top3_c / total_tasks_count) * 20.0
                win_comp = (wins_c / total_tasks_count) * 15.0
                cov_factor = 0.85 + 0.15 * (min(eval_c, total_tasks_count) / total_tasks_count)
                comp_score = round((rank_comp + top3_comp + win_comp) * cov_factor, 2)

                synth_profile: dict[str, Any] = {
                    "model_id": synth_id,
                    "model_name": disc_name,
                    "class_name": cls_guess,
                    "friendly_title": friendly_guess,
                    "family": "Qdrant Discovered Architecture",
                    "is_polyphonic": True,
                    "is_overall_best": False,
                    "is_promoted": False,
                    "in_latest_suite": True,
                    "overall_rank": 999,
                    "composite_score": comp_score,
                    "wins_count": wins_c,
                    "top3_count": top3_c,
                    "benchmarks_evaluated": eval_c,
                    "mean_rank": mean_r,
                    "median_rank": med_r,
                    "avg_num_params": avg_p,
                    "avg_elapsed_sec": avg_el,
                    "summary": f"{disc_name} dynamically discovered from Qdrant benchmark results.",
                    "architectural_highlights": [],
                    "source_file": "",
                    "source_line_start": 1,
                    "source_line_end": 1,
                    "source_code": "",
                    "docstring": "",
                    "method_names": [],
                    "benchmark_entries": disc_entries,
                    "historical_ranks": [],
                    "last_tested_at": "",
                    "promoted_metadata": {},
                    "markdown_overview": f"## {disc_name}\n\nDynamically discovered from Qdrant (`{self.collection_name}`).",
                    "related_modules": ["benchmark_ledger"],
                    "related_kis": ["ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history"],
                }
                models_by_name[disc_name] = synth_profile
                docs_by_id[synth_id] = {
                    "doc_id": synth_id,
                    "doc_type": "benchmark_model",
                    "title": disc_name,
                    "friendly_title": friendly_guess,
                    "subtitle": f"Qdrant Discovered Architecture • Mean Rank {mean_r:.2f}",
                    "category": "Polyphonic Architectures",
                    "summary": synth_profile["summary"],
                    "markdown": synth_profile["markdown_overview"],
                    "code_snippet": "",
                    "source_file": "",
                    "is_overall_best": False,
                    "is_promoted": False,
                    "overall_rank": 999,
                    "composite_score": comp_score,
                    "wins_count": wins_c,
                    "top3_count": top3_c,
                    "mean_rank": mean_r,
                    "best_model_name": disc_name,
                    "last_tested_at": "",
                    "related_modules": ["benchmark_ledger"],
                    "related_kis": ["ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history"],
                    "payload": synth_profile,
                    "qdrant_chunk_count": 1,
                    "rejoin_verified": True,
                    "rejoined_sha256": "",
                }

            # Dynamically resolve `last_tested_at` for every model from Qdrant benchmark results
            for m_name, m_dict in models_by_name.items():
                m_id = str(m_dict.get("model_id") or "")
                d_entry = docs_by_id.get(m_id)
                authentic_ts: list[str] = []
                fallback_ts: list[str] = []

                for base_ts in (
                    str(m_dict.get("last_tested_at") or ""),
                    str((d_entry or {}).get("last_tested_at") or ""),
                ):
                    if base_ts:
                        authentic_ts.append(base_ts)

                for r_obj in runs_list:
                    r_ts = str(r_obj.get("recorded_at") or "").strip()
                    if not r_ts:
                        continue
                    r_label_low = str(r_obj.get("run_label") or "").lower()
                    r_commit = str(r_obj.get("commit_hash") or "")
                    tested_set = set(str(x) for x in (r_obj.get("tested_models") or []))
                    in_run_ranks = m_name in (r_obj.get("model_mean_ranks") or {})
                    if "pytest" not in r_label_low and (
                        m_name in tested_set
                        or (in_run_ranks and r_commit not in {"worktree-head", "ui-checkpoint", "api-save"})
                    ):
                        authentic_ts.append(r_ts)
                    elif in_run_ranks or m_name in tested_set:
                        if r_commit != "worktree-head":
                            fallback_ts.append(r_ts)

                for t_obj in tasks_list:
                    for hp in t_obj.get("history_points") or []:
                        if not isinstance(hp, dict):
                            continue
                        hp_ts = str(hp.get("recorded_at") or "").strip()
                        if not hp_ts:
                            continue
                        in_hp = m_name in (hp.get("model_ranks") or {}) or m_name in (
                            hp.get("model_metrics") or {}
                        )
                        if not in_hp:
                            continue
                        hp_commit = str(hp.get("commit_hash") or "")
                        hp_label_low = str(hp.get("run_label") or "").lower()
                        if (
                            hp_commit not in {"worktree-head", "ui-checkpoint", "api-save"}
                            and "pytest" not in hp_label_low
                        ):
                            authentic_ts.append(hp_ts)
                        elif hp_commit != "worktree-head":
                            fallback_ts.append(hp_ts)

                for hr in m_dict.get("historical_ranks") or []:
                    if not isinstance(hr, dict):
                        continue
                    hr_ts = str(hr.get("recorded_at") or "").strip()
                    if not hr_ts:
                        continue
                    hr_commit = str(hr.get("commit_hash") or "")
                    hr_label_low = str(hr.get("run_label") or "").lower()
                    if (
                        hr_commit not in {"worktree-head", "ui-checkpoint", "api-save"}
                        and "pytest" not in hr_label_low
                    ):
                        authentic_ts.append(hr_ts)
                    elif hr_commit != "worktree-head":
                        fallback_ts.append(hr_ts)

                resolved_ts = max(authentic_ts) if authentic_ts else (max(fallback_ts) if fallback_ts else "")
                m_dict["last_tested_at"] = resolved_ts
                m_dict["friendly_title"] = str(
                    m_dict.get("friendly_title")
                    or derive_friendly_model_title(m_name, str(m_dict.get("class_name") or ""))
                )
                if d_entry is not None:
                    d_entry["last_tested_at"] = resolved_ts
                    d_entry["friendly_title"] = m_dict["friendly_title"]
                    d_entry["payload"] = m_dict

            models_list = list(models_by_name.values())
            models_list.sort(
                key=lambda m: (
                    0 if m.get("is_promoted") else 1,
                    int(m.get("overall_rank") or 999),
                    -float(m.get("composite_score") or 0.0),
                    float(m.get("mean_rank") or 99.0),
                    str(m.get("model_name") or ""),
                )
            )
            for rank_idx, m_dict in enumerate(models_list, start=1):
                m_dict["overall_rank"] = rank_idx
                m_dict["is_overall_best"] = rank_idx == 1
                m_id = str(m_dict.get("model_id") or "")
                if m_id in docs_by_id:
                    docs_by_id[m_id]["overall_rank"] = rank_idx
                    docs_by_id[m_id]["is_overall_best"] = rank_idx == 1

            tasks_list.sort(
                key=lambda t: (
                    0 if t.get("category") == "language" else 1,
                    str(t.get("benchmark_id") or ""),
                )
            )
            runs_list.sort(key=lambda r: str(r.get("recorded_at") or ""))

            documents_list = list(docs_by_id.values())
            type_priority = {"benchmark_model": 0, "benchmark_task": 1, "benchmark_run": 2}
            documents_list.sort(
                key=lambda d: (
                    0 if d.get("is_overall_best") else 1,
                    type_priority.get(str(d.get("doc_type")), 9),
                    int(d.get("overall_rank", 999)),
                    str(d.get("title", "")),
                )
            )

            best_model = models_list[0] if models_list else None
            promoted_model = next((m for m in models_list if m.get("is_promoted")), None) or best_model
            promoted_manifest = (
                dict((promoted_model or {}).get("promoted_metadata") or {})
                or self.ledger.get_overview().get("promoted_manifest")
                or {}
            )
            q_state = self._index.load_from_index("STATE", "STATUS") or {}

            reconstructed = {
                "repo_path": str(self.repo_path),
                "last_scraped_at": str(q_state.get("last_scraped_at") or ""),
                "total_models": len(models_list),
                "total_benchmarks": len(tasks_list),
                "total_runs": len(runs_list),
                "language_benchmarks_count": sum(1 for t in tasks_list if t.get("category") == "language"),
                "continuous_benchmarks_count": sum(
                    1 for t in tasks_list if t.get("category") == "continuous"
                ),
                "best_model": best_model,
                "promoted_model": promoted_model,
                "promoted_manifest": promoted_manifest,
                "models": models_list,
                "tasks": tasks_list,
                "runs": runs_list,
                "documents": documents_list,
            }
            self._index.store_in_index(
                "STATE",
                "QDRANT_OVERVIEW",
                ((self._last_source_sig, self._qdrant_state_generation), reconstructed),
            )
            return dict(reconstructed)

    def ingest_realtime_update(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Real-time ingestion endpoint invoked directly by neural_networks benchmark runners
        after each benchmark task completes. Incrementally updates Qdrant (`portfolio_neural_benchmarks`).
        """
        with self._lock:
            q_status = self.scrape_and_index(include_git_history=True, force_recreate=False)
            overview = self._load_state_from_qdrant()
            return {
                "status": "ingested_and_synced",
                "qdrant_status": q_status,
                "best_model": overview.get("best_model"),
                "total_models": overview.get("total_models"),
                "total_benchmarks": overview.get("total_benchmarks"),
                "total_runs": overview.get("total_runs"),
            }

    def get_qdrant_status(self) -> dict[str, Any]:
        return self.ensure_indexed(force=False)

    def get_overview(self) -> dict[str, Any]:
        q_status = self.ensure_indexed(force=False)
        qdrant_overview = self._load_state_from_qdrant()
        q_status = dict(q_status)
        q_status["total_models"] = qdrant_overview["total_models"]
        q_status["total_benchmarks"] = qdrant_overview["total_benchmarks"]
        q_status["total_runs"] = qdrant_overview["total_runs"]
        q_status["total_documents"] = len(qdrant_overview["documents"])
        if qdrant_overview.get("best_model"):
            q_status["best_model_name"] = str(qdrant_overview["best_model"].get("model_name") or "")
        return {
            **qdrant_overview,
            "collection": self.collection_name,
            "qdrant_status": q_status,
            "file_watcher_enabled": self.is_watcher_active,
            "poll_interval_ms": int(self.refresh_interval_seconds * 1000) if self.is_watcher_active else 0,
        }

    def get_history(self) -> dict[str, Any]:
        self.ensure_indexed(force=False)
        overview = self._load_state_from_qdrant()
        return {
            "total_runs": overview["total_runs"],
            "best_model": overview["best_model"],
            "runs": overview["runs"],
            "task_histories": [
                {
                    "benchmark_id": t["benchmark_id"],
                    "name": t["name"],
                    "title": t["title"],
                    "category": t["category"],
                    "primary_metric_name": t["primary_metric_name"],
                    "higher_is_better": t["higher_is_better"],
                    "best_model_name": t["best_model_name"],
                    "best_primary_metric": t["best_primary_metric"],
                    "history_points": t["history_points"],
                }
                for t in overview["tasks"]
            ],
            "model_histories": [
                {
                    "model_id": m["model_id"],
                    "model_name": m["model_name"],
                    "overall_rank": m["overall_rank"],
                    "wins_count": m["wins_count"],
                    "mean_rank": m["mean_rank"],
                    "last_tested_at": m.get("last_tested_at", ""),
                    "historical_ranks": m["historical_ranks"],
                }
                for m in overview["models"]
            ],
        }

    def _get_all_indexed_documents(self) -> list[dict[str, Any]]:
        return self._load_state_from_qdrant()["documents"]

    def get_model_detail(self, model_id_or_name: str) -> dict[str, Any] | None:
        overview = self._load_state_from_qdrant()
        q = (model_id_or_name or "").strip()
        q_low = q.lower()
        model: dict[str, Any] | None = None
        for m in overview["models"]:
            if (
                str(m.get("model_id", "")).lower() == q_low
                or str(m.get("model_name", "")).lower() == q_low
                or str(m.get("class_name", "")).lower() == q_low
            ):
                model = dict(m)
                break
        if model is None:
            for m in overview["models"]:
                if (
                    q_low in str(m.get("model_id", "")).lower()
                    or q_low in str(m.get("model_name", "")).lower()
                    or q_low in str(m.get("class_name", "")).lower()
                ):
                    model = dict(m)
                    break
        if model is None:
            return None
        doc_id = str(model["model_id"])
        chunks = self._fetch_doc_chunks_from_qdrant(doc_id)
        if not chunks:
            chunks = self._index.load_from_index("CHUNKS_BY_DOC", doc_id) or []
        rejoined = rejoin_chunks(chunks)
        return {
            **model,
            "qdrant_doc_id": doc_id,
            "qdrant_chunks_count": len(chunks),
            "qdrant_rejoin": rejoined,
        }

    def get_task_detail(self, benchmark_id_or_name: str) -> dict[str, Any] | None:
        overview = self._load_state_from_qdrant()
        q = (benchmark_id_or_name or "").strip().lower()
        task: dict[str, Any] | None = None
        for t in overview["tasks"]:
            if (
                str(t.get("benchmark_id", "")).lower() == q
                or str(t.get("name", "")).lower() == q
                or str(t.get("title", "")).lower() == q
            ):
                task = dict(t)
                break
        if task is None:
            return None
        doc_id = str(task["benchmark_id"])
        chunks = self._fetch_doc_chunks_from_qdrant(doc_id)
        if not chunks:
            chunks = self._index.load_from_index("CHUNKS_BY_DOC", doc_id) or []
        rejoined = rejoin_chunks(chunks)
        return {
            **task,
            "qdrant_doc_id": doc_id,
            "qdrant_chunks_count": len(chunks),
            "qdrant_rejoin": rejoined,
        }

    def save_benchmark_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Save a benchmark run via BenchmarkLedger and immediately re-index Qdrant."""
        with self._lock:
            req = BenchmarkSaveRequest.model_validate(payload)
            save_res = self.ledger.save_run(req, persist_path=self.ledger_path)
            q_status = self.scrape_and_index(include_git_history=True)
            return {
                "status": "saved",
                **save_res,
                "qdrant_status": q_status,
            }

    def search_benchmarks(
        self,
        query: str = "",
        doc_type: str | None = None,
        category: str | None = None,
        top_k: int = 24,
    ) -> dict[str, Any]:
        """
        Perform hybrid semantic Qdrant vector search + lexical scoring over all benchmark
        models, tasks, and historical runs. When query is empty, returns all documents
        ordered with the #1 Overall Best Model first.
        """
        q_status = self.ensure_indexed(force=False)
        all_docs = self._get_all_indexed_documents()

        if doc_type and doc_type != "All":
            all_docs = [d for d in all_docs if d.get("doc_type") == doc_type]
        if category and category != "All":
            all_docs = [d for d in all_docs if d.get("category") == category]

        cleaned_q = query.strip()
        if not cleaned_q:
            return {
                "query": "",
                "total_results": len(all_docs),
                "qdrant_status": q_status,
                "results": all_docs[: max(1, top_k)],
            }

        model_name = str(q_status.get("embedding_model", "sentence-transformers/all-MiniLM-L6-v2"))
        q_vec = encode_texts_cpu([cleaned_q], model_name=model_name)[0]
        client = self.qdrant_graph._ensure_qdrant_client()

        must_conditions = []
        if doc_type and doc_type != "All":
            must_conditions.append(FieldCondition(key="doc_type", match=MatchValue(value=doc_type)))
        if category and category != "All":
            must_conditions.append(FieldCondition(key="category", match=MatchValue(value=category)))

        query_filter = Filter(must=must_conditions) if must_conditions else None
        q_res = client.query_points(
            collection_name=self.collection_name,
            query=q_vec,
            query_filter=query_filter,
            limit=max(top_k * 3, 30),
            with_payload=True,
        )

        best_score_by_doc: dict[str, float] = {}
        matched_snippet_by_doc: dict[str, str] = {}
        for pt in q_res.points:
            payload = pt.payload or {}
            d_id = str(payload.get("doc_id", ""))
            if not d_id:
                continue
            sc = float(pt.score or 0.0)
            if sc > best_score_by_doc.get(d_id, -1.0):
                best_score_by_doc[d_id] = sc
                matched_snippet_by_doc[d_id] = str(payload.get("chunk_text", ""))[:260]

        q_lower = cleaned_q.lower()
        q_tokens = [tok for tok in q_lower.split() if tok]

        scored_docs: list[dict[str, Any]] = []
        for doc in all_docs:
            d_id = str(doc["doc_id"])
            sem_score = max(0.0, best_score_by_doc.get(d_id, 0.0))
            haystack = " ".join(
                [
                    str(doc.get("title", "")),
                    str(doc.get("subtitle", "")),
                    str(doc.get("category", "")),
                    str(doc.get("summary", "")),
                    str(doc.get("markdown", "")),
                ]
            ).lower()
            title_lower = str(doc.get("title", "")).lower()

            lexical_boost = 0.0
            if q_lower in title_lower:
                lexical_boost += 0.45
            elif q_lower in haystack:
                lexical_boost += 0.25
            token_hits = sum(1 for tok in q_tokens if tok in haystack)
            if q_tokens:
                lexical_boost += 0.20 * (token_hits / len(q_tokens))

            combined_score = round(sem_score + lexical_boost, 4)
            if token_hits > 0 or sem_score >= 0.22:
                enriched = dict(doc)
                enriched["search_score"] = combined_score
                enriched["semantic_score"] = round(sem_score, 4)
                enriched["matched_snippet"] = matched_snippet_by_doc.get(d_id, doc.get("summary", ""))
                scored_docs.append(enriched)

        scored_docs.sort(
            key=lambda d: (
                -float(d.get("search_score", 0.0)),
                0 if d.get("is_overall_best") else 1,
                int(d.get("overall_rank", 999)),
            )
        )
        return {
            "query": cleaned_q,
            "total_results": len(scored_docs),
            "qdrant_status": q_status,
            "results": scored_docs[: max(1, top_k)],
        }

    def query_datasource(
        self,
        query: str = "",
        doc_type: str | None = None,
        category: str | None = None,
        top_k: int = 24,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Implements `Datasource.query_datasource` over the Qdrant neural benchmarks index."""
        return self.search_benchmarks(
            query=query,
            doc_type=doc_type,
            category=category,
            top_k=top_k,
        )

    def _ensure_promoted_harness(self, force_replace_from_promoted_file: bool = False) -> Any:
        """
        Lazily initialize (or replace from the file in `neural_networks/promoted/` via `FSBase`)
        the live `PromotedModelHarness`. Also supports loading any user-selected model from the
        25-model dropdown when `self._selected_chat_model_id` is set.
        """
        with self._lock:
            import importlib
            import io
            import json
            import sys

            repo_str = str(self.repo_path)
            src_str = str(self.repo_path / "src")
            if src_str not in sys.path:
                sys.path.insert(0, src_str)
            if repo_str not in sys.path:
                sys.path.insert(0, repo_str)

            manifest_path = self.promoted_dir / "promoted_manifest.json"
            manifest_cls = ""
            if self.promoted_fs.client.exists(str(manifest_path)):
                try:
                    mbuf = io.BytesIO()
                    self.promoted_fs.read(str(manifest_path), mbuf)
                    manifest_cls = str(
                        json.loads(mbuf.getvalue().decode("utf-8")).get("class_name", "")
                    )
                except Exception:
                    manifest_cls = ""

            if force_replace_from_promoted_file:
                self._selected_chat_model_id = ""
                self._selected_chat_model_meta = {}
                self._promoted_harness = None

            if (
                self._promoted_harness is not None
                and not force_replace_from_promoted_file
                and (
                    self._selected_chat_model_id
                    or not manifest_cls
                    or self._promoted_harness_class == manifest_cls
                )
            ):
                return self._promoted_harness

            # Solidify reading promoted/promoted_model.py and src/model_harness/core.py via FSBase
            pm_file_path = self.promoted_dir / "promoted_model.py"
            harness_file_path = self.repo_path / "src" / "model_harness" / "core.py"
            if self.promoted_fs.client.exists(str(pm_file_path)):
                pbuf = io.BytesIO()
                self.promoted_fs.read(str(pm_file_path), pbuf)
                _ = pbuf.getvalue()
            if self.promoted_fs.client.exists(str(harness_file_path)):
                hbuf = io.BytesIO()
                self.promoted_fs.read(str(harness_file_path), hbuf)
                _ = hbuf.getvalue()

            import promoted.promoted_model as pm_mod
            import src.model_harness as h_mod

            if force_replace_from_promoted_file or self._promoted_harness is not None:
                importlib.reload(pm_mod)
                importlib.reload(h_mod)

            custom_model_instance = None
            if self._selected_chat_model_id and not force_replace_from_promoted_file:
                custom_model_instance = self._instantiate_model_by_id_or_name(
                    self._selected_chat_model_id
                )

            if custom_model_instance is not None:
                harness = h_mod.PromotedModelHarness(
                    model=custom_model_instance,
                    vocab_size=256,
                    d_model=128,
                    device="cpu",
                )
            else:
                fresh_model = pm_mod.build_promoted_model(
                    d_model=128,
                    vocab_size=256,
                    device="cpu",
                )
                harness = h_mod.PromotedModelHarness(
                    model=fresh_model,
                    vocab_size=256,
                    d_model=128,
                    device="cpu",
                )

            seed_name = type(harness.model).__name__
            if not force_replace_from_promoted_file:
                harness.memorize(
                    f"{seed_name} PromotedModelHarness "
                    "CohesiveUnifiedMonadicCavityModel PolyphonicStageCompilerCascadeModel "
                    "MonadicCavity AnyGram 6-Scale Cech Cohesive StageCompiler vCPU Poincare Geodesic\n"
                )
            self._promoted_harness = harness
            self._promoted_harness_class = type(harness.model).__name__
            return self._promoted_harness

    def _instantiate_model_by_id_or_name(self, model_id_or_name: str) -> Any:
        """
        Resolves any evaluated model dynamically from Qdrant (`portfolio_neural_benchmarks`)
        by `model_id`, `class_name`, or `model_name`, instantiates the PyTorch `nn.Module`,
        and ensures it exposes a 6-scale `CohesiveAnyGramEngine` so `PromotedModelHarness` can chat with it.
        """
        import importlib
        import torch

        overview = self._load_state_from_qdrant()
        all_models = list(overview.get("models") or [])
        q = (model_id_or_name or "").strip()
        q_low = q.lower()
        matched_meta: dict[str, Any] | None = None
        for m in all_models:
            if (
                str(m.get("model_id", "")).lower() == q_low
                or str(m.get("class_name", "")).lower() == q_low
                or str(m.get("model_name", "")).lower() == q_low
            ):
                matched_meta = dict(m)
                break
        if matched_meta is None:
            for m in all_models:
                if (
                    q_low in str(m.get("model_id", "")).lower()
                    or q_low in str(m.get("class_name", "")).lower()
                    or q_low in str(m.get("model_name", "")).lower()
                ):
                    matched_meta = dict(m)
                    break

        if matched_meta is None:
            return None

        self._selected_chat_model_meta = matched_meta
        self._selected_chat_model_id = str(matched_meta.get("model_id") or q)
        cls_name = str(matched_meta.get("class_name") or "")
        src_file = str(matched_meta.get("source_file") or "").strip()

        candidate_modules: list[str] = []
        if src_file.endswith(".py"):
            dyn_mod = src_file[:-3].strip("/").replace("/", ".")
            if dyn_mod:
                candidate_modules.append(dyn_mod)
        for fallback_mod in (
            "promoted.promoted_model",
            "src.models.stage_compiler.stage_compiler_model",
            "src.models.monadic_cavity.monadic_cavity_models",
        ):
            if fallback_mod not in candidate_modules:
                candidate_modules.append(fallback_mod)
        target_cls = None
        for mod_name in candidate_modules:
            try:
                mod = importlib.import_module(mod_name)
                if hasattr(mod, cls_name):
                    target_cls = getattr(mod, cls_name)
                    break
            except Exception:
                continue

        instance = None
        if target_cls is not None:
            for kwargs_try in (
                {"d_model": 128, "vocab_size": 256, "device": "cpu"},
                {"d_model": 128, "vocab_size": 256},
                {"vocab_size": 256, "d_model": 128},
                {},
            ):
                try:
                    instance = target_cls(**kwargs_try)
                    break
                except Exception:
                    continue

        if instance is None:
            import promoted.promoted_model as pm_mod

            instance = pm_mod.build_promoted_model(d_model=128, vocab_size=256, device="cpu")

        # Ensure baseline models also expose `.any_gram` and accept `return_logits=True`
        from src.polyphonic_model.any_gram.cohesive_any_gram import CohesiveAnyGramEngine

        if not hasattr(instance, "any_gram") and not hasattr(instance, "anygram_core") and not hasattr(instance, "cavity_core"):
            d_mod = int(getattr(instance, "d_model", 128))
            v_sz = int(getattr(instance, "vocab_size", 256))
            instance.any_gram = CohesiveAnyGramEngine(
                vocab_size=v_sz,
                coord_dim=min(64, d_mod),
                device="cpu",
                dtype=torch.float32,
            )

        orig_forward = instance.forward
        import inspect

        sig = inspect.signature(orig_forward)
        if "return_logits" not in sig.parameters and not any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        ):
            def _wrapped_forward(x: Any, *args: Any, **kwargs: Any) -> Any:
                kwargs.pop("return_logits", None)
                kwargs.pop("return_telemetry", None)
                out = orig_forward(x, *args, **kwargs)
                if isinstance(out, tuple):
                    return out[1] if len(out) > 1 and hasattr(out[1], "shape") else out[0]
                return out

            instance.forward = _wrapped_forward  # type: ignore[method-assign]

        return instance

    def get_available_chat_models(
        self,
        overview: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return all evaluated models dynamically from Qdrant (`portfolio_neural_benchmarks`) for the chat model selector dropdown."""
        with self._lock:
            resolved_overview = overview if overview is not None else self._load_state_from_qdrant()
            models = list(resolved_overview.get("models") or [])
            result: list[dict[str, Any]] = []
            for m in models:
                result.append(
                    {
                        "model_id": str(m.get("model_id") or ""),
                        "model_name": str(m.get("model_name") or ""),
                        "class_name": str(m.get("class_name") or ""),
                        "friendly_title": str(m.get("friendly_title") or ""),
                        "family": str(m.get("family") or ""),
                        "overall_rank": int(m.get("overall_rank") or 99),
                        "mean_rank": float(m.get("mean_rank") or 99.0),
                        "composite_score": float(m.get("composite_score") or 0.0),
                        "wins_count": int(m.get("wins_count") or 0),
                        "benchmarks_evaluated": int(m.get("benchmarks_evaluated") or 12),
                        "avg_num_params": int(m.get("avg_num_params") or 0),
                        "is_promoted": bool(m.get("is_promoted", False)),
                        "last_tested_at": str(m.get("last_tested_at") or ""),
                    }
                )
            return result

    def select_promoted_chat_model(self, model_id_or_name: str) -> dict[str, Any]:
        """Selects and loads one of the 25 models into `PromotedModelHarness` for chat."""
        with self._lock:
            clean_id = (model_id_or_name or "").strip()
            if not clean_id:
                raise ValueError("model_id cannot be empty")
            self._selected_chat_model_id = clean_id
            self._promoted_harness = None
            harness = self._ensure_promoted_harness(force_replace_from_promoted_file=False)
            status = self.get_promoted_model_status()
            active_model = self._selected_chat_model_meta or status.get("promoted_model")
            return {
                "status": "model_selected",
                "selected_model_id": self._selected_chat_model_id,
                "selected_model": active_model,
                "active_chat_model": active_model,
                "harness_class": type(harness.model).__name__,
                "harness_status": status["harness_status"],
            }

    def reset_promoted_knowledge(self) -> dict[str, Any]:
        """
        Replaces the active model in `PromotedModelHarness` directly with the file in
        `/Users/jarek/IdeaProjects/neural_networks/promoted/promoted_model.py` read via `FSBase`,
        clears the 6-scale Any-Gram memory store, and returns updated telemetry.
        """
        with self._lock:
            import io

            pm_file_path = self.promoted_dir / "promoted_model.py"
            pbuf = io.BytesIO()
            self.promoted_fs.read(str(pm_file_path), pbuf)
            pm_bytes = pbuf.getvalue()
            pm_sha256 = hashlib.sha256(pm_bytes).hexdigest()

            harness = self._ensure_promoted_harness(force_replace_from_promoted_file=True)
            harness.reset_memory()
            h_status = dict(harness.status())
            status = self.get_promoted_model_status()
            return {
                "status": "reset",
                "reset": True,
                "replaced_from_promoted_file": True,
                "promoted_file_path": str(pm_file_path),
                "promoted_file_sha256": pm_sha256,
                "class_name": type(harness.model).__name__,
                "total_store_entries": int(h_status.get("total_store_entries", 0)),
                "harness_status": h_status,
                "active_chat_model": status.get("promoted_model"),
            }

    @staticmethod
    def _extract_clean_promoted_agent_text(raw_text: str, prev_user_text: str = "") -> str:
        """
        Strips legacy `Autoregressive Output` wrappers, code fences, and predicted/telemetry
        bullet lines from an agent reply, returning only the agent text.
        """
        import re

        text = str(raw_text or "")
        if "Autoregressive Output" in text and "```" in text:
            m = re.search(r"```(?:text|python)?\n([\s\S]*?)\n```", text)
            if m:
                text = m.group(1)
        else:
            telemetry_prefixes = (
                "- **Action**:",
                "- **Callan-Symanzik RG-Flow Learning**:",
                "- **Top-5 Next Tokens**:",
                "- **6-Scale Any-Gram Store Entries**:",
                "- **Poincaré Ball Curvature",
                "- **O(1) Any-Gram Memorization**:",
                "- **Knowledge Reset**:",
            )
            kept_lines = [
                ln
                for ln in text.splitlines()
                if not ln.strip().startswith(telemetry_prefixes)
                and "Autoregressive Output**:" not in ln
            ]
            text = "\n".join(kept_lines)

        clean_prev = (prev_user_text or "").strip()
        if clean_prev and text.startswith(clean_prev):
            continuation = text[len(clean_prev) :].strip()
            if continuation:
                return continuation
        return text.strip()

    @classmethod
    def _sanitize_promoted_conversation_messages(
        cls,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Normalizes promoted chat messages to contain only user text and agent text with timestamps
        (dropping synthetic seed/sample banners, predicted-token telemetry, thoughts, and artifact chips).
        """
        cleaned: list[dict[str, Any]] = []
        last_user_text = ""
        for raw_msg in messages or []:
            if not isinstance(raw_msg, dict):
                continue
            entry_id = str(raw_msg.get("entry_id") or "")
            raw_body = str(raw_msg.get("message_text") or "")
            if entry_id.startswith("promoted_chat_init_") or raw_body.lstrip().startswith(
                "### Promoted Model Harness Online"
            ):
                continue
            msg = dict(raw_msg)
            role = str(msg.get("message_role") or "AGENT").upper()
            if role == "USER":
                user_body = raw_body.strip()
                last_user_text = user_body
                msg["message_text"] = user_body
                msg["snippet"] = user_body[:200]
            else:
                agent_body = cls._extract_clean_promoted_agent_text(raw_body, last_user_text)
                if not agent_body:
                    agent_body = raw_body.strip()
                msg["message_text"] = agent_body
                msg["snippet"] = agent_body[:200]
            msg["agent_thoughts"] = ""
            msg["artifact_items"] = []
            msg["artifact_count"] = 0
            cleaned.append(msg)
        return cleaned

    def execute_promoted_fs_crud(self, action: str, **kwargs: Any) -> dict[str, Any]:
        """Executes `FSBaseCrudService` operations on the promoted repo and staging Qdrant workspace."""
        return self.fs_crud.execute_crud(action=action, **kwargs)

    def _load_conversations_from_staging_qdrant(self) -> None:
        """
        Loads saved promoted model conversations from `/Users/jarek/IdeaProjects/qdrant_staging/storage`
        (`promoted_model_conversations`) and `FSBase` (`qdrant_staging/promoted_workspace/conversations/`).
        """
        with self._lock:
            import io
            import json

            pts = self.fs_crud.staging_manager.read_points_lockfree(
                self.fs_crud.staging_manager.storage_dir,
                self.conversations_collection_name,
            )
            for pt in pts:
                pl = pt.payload or {}
                if pl.get("doc_type") == "promoted_conversation":
                    cid = str(pl.get("conversation_id") or "")
                    if cid:
                        rec = dict(pl)
                        sanitized = self._sanitize_promoted_conversation_messages(
                            list(rec.get("messages") or [])
                        )
                        rec["messages"] = sanitized
                        rec["message_count"] = len(sanitized)
                        self._conversations_by_id[cid] = rec

            # Also check FSBase conversation JSON files in qdrant_staging/promoted_workspace/conversations
            conv_dir = self.fs_crud.staging_conversations_dir
            if conv_dir.is_dir():
                for _, files in self.promoted_fs.walk_files(str(conv_dir)):
                    for fname in sorted(files):
                        if not fname.endswith(".json"):
                            continue
                        fpath = conv_dir / fname
                        try:
                            buf = io.BytesIO()
                            self.promoted_fs.read(str(fpath), buf)
                            data = json.loads(buf.getvalue().decode("utf-8"))
                            cid = str(data.get("conversation_id") or "")
                            if cid and cid not in self._conversations_by_id:
                                sanitized = self._sanitize_promoted_conversation_messages(
                                    list(data.get("messages") or [])
                                )
                                data["messages"] = sanitized
                                data["message_count"] = len(sanitized)
                                self._conversations_by_id[cid] = data
                        except Exception:
                            continue

            if self._conversations_by_id:
                sorted_convs = sorted(
                    self._conversations_by_id.values(),
                    key=lambda c: int(c.get("sort_ts_ms") or 0),
                    reverse=True,
                )
                active_conv = self._conversations_by_id.get(self._active_conversation_id) or sorted_convs[0]
                self._active_conversation_id = str(active_conv["conversation_id"])
                self._promoted_chat_messages = list(active_conv.get("messages") or [])

    def _persist_conversation_to_staging_qdrant(self, conv_id: str) -> dict[str, Any]:
        """
        Persists a promoted model conversation and its messages to staging Qdrant
        (`/Users/jarek/IdeaProjects/qdrant_staging/storage`, collection `promoted_model_conversations`)
        and backs up the JSON transcript via `FSBase` to `qdrant_staging/promoted_workspace/conversations/`.
        """
        with self._lock:
            import datetime as dt
            import io
            import json

            messages = self._sanitize_promoted_conversation_messages(
                list(self._promoted_chat_messages)
            )
            self._promoted_chat_messages = list(messages)
            now_ms = int(time.time() * 1000)
            now_str = dt.datetime.fromtimestamp(now_ms / 1000.0, tz=dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            existing = self._conversations_by_id.get(conv_id) or {}
            created_at = str(existing.get("created_at") or now_str)
            user_msgs = [m for m in messages if m.get("message_role") == "USER"]
            default_title = (
                str(user_msgs[0].get("message_text", ""))[:56]
                if user_msgs
                else f"Session — {self._promoted_harness_class or 'PromotedModel'}"
            )
            title = str(existing.get("title") or default_title)
            if user_msgs and title.startswith("Session —"):
                title = str(user_msgs[0].get("message_text", ""))[:56]

            conv_record = {
                "doc_type": "promoted_conversation",
                "conversation_id": conv_id,
                "title": title,
                "model_id": self._selected_chat_model_id or "promoted_default",
                "class_name": self._promoted_harness_class or "PromotedModel",
                "created_at": created_at,
                "updated_at": now_str,
                "sort_ts_ms": now_ms,
                "message_count": len(messages),
                "messages": messages,
            }
            self._conversations_by_id[conv_id] = conv_record

            # Backup via FSBase to qdrant_staging/promoted_workspace/conversations/<conv_id>.json
            conv_file = self.fs_crud.staging_conversations_dir / f"{conv_id}.json"
            raw_json = json.dumps(conv_record, indent=2).encode("utf-8")
            self.promoted_fs.write(str(conv_file), io.BytesIO(raw_json))

            # Upsert into staging Qdrant `promoted_model_conversations`
            pt_id = str(
                uuid5_from_args(
                    INFORMATIONAL_NAMESPACE_UUID,
                    self.conversations_collection_name,
                    conv_id,
                )
            )
            text_blob = f"{conv_id} {title} {conv_record['class_name']} " + " ".join(
                str(m.get("message_text", ""))[:200] for m in messages[-6:]
            )
            points = [
                PointStruct(
                    id=pt_id,
                    vector=_deterministic_384d_vector(text_blob),
                    payload=conv_record,
                )
            ]
            staged_total = self.fs_crud.staging_manager.upsert_points_lockfree(
                storage_path=self.fs_crud.staging_manager.storage_dir,
                collection_name=self.conversations_collection_name,
                points=points,
                recreate=False,
            )
            conv_record["staged_qdrant_points"] = staged_total
            return conv_record

    def list_promoted_conversations(self) -> list[dict[str, Any]]:
        """Returns summaries of all tracked promoted model conversations from staging Qdrant."""
        with self._lock:
            if not self._conversations_by_id and self._promoted_chat_messages:
                self._persist_conversation_to_staging_qdrant(self._active_conversation_id)
            items: list[dict[str, Any]] = []
            for cid, rec in self._conversations_by_id.items():
                msgs = rec.get("messages") or []
                items.append(
                    {
                        "conversation_id": cid,
                        "title": str(rec.get("title") or cid),
                        "model_id": str(rec.get("model_id") or "promoted_default"),
                        "class_name": str(rec.get("class_name") or "PromotedModel"),
                        "created_at": str(rec.get("created_at") or ""),
                        "updated_at": str(rec.get("updated_at") or ""),
                        "sort_ts_ms": int(rec.get("sort_ts_ms") or 0),
                        "message_count": len(msgs),
                        "is_active": cid == self._active_conversation_id,
                    }
                )
            items.sort(key=lambda x: (-int(x["sort_ts_ms"]), x["conversation_id"]))
            return items

    def create_promoted_conversation(
        self,
        title: str = "",
        model_id: str = "",
    ) -> dict[str, Any]:
        """Creates a new promoted model conversation in staging Qdrant and makes it active."""
        with self._lock:
            if model_id:
                self.select_promoted_chat_model(model_id)
            now_ms = int(time.time() * 1000)
            new_cid = f"promoted_conv_{now_ms}"
            self._active_conversation_id = new_cid
            self._promoted_chat_messages = []
            if title.strip():
                self._conversations_by_id[new_cid] = {
                    "conversation_id": new_cid,
                    "title": title.strip(),
                    "sort_ts_ms": now_ms,
                    "messages": [],
                }
            status = self.get_promoted_model_status()
            conv_rec = self._persist_conversation_to_staging_qdrant(new_cid)
            return {
                "status": "created",
                "active_conversation_id": new_cid,
                "conversation": conv_rec,
                "conversations": self.list_promoted_conversations(),
                "messages": status["chat_messages"],
            }

    def open_promoted_conversation(self, conversation_id: str) -> dict[str, Any]:
        """Re-opens a tracked promoted model conversation from staging Qdrant by `conversation_id`."""
        with self._lock:
            cid = (conversation_id or "").strip()
            if not cid:
                raise ValueError("conversation_id cannot be empty")
            if cid not in self._conversations_by_id:
                self._load_conversations_from_staging_qdrant()
            if cid not in self._conversations_by_id:
                raise KeyError(f"Unknown promoted conversation in staging Qdrant: {cid}")
            rec = self._conversations_by_id[cid]
            self._active_conversation_id = cid
            sanitized = self._sanitize_promoted_conversation_messages(
                list(rec.get("messages") or [])
            )
            rec["messages"] = sanitized
            rec["message_count"] = len(sanitized)
            self._promoted_chat_messages = list(sanitized)
            return {
                "status": "opened",
                "active_conversation_id": cid,
                "conversation": rec,
                "conversations": self.list_promoted_conversations(),
                "messages": list(self._promoted_chat_messages),
            }

    def _read_repo_file_via_fs(self, rel_path: str) -> str:
        from benchmark_ledger.vocab_mask import mask_restricted_string

        target = self.repo_path / mask_restricted_string(rel_path)
        if not target.exists():
            target = self.repo_path / rel_path
        if not target.exists():
            return ""
        try:
            import io

            buf = io.BytesIO()
            self.promoted_fs.read(str(target), buf)
            return mask_restricted_string(buf.getvalue().decode("utf-8"))
        except Exception:
            return ""

    def lookup_promoted_model_in_qdrant(
        self,
        overview: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Queries Qdrant (`portfolio_neural_benchmarks` and `neural_networks_code_corpus`)
        for the promoted model saved by `neural_networks` (`is_promoted=True`), verifying
        chunk rejoin integrity and returning the Qdrant point metadata.
        """
        with self._lock:
            import json
            import pickle
            import sqlite3

            self.ensure_indexed(force=False)
            resolved_overview = overview if overview is not None else self._load_state_from_qdrant()
            promoted_model = (
                resolved_overview.get("promoted_model")
                or resolved_overview.get("best_model")
                or {}
            )
            target_model_id = str(promoted_model.get("model_id", ""))
            target_class_name = str(promoted_model.get("class_name", ""))

            # 1. Query portfolio_neural_benchmarks Qdrant collection for is_promoted=True
            client = self.qdrant_graph._ensure_qdrant_client()
            qdrant_benchmark_points: list[dict[str, Any]] = []
            try:
                scrolled, _ = client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=Filter(
                        must=[FieldCondition(key="is_promoted", match=MatchValue(value=True))]
                    ),
                    limit=32,
                    with_payload=True,
                    with_vectors=False,
                )
                for pt in scrolled:
                    if pt.payload:
                        qdrant_benchmark_points.append(dict(pt.payload))
            except Exception:
                pass

            if not qdrant_benchmark_points and target_model_id:
                qdrant_benchmark_points = list(
                    self._index.load_from_index("CHUNKS_BY_DOC", target_model_id) or []
                )

            rejoined = rejoin_chunks(qdrant_benchmark_points) if qdrant_benchmark_points else {}

            # 2. Query neural_networks_code_corpus (Qdrant SQLite & code_corpus_ledger.json),
            # keyed by file mtime_ns so status polls avoid repeated unindexed SQLite blob scans
            nn_qdrant_record: dict[str, Any] | None = None
            sqlite_candidates = [
                self.repo_path.parent
                / "qdrant_staging"
                / "storage"
                / "collection"
                / "neural_networks_code_corpus"
                / "storage.sqlite",
                self.repo_path
                / ".qdrant_code_index"
                / "collection"
                / "neural_networks_code_corpus"
                / "storage.sqlite",
            ]
            for sq_path in sqlite_candidates:
                if not sq_path.is_file():
                    continue
                try:
                    mtime_ns = sq_path.stat().st_mtime_ns
                    cache_key = (str(sq_path), mtime_ns, target_class_name)
                    if cache_key in self._nn_corpus_record_cache:
                        cached_rec = self._nn_corpus_record_cache[cache_key]
                        if cached_rec is not None:
                            nn_qdrant_record = dict(cached_rec)
                            break
                        continue
                    found_rec: dict[str, Any] | None = None
                    with sqlite3.connect(str(sq_path), timeout=3.0) as conn:
                        rows = conn.execute("SELECT point FROM points").fetchall()
                    for (blob,) in rows:
                        pt_obj = pickle.loads(blob)
                        pl = getattr(pt_obj, "payload", None) or {}
                        if pl.get("doc_type") == "indexed_model" and (
                            pl.get("is_promoted") is True
                            or pl.get("class_name") == target_class_name
                        ):
                            found_rec = dict(pl)
                            found_rec["source_collection"] = "neural_networks_code_corpus"
                            found_rec["storage_sqlite"] = str(sq_path)
                            if pl.get("is_promoted") is True:
                                break
                    self._nn_corpus_record_cache[cache_key] = found_rec
                    if found_rec is not None:
                        nn_qdrant_record = dict(found_rec)
                        break
                except Exception:
                    continue

            if nn_qdrant_record is None:
                code_ledger_path = (
                    self.repo_path
                    / "src"
                    / "models"
                    / "monadic_cavity"
                    / "results"
                    / "code_corpus_ledger.json"
                )
                if code_ledger_path.is_file():
                    try:
                        ledger_data = json.loads(code_ledger_path.read_text(encoding="utf-8"))
                        for m_item in ledger_data.get("indexed_models", []):
                            if isinstance(m_item, dict) and (
                                m_item.get("is_promoted") is True
                                or m_item.get("class_name") == target_class_name
                            ):
                                nn_qdrant_record = dict(m_item)
                                nn_qdrant_record["source_collection"] = "neural_networks_code_corpus"
                                if m_item.get("is_promoted") is True:
                                    break
                    except Exception:
                        pass

            from benchmark_ledger.vocab_mask import mask_restricted_vocabulary

            return mask_restricted_vocabulary(
                {
                    "found_in_qdrant": bool(qdrant_benchmark_points or nn_qdrant_record),
                    "benchmark_collection": self.collection_name,
                    "benchmark_doc_id": target_model_id,
                    "benchmark_chunks_count": len(qdrant_benchmark_points),
                    "rejoin_verified": bool(rejoined.get("integrity_verified", False)),
                    "rejoined_sha256": str(rejoined.get("rejoined_sha256", "")),
                    "neural_networks_code_corpus_record": nn_qdrant_record or {},
                }
            )

    def get_promoted_model_status(self) -> dict[str, Any]:
        """
        Return the promoted gauntlet winner manifest (`promoted/promoted_manifest.json`),
        Qdrant lookup provenance (`qdrant_lookup`), live `PromotedModelHarness` runtime telemetry,
        source files (`promoted/promoted_model.py` and `promoted/harness.py`), sample generation,
        active Promoted Model chat messages, tracked conversations in staging Qdrant,
        available chat models for the dropdown, and the `FSBase` artifact/multimedia file tree.
        """
        with self._lock:
            self.ensure_indexed(force=False)
            overview = self._load_state_from_qdrant()
            harness = self._ensure_promoted_harness()
            from promoted.promoted_model import get_promoted_manifest

            h_status = dict(harness.status())
            h_status.setdefault("num_params", h_status.get("num_parameters", 554400))
            manifest = overview.get("promoted_manifest") or get_promoted_manifest()
            default_promoted = overview.get("promoted_model") or overview.get("best_model")
            promoted_model = (
                dict(self._selected_chat_model_meta)
                if self._selected_chat_model_meta
                else default_promoted
            )
            qdrant_lookup = self.lookup_promoted_model_in_qdrant(overview=overview)
            default_prompt = "Cohesive"
            sample_key = (
                id(harness),
                self._promoted_harness_class,
                int(h_status.get("step_count") or 0),
                int(h_status.get("total_store_entries") or 0),
            )
            if self._sample_gen_cache is not None and self._sample_gen_cache[0] == sample_key:
                next_pred = self._sample_gen_cache[1]
                generated_sample = self._sample_gen_cache[2]
            else:
                next_pred = harness.predict_next(default_prompt, top_k=5)
                generated_sample = harness.generate(
                    default_prompt,
                    max_new_tokens=28,
                    temperature=0.8,
                    top_k=8,
                )
                self._sample_gen_cache = (sample_key, next_pred, generated_sample)
            chat_messages = self._ensure_promoted_chat_seeded(
                promoted_model=promoted_model,
                manifest=manifest,
                harness_status=h_status,
                qdrant_lookup=qdrant_lookup,
                generated_sample=str(generated_sample),
            )
            available_models = self.get_available_chat_models(overview=overview)
            selected_model_id = self._selected_chat_model_id or str(
                (promoted_model or {}).get("model_id") or ""
            )
            return {
                "promoted_model": promoted_model,
                "active_chat_model": promoted_model,
                "default_promoted_model": default_promoted,
                "selected_model_id": selected_model_id,
                "available_models": available_models,
                "active_conversation_id": self._active_conversation_id,
                "conversations": self.list_promoted_conversations(),
                "fs_tree": self.fs_crud.list_tree()["tree"],
                "manifest": manifest,
                "qdrant_lookup": qdrant_lookup,
                "harness_status": h_status,
                "default_prompt": default_prompt,
                "generated_text": generated_sample,
                "next_token_prediction": next_pred,
                "promoted_model_source": self._read_repo_file_via_fs("promoted/promoted_model.py"),
                "harness_source": self._read_repo_file_via_fs("src/model_harness/core.py"),
                "chat_messages": chat_messages,
            }

    def _ensure_promoted_chat_seeded(
        self,
        promoted_model: dict[str, Any] | None = None,
        manifest: dict[str, Any] | None = None,
        harness_status: dict[str, Any] | None = None,
        qdrant_lookup: dict[str, Any] | None = None,
        generated_sample: str = "",
    ) -> list[dict[str, Any]]:
        """Ensure the active Promoted Model conversation record exists in staging Qdrant with clean user/agent messages only."""
        _ = (promoted_model, manifest, harness_status, qdrant_lookup, generated_sample)
        with self._lock:
            self._promoted_chat_messages = self._sanitize_promoted_conversation_messages(
                list(self._promoted_chat_messages)
            )
            if self._active_conversation_id not in self._conversations_by_id:
                self._persist_conversation_to_staging_qdrant(self._active_conversation_id)
            return list(self._promoted_chat_messages)

    def get_promoted_chat_history(self, conversation_id: str | None = None) -> dict[str, Any]:
        """Return the active Promoted Model chat history, Qdrant lookup metadata, and live harness status."""
        with self._lock:
            if conversation_id and conversation_id != self._active_conversation_id:
                try:
                    self.open_promoted_conversation(conversation_id)
                except KeyError:
                    pass
            status_payload = self.get_promoted_model_status()
            return {
                "conversation_id": self._active_conversation_id,
                "active_conversation_id": self._active_conversation_id,
                "conversations": status_payload["conversations"],
                "available_models": status_payload["available_models"],
                "selected_model_id": status_payload["selected_model_id"],
                "promoted_model": status_payload["promoted_model"],
                "active_chat_model": status_payload["promoted_model"],
                "fs_tree": status_payload["fs_tree"],
                "manifest": status_payload["manifest"],
                "qdrant_lookup": status_payload["qdrant_lookup"],
                "harness_status": status_payload["harness_status"],
                "messages": list(self._promoted_chat_messages),
                "total_messages": len(self._promoted_chat_messages),
            }

    def send_promoted_chat_message(
        self,
        message_text: str,
        action: str = "learn",
        steps: int = 3,
        max_new_tokens: int = 36,
        temperature: float = 0.7,
        top_k: int = 8,
        conversation_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Send a user chat message exclusively to the active model in `PromotedModelHarness`,
        executing the requested harness action (`learn`, `generate`, `memorize`, or `reset_memory`),
        persisting the conversation turn to staging Qdrant (`promoted_model_conversations`),
        and returning the user message, model response, and token stream chunks.
        """
        with self._lock:
            import datetime as dt

            clean_text = (message_text or "").strip()
            if not clean_text:
                raise ValueError("Message text cannot be empty")

            if model_id and model_id != self._selected_chat_model_id:
                self.select_promoted_chat_model(model_id)
            if conversation_id and conversation_id != self._active_conversation_id:
                if conversation_id in self._conversations_by_id:
                    self.open_promoted_conversation(conversation_id)
                else:
                    self._active_conversation_id = conversation_id
                    self._promoted_chat_messages = []

            # Ensure initial seed message exists
            self.get_promoted_model_status()

            eff_action = (action or "learn").strip().lower()
            if clean_text.lower().startswith("/reset"):
                eff_action = "reset_memory"
            elif clean_text.lower().startswith("/memorize "):
                eff_action = "memorize"
                clean_text = clean_text[10:].strip() or clean_text
            elif clean_text.lower().startswith("/generate "):
                eff_action = "generate"
                clean_text = clean_text[10:].strip() or clean_text
            elif clean_text.lower().startswith("/learn "):
                eff_action = "learn"
                clean_text = clean_text[7:].strip() or clean_text

            now_ns = time.time_ns()
            now_ms = now_ns // 1_000_000
            now_str = dt.datetime.fromtimestamp(now_ms / 1000.0, tz=dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )

            user_msg = {
                "entry_id": f"promoted_chat_u_{now_ms}",
                "conversation_id": self._active_conversation_id,
                "short_id": "promoted",
                "friendly_session_name": "Promoted Model Harness",
                "entry_type": "conversation_turn",
                "title": clean_text[:96],
                "subtitle": f"User Input · Mode: {eff_action}",
                "workspace_project": "neural_networks",
                "snippet": clean_text[:200],
                "message_role": "USER",
                "message_text": clean_text,
                "agent_thoughts": "",
                "artifact_items": [],
                "created_at": now_str,
                "updated_at": now_str,
                "mtime_ns": now_ns,
                "sort_ts_ms": now_ms,
                "code_block_count": 1 if "```" in clean_text else 0,
                "step_count": 1,
                "user_turn_count": 1,
                "tool_call_count": 0,
                "artifact_count": 0,
                "task_log_count": 0,
                "where_to_find": {
                    "primary_location": str(self.repo_path / "src" / "model_harness" / "core.py"),
                    "brain_dir": str(self.repo_path / "promoted"),
                    "transcript_path": str(self.repo_path / "promoted" / "promoted_manifest.json"),
                    "artifact_files": [],
                    "referenced_files": [],
                },
                "what_else_is_related": {
                    "related_modules": ["benchmark_ledger"],
                    "related_kis": [],
                    "related_models": [],
                    "related_benchmarks": [],
                    "related_conversations": [],
                },
            }

            tool_res = self.run_promoted_model_tool(
                {
                    "action": eff_action,
                    "prompt": clean_text,
                    "learn_text": clean_text,
                    "steps": steps,
                    "max_new_tokens": max_new_tokens,
                    "temperature": temperature,
                    "top_k": top_k,
                }
            )
            hs = tool_res["harness_status"]
            cls_name = str(hs.get("class_name", "PromotedModel"))
            gen_text = str(tool_res.get("generated_text") or "")
            agent_reply_text = (
                self._extract_clean_promoted_agent_text(gen_text, clean_text)
                or gen_text.strip()
                or clean_text
            )

            agent_ns = now_ns + 1_000_000
            agent_ms = agent_ns // 1_000_000
            agent_msg = {
                "entry_id": f"promoted_chat_a_{agent_ms}",
                "conversation_id": self._active_conversation_id,
                "short_id": "promoted",
                "friendly_session_name": f"Promoted Model — {cls_name}",
                "entry_type": "conversation_turn",
                "title": f"{cls_name} Response: {clean_text[:64]}",
                "subtitle": f"PromotedModelHarness · {eff_action}",
                "workspace_project": "neural_networks",
                "snippet": agent_reply_text[:200],
                "message_role": "AGENT",
                "message_text": agent_reply_text,
                "agent_thoughts": "",
                "artifact_items": [],
                "created_at": now_str,
                "updated_at": now_str,
                "mtime_ns": agent_ns,
                "sort_ts_ms": agent_ms,
                "code_block_count": 0,
                "step_count": int(hs.get("step_count", 1)),
                "user_turn_count": 0,
                "tool_call_count": 1,
                "artifact_count": 0,
                "task_log_count": 0,
                "where_to_find": user_msg["where_to_find"],
                "what_else_is_related": user_msg["what_else_is_related"],
            }

            self._promoted_chat_messages.append(user_msg)
            self._promoted_chat_messages.append(agent_msg)
            self._persist_conversation_to_staging_qdrant(self._active_conversation_id)

            words = agent_reply_text.split(" ")
            stream_chunks: list[str] = []
            step_size = max(1, len(words) // 10 or 1)
            for i in range(0, len(words), step_size):
                piece = " ".join(words[i : i + step_size])
                if i + step_size < len(words):
                    piece += " "
                stream_chunks.append(piece)

            return {
                "conversation_id": self._active_conversation_id,
                "conversations": self.list_promoted_conversations(),
                "action": eff_action,
                "user_message": user_msg,
                "agent_message": agent_msg,
                "stream_chunks": stream_chunks,
                "harness_status": hs,
                "tool_result": tool_res,
            }

    def get_promoted_artifact_detail(self, artifact_id: str) -> dict[str, Any] | None:
        """
        Return code or multimedia modal detail for `promoted/harness.py`, `promoted/promoted_model.py`,
        `promoted/promoted_manifest.json`, or any artifact/multimedia file stored in `FSBaseCrudService`.
        """
        with self._lock:
            clean_id = (artifact_id or "").strip()
            if clean_id in (
                "promoted_artifact_harness",
                "harness.py",
                "promoted/harness.py",
                "src/model_harness/core.py",
            ):
                code = self._read_repo_file_via_fs("src/model_harness/core.py")
                rel_file = "src/model_harness/core.py"
                title = "src/model_harness/core.py — PromotedModelHarness"
                lang = "python"
                full_path = str(self.repo_path / rel_file)
            elif clean_id in ("promoted_artifact_model", "promoted_model.py", "promoted/promoted_model.py"):
                code = self._read_repo_file_via_fs("promoted/promoted_model.py")
                rel_file = "promoted/promoted_model.py"
                title = "promoted/promoted_model.py — PromotedModel"
                lang = "python"
                full_path = str(self.repo_path / rel_file)
            elif clean_id in ("promoted_artifact_manifest", "promoted_manifest.json", "promoted/promoted_manifest.json"):
                code = self._read_repo_file_via_fs("promoted/promoted_manifest.json")
                rel_file = "promoted/promoted_manifest.json"
                title = "promoted/promoted_manifest.json — Promoted Manifest"
                lang = "json"
                full_path = str(self.repo_path / rel_file)
            else:
                try:
                    f_info = self.fs_crud.read_file(clean_id)
                except Exception:
                    return None
                code = str(f_info.get("content") or f_info.get("data_uri") or "")
                rel_file = str(f_info.get("relative_path") or f_info.get("name") or clean_id)
                title = f"{rel_file} — {f_info.get('file_type', 'artifact')}"
                lang = str(f_info.get("extension") or ".txt").lstrip(".") or "text"
                full_path = str(f_info.get("path") or clean_id)

            return {
                "entry_id": clean_id,
                "conversation_id": self._active_conversation_id,
                "short_id": "promoted",
                "entry_type": "artifact",
                "title": title,
                "subtitle": full_path,
                "workspace_project": "neural_networks",
                "snippet": code[:200],
                "message_role": "AGENT",
                "message_text": f"### {title}\n\n```{lang}\n{code}\n```",
                "agent_thoughts": "",
                "artifact_items": [],
                "created_at": "",
                "updated_at": "",
                "mtime_ns": 0,
                "step_count": 1,
                "user_turn_count": 0,
                "tool_call_count": 0,
                "artifact_count": 1,
                "task_log_count": 0,
                "code_block_count": 1,
                "code_blocks": [
                    {
                        "block_id": f"{clean_id}_block_0",
                        "step_index": 1,
                        "role": "ARTIFACT",
                        "title": title,
                        "language": lang,
                        "code": code,
                        "timestamp": "",
                        "file_path": full_path,
                    }
                ],
                "where_to_find": {
                    "primary_location": full_path,
                    "brain_dir": str(self.repo_path / "promoted"),
                    "transcript_path": str(self.repo_path / "promoted" / "promoted_manifest.json"),
                    "artifact_files": [full_path],
                    "referenced_files": [full_path],
                },
                "what_else_is_related": {
                    "related_modules": ["benchmark_ledger", "code_index"],
                    "related_kis": [],
                    "related_models": [],
                    "related_benchmarks": [],
                    "related_conversations": [],
                },
                "markdown": f"### {title}\n\n```{lang}\n{code}\n```",
                "qdrant_doc_id": clean_id,
                "qdrant_chunks_count": 1,
                "qdrant_rejoin": {
                    "rejoined_text": code,
                    "rejoined_sha256": hashlib.sha256(code.encode("utf-8")).hexdigest(),
                    "integrity_verified": True,
                },
            }

    def run_promoted_model_tool(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute an interactive operation on the live `PromotedModelHarness`:
        - `generate` / `predict`: Autoregressive generation + Top-K next-token distribution
        - `learn`: Online Riemannian RG-Flow step + 6-scale Any-Gram memory ingestion
        - `memorize`: Zero-backprop O(1) 6-scale Any-Gram memory ingestion
        - `reset_memory`: Replace the model with the file in `neural_networks/promoted/` via `FSBase`
          and clear 6-scale Any-Gram memory stores
        """
        with self._lock:
            t0 = time.perf_counter()
            self.ensure_indexed(force=False)
            from promoted.promoted_model import get_promoted_manifest

            action = str(payload.get("action") or "generate").strip().lower()
            if action == "reset_memory":
                reset_result = self.reset_promoted_knowledge()
                harness = self._ensure_promoted_harness()
            else:
                reset_result = None
                harness = self._ensure_promoted_harness()

            prompt = str(payload.get("prompt") or "Cohesive")
            if not prompt:
                prompt = "Cohesive"
            learn_text = str(
                payload.get("learn_text")
                or payload.get("text")
                or "CohesiveUnifiedMonadicCavityModel AnyGram 2-GRU Unified Cavity Poincare Geodesic"
            )
            max_new_tokens = max(1, min(int(payload.get("max_new_tokens", 32)), 128))
            temperature = max(0.05, min(float(payload.get("temperature", 0.8)), 2.0))
            top_k = max(1, min(int(payload.get("top_k", 8)), 32))
            steps = max(1, min(int(payload.get("steps", 3)), 20))
            lr = max(1e-5, min(float(payload.get("lr", 3e-3)), 5e-2))
            calibrate_gates = bool(payload.get("calibrate_gates", True))

            learn_result: dict[str, Any] | None = None
            memorize_result: dict[str, Any] | None = None

            if action == "learn":
                res = harness.learn_text(
                    learn_text,
                    steps=steps,
                    lr=lr,
                    ingest_memory=True,
                    calibrate_gates=calibrate_gates,
                )
                learn_result = dict(res.to_dict() if hasattr(res, "to_dict") else res)
                learn_result.setdefault("initial_ce_loss", learn_result.get("initial_loss", 0.0))
                learn_result.setdefault("final_ce_loss", learn_result.get("final_loss", 0.0))
            elif action == "memorize":
                memorize_result = dict(harness.memorize(learn_text))

            generated_text = harness.generate(
                prompt,
                max_new_tokens=max_new_tokens,
                temperature=temperature,
                top_k=top_k,
                ingest_prompt=(action != "reset_memory"),
            )
            next_pred = harness.predict_next(prompt, top_k=5)
            h_status = dict(harness.status())
            h_status.setdefault("num_params", h_status.get("num_parameters", 554400))
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)

            return {
                "status": "ok",
                "action": action,
                "elapsed_ms": elapsed_ms,
                "prompt": prompt,
                "learn_text": learn_text,
                "generated_text": generated_text,
                "next_token_prediction": next_pred,
                "learn_result": learn_result,
                "memorize_result": memorize_result,
                "reset_result": reset_result,
                "harness_status": h_status,
                "manifest": self.ledger.get_overview().get("promoted_manifest") or get_promoted_manifest(),
            }


