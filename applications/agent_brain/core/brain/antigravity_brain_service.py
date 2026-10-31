from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
import re
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

from benchmark_ledger.antigravity_brain import (
    ANTIGRAVITY_BRAIN_NAMESPACE_UUID,
    AntigravityBrainLedger,
    AntigravityBrainQdrantDocument,
)
from indexes.datasource_index.datasource_index import Datasource
from service_controller.service_controller import ServiceController
from simple_functions.simple_functions import uuid5_from_args
from stats_collector.stats_collector import new_stats
from superlative_times.superlative_times import SuperlativeTimes
from thread_safe.index import Index

from applications.agent_brain.core.brain.qdrant_chunk_provider import (
    MODEL_DIMENSIONS,
    QdrantChunkClientProvider,
    chunk_document_text,
    encode_texts_cpu,
    rejoin_chunks,
)

_DEFAULT_BRAIN_LEDGER = (
    Path(__file__).resolve().parents[2] / "data" / "antigravity_brain_ledger.json"
)


class AntigravityBrainService(ServiceController, Datasource):
    """
    Indexes the Antigravity Agent Brain (`/Users/jarek/.gemini/antigravity-cli/brain`) into Qdrant
    (`portfolio_antigravity_brain`) using `FSBase(filesystem="file")` and `PeriodicProducer` from
    the `pytools-toolset` skill.

    - Cards expose a small snippet of what was found alongside relational metadata ("Where to Find
      Things" and "What Else Is Related").
    - Modal detail returns the conversation broken up into structured code blocks (`USER_REQUEST`,
      `AGENT_THINKING`, `TOOL_CALLS`, `TOOL_OUTPUT`, `AGENT_RESPONSE`, `BRAIN_ARTIFACT`) plus
      lossless Qdrant SHA-256 chunk re-joining.
    """

    def __init__(
        self,
        qdrant_graph: QdrantChunkClientProvider,
        brain_root: str | Path = "/Users/jarek/.gemini/antigravity-cli/brain",
        ledger_path: str | Path = _DEFAULT_BRAIN_LEDGER,
        collection_name: str = "portfolio_antigravity_brain",
        refresh_interval_seconds: int = 20,
        watch_files: bool = True,
    ) -> None:
        super().__init__()
        self.controllers = []
        self.queues = []
        self.qdrant_graph = qdrant_graph
        self.brain_root = Path(brain_root)
        self.ledger_path = Path(ledger_path)
        self.collection_name = collection_name
        self.refresh_interval_seconds = max(0, int(refresh_interval_seconds))
        self.watch_files = bool(watch_files)

        self.ledger = AntigravityBrainLedger(
            brain_root=self.brain_root,
            ledger_path=self.ledger_path,
        )
        self._lock = threading.RLock()
        self._index = Index()
        for ns in ("ENTRIES", "DOCUMENTS", "CHUNKS_BY_DOC", "STATE"):
            self._index.new(ns)

        self.stats = new_stats()
        self.times = SuperlativeTimes()
        self._initialized = False
        self._last_brain_sig: str = ""
        self._last_sig_check_mono: float = 0.0
        self._doc_hashes: dict[str, str] = {}
        self._live_chat_entries: list[dict[str, Any]] = []

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
                    current_sig = self.ledger.get_live_brain_signature()
                    if current_sig == self._last_brain_sig:
                        return cached
            return self.scan_and_index(force_recreate=force)

    def scan_and_index(self, force_recreate: bool = False) -> dict[str, Any]:
        """
        Scans `/Users/jarek/.gemini/antigravity-cli/brain` via `FSBase`, builds conversation
        code blocks and relational metadata, and incrementally indexes changed documents into
        Qdrant (`portfolio_antigravity_brain`).
        """
        with self._lock:
            t0 = time.perf_counter()
            overview = self.ledger.scan_and_build(force=force_recreate)
            docs: list[AntigravityBrainQdrantDocument] = self.ledger.build_qdrant_documents()

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
                for ns in ("ENTRIES", "DOCUMENTS", "CHUNKS_BY_DOC"):
                    self._index.delete_index(ns)
                    self._index.new(ns)

            for entry_dict in overview.get("entries", []):
                self._index.store_in_index("ENTRIES", entry_dict["entry_id"], entry_dict)

            existing_qdrant_meta: dict[str, dict[str, Any]] = {}
            if collection_exists and not force_recreate:
                try:
                    scrolled_zero, _ = client.scroll(
                        collection_name=self.collection_name,
                        scroll_filter=Filter(
                            must=[FieldCondition(key="chunk_index", match=MatchValue(value=0))]
                        ),
                        limit=4000,
                        with_payload=True,
                        with_vectors=False,
                    )
                    for pt in scrolled_zero:
                        pl = pt.payload or {}
                        if pl.get("doc_id"):
                            existing_qdrant_meta[str(pl["doc_id"])] = dict(pl)
                except Exception:
                    existing_qdrant_meta = {}

            changed_chunk_records: list[dict[str, Any]] = []
            embed_inputs: list[str] = []
            changed_docs: list[AntigravityBrainQdrantDocument] = []
            chunks_by_doc: dict[str, list[dict[str, Any]]] = {}

            for doc in docs:
                full_text = doc.markdown.strip()
                text_sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
                meta_sha256 = hashlib.sha256(
                    ("v4|" + full_text + "|" + json.dumps(doc.json_metadata or {}, sort_keys=True)).encode("utf-8")
                ).hexdigest()
                if (
                    not force_recreate
                    and self._doc_hashes.get(doc.doc_id) == meta_sha256
                    and self._index.load_from_index("DOCUMENTS", doc.doc_id) is not None
                ):
                    existing_chunks = self._index.load_from_index("CHUNKS_BY_DOC", doc.doc_id) or []
                    chunks_by_doc[doc.doc_id] = existing_chunks
                    continue

                raw_chunks = chunk_document_text(
                    full_text,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                )
                total_c = len(raw_chunks)
                point_ids = [
                    uuid5_from_args(
                        ANTIGRAVITY_BRAIN_NAMESPACE_UUID,
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
                    rec = {
                        "point_id": point_ids[idx],
                        "doc_id": doc.doc_id,
                        "entry_id": doc.entry_id,
                        "conversation_id": doc.conversation_id,
                        "friendly_session_name": doc.friendly_session_name,
                        "doc_type": doc.doc_type,
                        "title": doc.title,
                        "subtitle": doc.subtitle,
                        "workspace_project": doc.workspace_project,
                        "snippet": doc.snippet,
                        "primary_location": doc.primary_location,
                        "brain_dir": doc.brain_dir,
                        "transcript_path": doc.transcript_path,
                        "referenced_files": doc.referenced_files,
                        "artifact_names": doc.artifact_names,
                        "related_modules": doc.related_modules,
                        "related_kis": doc.related_kis,
                        "related_models": doc.related_models,
                        "related_benchmarks": doc.related_benchmarks,
                        "related_conversation_ids": doc.related_conversation_ids,
                        "code_block_count": doc.code_block_count,
                        "step_count": doc.step_count,
                        "updated_at": doc.updated_at,
                        "sort_ts_ms": int((doc.json_metadata or {}).get("sort_ts_ms", 0)),
                        "json_metadata": doc.json_metadata or {},
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
                    doc_chunk_list.append(rec)

                prev_pl = existing_qdrant_meta.get(doc.doc_id)
                is_unchanged_in_qdrant = (
                    not force_recreate
                    and prev_pl is not None
                    and str(prev_pl.get("content_sha256") or "") == text_sha256
                    and int(prev_pl.get("total_chunks") or -1) == total_c
                    and str(prev_pl.get("embedding_model") or "") == model_name
                )
                if is_unchanged_in_qdrant:
                    self._doc_hashes[doc.doc_id] = meta_sha256
                    chunks_by_doc[doc.doc_id] = doc_chunk_list
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
                self._doc_hashes[doc.doc_id] = meta_sha256
                for rec in doc_chunk_list:
                    changed_chunk_records.append(rec)
                    embed_inputs.append(
                        f"[{doc.doc_type}] {doc.title} ({doc.workspace_project}) — {rec['chunk_text']}"
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
            live_sig = str(
                overview.get("live_signature")
                or self.ledger.get_live_brain_signature()
                or ""
            )
            self._last_brain_sig = live_sig
            status = {
                "collection_name": self.collection_name,
                "brain_root": str(self.brain_root),
                "ledger_path": str(self.ledger_path),
                "embedding_model": model_name,
                "vector_dimensions": vector_dim,
                "total_documents": len(docs),
                "total_chunks_indexed": total_chunks_count,
                "incremental_updated_documents": len(changed_docs),
                "incremental_upserted_chunks": len(changed_chunk_records),
                "rejoin_verified_documents": rejoin_verified_count,
                "primary_conversations_count": int(overview.get("primary_conversations_count", 0)),
                "conversation_segments_count": int(overview.get("conversation_segments_count", 0)),
                "artifacts_count": int(overview.get("artifacts_count", 0)),
                "subagent_sessions_count": int(overview.get("subagent_sessions_count", 0)),
                "conversation_tabs": overview.get("conversation_tabs", []),
                "projects_breakdown": overview.get("projects_breakdown", {}),
                "fsbase_telemetry": overview.get("fsbase_telemetry", {}),
                "periodic_refresh_interval_seconds": self.refresh_interval_seconds if self.is_watcher_active else 0,
                "periodic_refresh_active": self.is_watcher_active,
                "last_indexed_ms": elapsed_ms,
                "last_synced_at": str(overview.get("last_synced_at", "")),
                "live_signature": live_sig,
            }
            self._last_sig_check_mono = time.monotonic()
            self._index.store_in_index("STATE", "STATUS", status)
            self._initialized = True
            return status

    def _to_card_summary(
        self,
        entry: dict[str, Any],
        matched_snippet: str | None = None,
        search_score: float | None = None,
        semantic_score: float | None = None,
    ) -> dict[str, Any]:
        """
        Formats an entry for the infinite-scrolling card view, exposing ONLY a small snippet
        of what was found plus compact "Where to Find Things" and "What Else Is Related" metadata.
        """
        raw_snippet = matched_snippet or str(entry.get("snippet", ""))
        clean_snip = re.sub(r"\s+", " ", raw_snippet).strip()
        if len(clean_snip) > 210:
            clean_snip = clean_snip[:207].rsplit(" ", 1)[0] + "..."

        locs = entry.get("locations") or {}
        rels = entry.get("relations") or {}
        cbs = entry.get("code_blocks") or []
        doc_rec = self._index.load_from_index("DOCUMENTS", entry["entry_id"]) or {}

        block_types: list[str] = []
        for b in cbs:
            bt = str(b.get("badge_label") or b.get("block_type") or "")
            if bt and bt not in block_types:
                block_types.append(bt)

        mtime_ns = int(entry.get("mtime_ns", 0))
        json_meta = dict(entry.get("json_metadata") or {})
        sort_ts_ms = int(json_meta.get("sort_ts_ms") or (mtime_ns // 1_000_000))
        card = {
            "entry_id": entry["entry_id"],
            "conversation_id": entry["conversation_id"],
            "short_id": entry["short_id"],
            "friendly_session_name": entry.get("friendly_session_name", ""),
            "entry_type": entry["entry_type"],
            "title": entry["title"],
            "subtitle": entry["subtitle"],
            "workspace_project": entry["workspace_project"],
            "snippet": clean_snip,
            "message_role": entry.get("message_role", "AGENT"),
            "message_text": entry.get("message_text", clean_snip),
            "agent_thoughts": entry.get("agent_thoughts", ""),
            "artifact_items": entry.get("artifact_items") or [],
            "created_at": entry.get("created_at", ""),
            "updated_at": entry.get("updated_at", ""),
            "mtime_ns": mtime_ns,
            "sort_ts_ms": sort_ts_ms,
            "json_metadata": json_meta,
            "step_count": entry.get("step_count", 0),
            "user_turn_count": entry.get("user_turn_count", 0),
            "tool_call_count": entry.get("tool_call_count", 0),
            "artifact_count": entry.get("artifact_count", 0),
            "task_log_count": entry.get("task_log_count", 0),
            "code_block_count": len(cbs),
            "code_block_badges": block_types[:5],
            "qdrant_chunk_count": doc_rec.get("qdrant_chunk_count", 1),
            "rejoin_verified": doc_rec.get("rejoin_verified", True),
            "where_to_find": {
                "primary_location": locs.get("primary_location", ""),
                "brain_dir": locs.get("brain_dir", ""),
                "transcript_path": locs.get("transcript_path", ""),
                "artifact_names": [a.get("name", "") for a in (locs.get("artifact_files") or [])],
                "referenced_files": (locs.get("referenced_files") or [])[:6],
                "referenced_files_count": len(locs.get("referenced_files") or []),
            },
            "related_metadata": {
                "related_modules": rels.get("related_modules") or [],
                "related_kis": rels.get("related_kis") or [],
                "related_models": rels.get("related_models") or [],
                "related_benchmarks": rels.get("related_benchmarks") or [],
                "related_conversations": (rels.get("related_conversations") or [])[:4],
            },
        }
        if search_score is not None:
            card["search_score"] = search_score
        if semantic_score is not None:
            card["semantic_score"] = semantic_score
        return card

    def _get_merged_entries(self, overview: dict[str, Any]) -> list[dict[str, Any]]:
        base_entries = list(overview.get("entries", []))
        if not self._live_chat_entries:
            return base_entries
        seen_ids = {str(e.get("entry_id", "")) for e in base_entries}
        merged = list(base_entries)
        for le in self._live_chat_entries:
            eid = str(le.get("entry_id", ""))
            if eid and eid not in seen_ids:
                merged.append(le)
                seen_ids.add(eid)
        merged.sort(key=lambda e: (int(e.get("mtime_ns", 0)), str(e.get("entry_id", ""))))
        return merged

    def get_overview(self) -> dict[str, Any]:
        q_status = self.ensure_indexed(force=False)
        overview = self.ledger.scan_and_build(force=False)
        merged_entries = self._get_merged_entries(overview)
        cards = [self._to_card_summary(e) for e in merged_entries]
        return {
            "total_entries": len(cards),
            "primary_conversations_count": int(overview.get("primary_conversations_count", 0)),
            "conversation_segments_count": int(overview.get("conversation_segments_count", 0)),
            "artifacts_count": int(overview.get("artifacts_count", 0)),
            "subagent_sessions_count": int(overview.get("subagent_sessions_count", 0)),
            "conversation_tabs": overview.get("conversation_tabs", []),
            "brain_directory_tree": overview.get("brain_directory_tree", []),
            "projects_breakdown": overview.get("projects_breakdown", {}),
            "fsbase_telemetry": overview.get("fsbase_telemetry", {}),
            "qdrant_status": q_status,
            "entries": cards,
        }

    def get_entry_detail(self, entry_id_or_conversation_id: str) -> dict[str, Any] | None:
        """
        Returns the full modal payload for a brain entry, including:
        - `code_blocks`: The conversation broken up into structured code blocks for clarity.
        - `locations` ("Where to Find Things"): exact FSBase paths, artifacts, task logs, and workspace files.
        - `relations` ("What Else Is Related"): related toolset modules, KIs, neural models, and conversations.
        - `qdrant_rejoin`: SHA-256 lossless chunk re-joining verification.
        """
        self.ensure_indexed(force=False)
        target = entry_id_or_conversation_id.strip()
        entry = self._index.load_from_index("ENTRIES", target)
        if entry is None:
            for _, cand in self._index.range_index("ENTRIES"):
                if (
                    cand.get("conversation_id") == target
                    or cand.get("short_id") == target[:8]
                    or cand.get("entry_id") == f"brain_conv_{target[:8]}"
                ):
                    entry = cand
                    break
        if entry is None:
            return None

        doc_id = str(entry["entry_id"])
        chunks = self._index.load_from_index("CHUNKS_BY_DOC", doc_id) or []
        rejoined = rejoin_chunks(chunks)
        return {
            **entry,
            "qdrant_doc_id": doc_id,
            "qdrant_chunks_count": len(chunks),
            "qdrant_rejoin": rejoined,
        }

    @staticmethod
    def _extract_query_centered_snippet(text: str, query: str, max_len: int = 200) -> str:
        clean = re.sub(r"```[a-zA-Z0-9_-]*", " ", text)
        clean = re.sub(r"[#>*`]+", " ", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        if not clean:
            return ""
        q_lower = query.lower().strip()
        pos = clean.lower().find(q_lower)
        if pos == -1:
            for tok in q_lower.split():
                if len(tok) >= 3:
                    pos = clean.lower().find(tok)
                    if pos != -1:
                        break
        if pos == -1 or len(clean) <= max_len:
            return clean[: max_len - 3] + "..." if len(clean) > max_len else clean
        start = max(0, pos - 60)
        end = min(len(clean), start + max_len)
        slice_str = clean[start:end].strip()
        prefix = "..." if start > 0 else ""
        suffix = "..." if end < len(clean) else ""
        return f"{prefix}{slice_str}{suffix}"

    @staticmethod
    def _filter_entries_by_metadata(
        entries: list[dict[str, Any]],
        time_range: str | None = None,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        role_filter: str | None = None,
        has_mermaid: bool | None = None,
        has_code_blocks: bool | None = None,
    ) -> list[dict[str, Any]]:
        if not entries:
            return []

        filtered = list(entries)
        if role_filter and role_filter.upper() not in ("ALL", ""):
            rf = role_filter.upper()
            filtered = [
                e
                for e in filtered
                if str(e.get("message_role") or (e.get("json_metadata") or {}).get("message_role") or "").upper() == rf
            ]

        if has_mermaid is not None:
            filtered = [
                e
                for e in filtered
                if bool((e.get("json_metadata") or {}).get("has_mermaid", "```mermaid" in str(e.get("message_text", ""))))
                == bool(has_mermaid)
            ]

        if has_code_blocks is not None:
            filtered = [
                e
                for e in filtered
                if bool((e.get("json_metadata") or {}).get("has_code_blocks", bool(e.get("code_blocks"))))
                == bool(has_code_blocks)
            ]

        def _entry_ts_ms(e: dict[str, Any]) -> int:
            jm = e.get("json_metadata") or {}
            ts = int(jm.get("sort_ts_ms") or e.get("sort_ts_ms") or (int(e.get("mtime_ns", 0)) // 1_000_000))
            return ts

        tr = (time_range or "all").strip().lower()
        duration_map_ms = {
            "1h": 3600 * 1000,
            "6h": 6 * 3600 * 1000,
            "24h": 24 * 3600 * 1000,
            "7d": 7 * 24 * 3600 * 1000,
            "30d": 30 * 24 * 3600 * 1000,
        }
        if tr in duration_map_ms and filtered:
            anchor_ts = max((_entry_ts_ms(e) for e in entries), default=int(time.time() * 1000))
            cutoff = anchor_ts - duration_map_ms[tr]
            filtered = [e for e in filtered if _entry_ts_ms(e) >= cutoff]

        if start_ts_ms is not None and start_ts_ms > 0:
            filtered = [e for e in filtered if _entry_ts_ms(e) >= int(start_ts_ms)]
        if end_ts_ms is not None and end_ts_ms > 0:
            filtered = [e for e in filtered if _entry_ts_ms(e) <= int(end_ts_ms)]

        return filtered

    def search_brain(
        self,
        query: str = "",
        entry_type: str | None = None,
        workspace_project: str | None = None,
        conversation_id: str | None = None,
        time_range: str | None = None,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        role_filter: str | None = None,
        has_mermaid: bool | None = None,
        has_code_blocks: bool | None = None,
        sort_order: str = "asc",
        top_k: int = 80,
    ) -> dict[str, Any]:
        """
        Hybrid Qdrant semantic vector search + lexical matching across all Antigravity Brain
        conversations, turn segments, and artifacts. Supports filtering and sorting on re-ingested
        JSON metadata (timestamp epoch ms, time range buckets, role, mermaid/code presence).
        """
        q_status = self.ensure_indexed(force=False)
        overview = self.ledger.scan_and_build(force=False)
        all_entries: list[dict[str, Any]] = self._get_merged_entries(overview)

        if entry_type and entry_type != "All":
            all_entries = [e for e in all_entries if e.get("entry_type") == entry_type]
        if workspace_project and workspace_project != "All":
            all_entries = [e for e in all_entries if e.get("workspace_project") == workspace_project]

        if conversation_id and conversation_id != "All":
            direct = [e for e in all_entries if e.get("conversation_id") == conversation_id]
            target_proj = direct[0].get("workspace_project") if direct else ""
            target_mods = set(
                m for e in direct for m in ((e.get("relations") or {}).get("related_modules") or [])
            )
            target_models = set(
                m for e in direct for m in ((e.get("relations") or {}).get("related_models") or [])
            )
            relative = [
                e
                for e in all_entries
                if e.get("conversation_id") != conversation_id
                and (
                    e.get("workspace_project") == target_proj
                    or bool(
                        target_models.intersection(
                            (e.get("relations") or {}).get("related_models") or []
                        )
                    )
                    or len(
                        target_mods.intersection(
                            (e.get("relations") or {}).get("related_modules") or []
                        )
                    )
                    >= 2
                )
            ]
            all_entries = [*direct, *relative]

        all_entries = self._filter_entries_by_metadata(
            all_entries,
            time_range=time_range,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            role_filter=role_filter,
            has_mermaid=has_mermaid,
            has_code_blocks=has_code_blocks,
        )

        # Sort chronologically by time (oldest at top -> newest at bottom by default)
        reverse_time = str(sort_order or "asc").strip().lower() == "desc"
        all_entries.sort(
            key=lambda e: (int(e.get("mtime_ns", 0)), str(e.get("entry_id", ""))),
            reverse=reverse_time,
        )

        cleaned_q = query.strip()
        if not cleaned_q:
            sliced = all_entries[: max(1, top_k)] if reverse_time else all_entries[-max(1, top_k) :]
            cards = [self._to_card_summary(e) for e in sliced]
            return {
                "query": "",
                "time_range": time_range or "all",
                "role_filter": role_filter or "ALL",
                "sort_order": sort_order,
                "total_results": len(all_entries),
                "qdrant_status": q_status,
                "results": cards,
            }

        model_name = str(q_status.get("embedding_model", "sentence-transformers/all-MiniLM-L6-v2"))
        q_vec = encode_texts_cpu([cleaned_q], model_name=model_name)[0]
        client = self.qdrant_graph._ensure_qdrant_client()

        must_conditions = []
        if entry_type and entry_type != "All":
            must_conditions.append(FieldCondition(key="doc_type", match=MatchValue(value=entry_type)))
        if workspace_project and workspace_project != "All":
            must_conditions.append(
                FieldCondition(key="workspace_project", match=MatchValue(value=workspace_project))
            )

        query_filter = Filter(must=must_conditions) if must_conditions else None
        q_res = client.query_points(
            collection_name=self.collection_name,
            query=q_vec,
            query_filter=query_filter,
            limit=max(top_k * 3, 40),
            with_payload=True,
        )

        best_score_by_doc: dict[str, float] = {}
        matched_chunk_by_doc: dict[str, str] = {}
        for pt in q_res.points:
            payload = pt.payload or {}
            d_id = str(payload.get("doc_id", ""))
            if not d_id:
                continue
            sc = float(pt.score or 0.0)
            if sc > best_score_by_doc.get(d_id, -1.0):
                best_score_by_doc[d_id] = sc
                matched_chunk_by_doc[d_id] = str(payload.get("chunk_text", ""))

        q_lower = cleaned_q.lower()
        q_tokens = [tok for tok in q_lower.split() if tok]

        scored_cards: list[dict[str, Any]] = []
        for entry in all_entries:
            e_id = str(entry["entry_id"])
            sem_score = max(0.0, best_score_by_doc.get(e_id, 0.0))
            locs = entry.get("locations") or {}
            rels = entry.get("relations") or {}
            cbs_text = " ".join(str(b.get("code", ""))[:400] for b in (entry.get("code_blocks") or [])[:8])
            haystack = " ".join(
                [
                    str(entry.get("title", "")),
                    str(entry.get("subtitle", "")),
                    str(entry.get("workspace_project", "")),
                    str(entry.get("snippet", "")),
                    str(entry.get("message_text", "")),
                    " ".join(locs.get("referenced_files") or []),
                    " ".join(rels.get("related_modules") or []),
                    " ".join(rels.get("related_kis") or []),
                    " ".join(rels.get("related_models") or []),
                    " ".join(rels.get("related_benchmarks") or []),
                    cbs_text,
                ]
            ).lower()

            title_lower = str(entry.get("title", "")).lower()
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
                raw_match_text = matched_chunk_by_doc.get(e_id) or cbs_text or str(entry.get("snippet", ""))
                centered_snip = self._extract_query_centered_snippet(raw_match_text, cleaned_q, max_len=200)
                card = self._to_card_summary(
                    entry,
                    matched_snippet=centered_snip or entry.get("snippet", ""),
                    search_score=combined_score,
                    semantic_score=round(sem_score, 4),
                )
                scored_cards.append(card)

        scored_cards.sort(
            key=lambda c: (
                -float(c.get("search_score", 0.0)),
                0 if c.get("entry_type") == "primary_conversation" else 1,
            )
        )
        return {
            "query": cleaned_q,
            "time_range": time_range or "all",
            "role_filter": role_filter or "ALL",
            "sort_order": sort_order,
            "total_results": len(scored_cards),
            "qdrant_status": q_status,
            "results": scored_cards[: max(1, top_k)],
        }

    def get_brain_directory_tree(self) -> dict[str, Any]:
        """
        Returns the friendly-named Brain Directory Tree mimicking `/Users/jarek/.gemini/antigravity-cli/brain/<uuid>`
        with nested folders (`Markdown Artifacts`, `Conversation Logs`, `Task Execution Logs`, `Scratch`).
        """
        self.ensure_indexed(force=False)
        overview = self.ledger.scan_and_build(force=False)
        tree = overview.get("brain_directory_tree", [])
        return {
            "brain_root": str(self.brain_root),
            "total_sessions": len(tree),
            "total_artifacts": overview.get("artifacts_count", 0),
            "tree": tree,
        }

    def get_artifacts_catalog(
        self,
        conversation_id: str | None = None,
        query: str = "",
        time_range: str = "all",
    ) -> dict[str, Any]:
        """
        Returns a full catalog of all brain markdown artifacts across sessions with their friendly
        session names, rendered markdown content, Mermaid diagram detection, timestamps, and download metadata.
        """
        self.ensure_indexed(force=False)
        overview = self.ledger.scan_and_build(force=False)
        tree = overview.get("brain_directory_tree", [])
        all_entries = list(overview.get("entries", []))

        # Collect both standalone artifact entries and embedded artifact_items across conversations
        artifacts_by_path: dict[str, dict[str, Any]] = {}
        friendly_by_conv: dict[str, str] = {}
        for node in tree:
            cid = str(node.get("conversation_id", ""))
            if cid:
                friendly_by_conv[cid] = str(node.get("friendly_name", cid[:8]))

        art_text_by_path: dict[str, str] = {}
        for e in all_entries:
            if e.get("entry_type") == "brain_artifact":
                ploc = str((e.get("locations") or {}).get("primary_location") or "")
                if ploc and e.get("message_text"):
                    art_text_by_path[ploc] = str(e["message_text"])

        for e in all_entries:
            cid = str(e.get("conversation_id", ""))
            friendly_name = str(e.get("friendly_session_name") or friendly_by_conv.get(cid) or cid[:8])
            ws_proj = str(e.get("workspace_project", "Portfolio"))
            for item in e.get("artifact_items") or []:
                p = str(item.get("path", "")).strip()
                if not p or p in artifacts_by_path:
                    continue
                p_obj = Path(p)
                content = str(item.get("content") or art_text_by_path.get(p) or "")
                if not content and p_obj.is_file():
                    try:
                        with self.ledger.fs.open(str(p_obj), "rb") as f:
                            content = f.read(256_000).decode("utf-8", errors="replace")
                    except Exception:
                        content = ""
                try:
                    st = p_obj.stat()
                    mtime_ns = int(st.st_mtime_ns)
                    size_bytes = int(st.st_size)
                except Exception:
                    mtime_ns = int(e.get("mtime_ns", 0))
                    size_bytes = int(item.get("size_bytes", len(content.encode("utf-8"))))
                sort_ts_ms = mtime_ns // 1_000_000
                artifacts_by_path[p] = {
                    "artifact_id": f"art_{hashlib.sha1(p.encode('utf-8')).hexdigest()[:12]}",
                    "name": str(item.get("name") or p_obj.name),
                    "title": str(item.get("title") or p_obj.stem),
                    "path": p,
                    "conversation_id": cid,
                    "short_id": cid[:8],
                    "friendly_session_name": friendly_name,
                    "workspace_project": ws_proj,
                    "size_bytes": size_bytes,
                    "updated_at": str(item.get("updated_at") or e.get("updated_at") or ""),
                    "mtime_ns": mtime_ns,
                    "sort_ts_ms": sort_ts_ms,
                    "has_mermaid": "```mermaid" in content,
                    "content": content,
                    "snippet": re.sub(r"\s+", " ", re.sub(r"[#*`>]+", " ", content)).strip()[:220],
                    "download_url": f"/api/antigravity/files/download?path={p}",
                }

        items = list(artifacts_by_path.values())
        if conversation_id and conversation_id != "All":
            items = [a for a in items if a["conversation_id"] == conversation_id]

        tr = (time_range or "all").strip().lower()
        duration_map_ms = {
            "1h": 3600 * 1000,
            "6h": 6 * 3600 * 1000,
            "24h": 24 * 3600 * 1000,
            "7d": 7 * 24 * 3600 * 1000,
            "30d": 30 * 24 * 3600 * 1000,
        }
        if tr in duration_map_ms and items:
            anchor_ts = max((int(a["sort_ts_ms"]) for a in items), default=int(time.time() * 1000))
            cutoff = anchor_ts - duration_map_ms[tr]
            items = [a for a in items if int(a["sort_ts_ms"]) >= cutoff]

        q_clean = query.strip().lower()
        if q_clean:
            items = [
                a
                for a in items
                if q_clean in a["name"].lower()
                or q_clean in a["title"].lower()
                or q_clean in a["friendly_session_name"].lower()
                or q_clean in a["content"].lower()
            ]

        items.sort(key=lambda a: (-int(a["sort_ts_ms"]), a["name"]))
        return {
            "total_artifacts": len(items),
            "brain_directory_tree": tree,
            "artifacts": items,
        }

    def get_brain_file_content(self, file_path: str) -> dict[str, Any] | None:
        """
        Safely reads a file inside `self.brain_root` via `FSBase` for viewing or downloading.
        """
        raw_path = (file_path or "").strip()
        if not raw_path:
            return None
        try:
            resolved = Path(raw_path).resolve()
            brain_root_resolved = self.brain_root.resolve()
            if not resolved.is_relative_to(brain_root_resolved):
                return None
            if not resolved.is_file():
                return None
            st = resolved.stat()
            max_read = min(int(st.st_size), 512_000)
            with self.ledger.fs.open(str(resolved), "rb") as f:
                raw_bytes = f.read(max_read)
            text_content = raw_bytes.decode("utf-8", errors="replace")
            guessed_mime, _ = mimetypes.guess_type(resolved.name)
            if resolved.suffix.lower() in (".md", ".markdown"):
                media_type = "text/markdown; charset=utf-8"
            elif resolved.suffix.lower() in (".json", ".jsonl"):
                media_type = "application/json; charset=utf-8"
            else:
                media_type = guessed_mime or "text/plain; charset=utf-8"
            return {
                "name": resolved.name,
                "path": str(resolved),
                "relative_path": str(resolved.relative_to(brain_root_resolved)),
                "size_bytes": int(st.st_size),
                "updated_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "media_type": media_type,
                "content": text_content,
                "truncated": int(st.st_size) > max_read,
            }
        except Exception:
            return None

    def send_bidirectional_chat_message(
        self,
        conversation_id: str,
        message_text: str,
    ) -> dict[str, Any]:
        """
        Processes a user chat message in bidirectional stream mode, records the user entry,
        queries Qdrant for grounded brain/codebase context, and synthesizes the agent response
        along with streamable chunks.
        """
        clean_text = (message_text or "").strip()
        if not clean_text:
            raise ValueError("Message text cannot be empty")

        overview = self.ledger.scan_and_build(force=False)
        tabs = overview.get("conversation_tabs", [])
        target_conv_id = (conversation_id or "").strip()
        if not target_conv_id or target_conv_id == "All":
            target_conv_id = str(tabs[0]["conversation_id"]) if tabs else "1147de54-f2df-4465-984d-92ea0e103818"

        short_id = target_conv_id[:8]
        tab_info = next((t for t in tabs if t.get("conversation_id") == target_conv_id), None)
        ws_project = str((tab_info or {}).get("workspace_project") or "Portfolio")
        friendly_name = str(
            (tab_info or {}).get("friendly_name")
            or f"{ws_project} — Session ({short_id})"
        )

        now_ns = time.time_ns()
        now_ms = now_ns // 1_000_000
        now_str = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        user_entry_id = f"brain_conv_{short_id}_live_u_{now_ms}"
        user_entry = {
            "entry_id": user_entry_id,
            "conversation_id": target_conv_id,
            "short_id": short_id,
            "friendly_session_name": friendly_name,
            "entry_type": "conversation_turn",
            "title": clean_text[:96],
            "subtitle": f"{ws_project} · Live User Message",
            "workspace_project": ws_project,
            "snippet": clean_text[:200],
            "message_role": "USER",
            "message_text": clean_text,
            "agent_thoughts": "",
            "artifact_items": [],
            "created_at": now_str,
            "updated_at": now_str,
            "mtime_ns": now_ns,
            "sort_ts_ms": now_ms,
            "step_count": 1,
            "user_turn_count": 1,
            "tool_call_count": 0,
            "artifact_count": 0,
            "task_log_count": 0,
            "code_blocks": [],
            "locations": {
                "brain_dir": str(self.brain_root / target_conv_id),
                "transcript_path": str(self.brain_root / target_conv_id / ".system_generated/logs/transcript.jsonl"),
                "primary_location": str(self.brain_root / target_conv_id),
                "artifact_files": [],
                "referenced_files": [],
            },
            "relations": {
                "related_modules": [],
                "related_kis": [],
                "related_models": [],
                "related_benchmarks": [],
                "related_conversations": [],
            },
            "json_metadata": {
                "schema_version": "4.0",
                "timestamp_epoch_ms": now_ms,
                "created_epoch_ms": now_ms,
                "updated_epoch_ms": now_ms,
                "sort_ts_ms": now_ms,
                "date_bucket": now_str[:10],
                "hour_bucket": now_str[:13],
                "step_index": 9999,
                "entry_type": "conversation_turn",
                "message_role": "USER",
                "workspace_project": ws_project,
                "conversation_id": target_conv_id,
                "short_id": short_id,
                "friendly_session_name": friendly_name,
                "has_thoughts": False,
                "has_artifacts": False,
                "has_code_blocks": "```" in clean_text,
                "has_mermaid": "```mermaid" in clean_text,
                "live_stream": True,
            },
        }

        # Retrieve top Qdrant matches to ground the agent reply
        search_hits = self.search_brain(query=clean_text, top_k=3).get("results", [])
        hit_lines = []
        for idx, hit in enumerate(search_hits[:3], start=1):
            hit_title = hit.get("title", "Brain Entry")
            hit_proj = hit.get("workspace_project", ws_project)
            hit_snip = hit.get("snippet", "")
            hit_loc = (hit.get("where_to_find") or {}).get("primary_location", "")
            hit_lines.append(f"{idx}. **{hit_title}** (`{hit_proj}`): {hit_snip}" + (f"\n   - Path: `{hit_loc}`" if hit_loc else ""))

        if hit_lines:
            reply_md = (
                f"Indexed Qdrant context for `{clean_text}` in **{friendly_name}**:\n\n"
                + "\n".join(hit_lines)
            )
        else:
            reply_md = (
                f"Recorded message in **{friendly_name}** (`{short_id}`). "
                f"Active Qdrant brain index contains `{overview.get('total_entries', 0)}` entries across `{overview.get('primary_conversations_count', 0)}` sessions."
            )

        agent_ns = now_ns + 1_000_000
        agent_ms = agent_ns // 1_000_000
        agent_entry_id = f"brain_conv_{short_id}_live_a_{agent_ms}"
        agent_thoughts = (
            f"Queried Qdrant collection `{self.collection_name}` for `{clean_text}` "
            f"(matched {len(search_hits)} entries in session {short_id})."
        )
        agent_entry = {
            "entry_id": agent_entry_id,
            "conversation_id": target_conv_id,
            "short_id": short_id,
            "friendly_session_name": friendly_name,
            "entry_type": "conversation_turn",
            "title": f"Agent Response: {clean_text[:72]}",
            "subtitle": f"{ws_project} · Live Agent Stream",
            "workspace_project": ws_project,
            "snippet": reply_md[:200],
            "message_role": "AGENT",
            "message_text": reply_md,
            "agent_thoughts": agent_thoughts,
            "artifact_items": [],
            "created_at": now_str,
            "updated_at": now_str,
            "mtime_ns": agent_ns,
            "sort_ts_ms": agent_ms,
            "step_count": 1,
            "user_turn_count": 0,
            "tool_call_count": 1,
            "artifact_count": 0,
            "task_log_count": 0,
            "code_blocks": [],
            "locations": user_entry["locations"],
            "relations": user_entry["relations"],
            "json_metadata": {
                **user_entry["json_metadata"],
                "timestamp_epoch_ms": agent_ms,
                "created_epoch_ms": agent_ms,
                "updated_epoch_ms": agent_ms,
                "sort_ts_ms": agent_ms,
                "step_index": 10000,
                "message_role": "AGENT",
                "has_thoughts": True,
                "live_stream": True,
            },
        }

        with self._lock:
            self._live_chat_entries.append(user_entry)
            self._live_chat_entries.append(agent_entry)
            self._index.store_in_index("ENTRIES", user_entry_id, user_entry)
            self._index.store_in_index("ENTRIES", agent_entry_id, agent_entry)

        user_card = self._to_card_summary(user_entry)
        agent_card = self._to_card_summary(agent_entry)

        # Break agent response into token chunks for bidirectional WebSocket streaming
        words = reply_md.split(" ")
        stream_chunks: list[str] = []
        step_size = max(1, len(words) // 10 or 1)
        for i in range(0, len(words), step_size):
            piece = " ".join(words[i : i + step_size])
            if i + step_size < len(words):
                piece += " "
            stream_chunks.append(piece)

        return {
            "conversation_id": target_conv_id,
            "user_message": user_card,
            "agent_message": agent_card,
            "stream_chunks": stream_chunks,
        }

    def query_datasource(
        self,
        query: str = "",
        entry_type: str | None = None,
        workspace_project: str | None = None,
        top_k: int = 36,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return self.search_brain(
            query=query,
            entry_type=entry_type,
            workspace_project=workspace_project,
            top_k=top_k,
        )
