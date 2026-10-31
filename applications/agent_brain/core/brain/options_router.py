from __future__ import annotations

import asyncio
import base64
import os
import pickle
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Response, WebSocket, WebSocketDisconnect

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.core.brain.antigravity_brain_service import (
    AntigravityBrainService,
)
from applications.agent_brain.core.brain.qdrant_chunk_provider import (
    QdrantChunkClientProvider,
)
from applications.agent_brain.core.settings import get as get_setting
from applications.agent_brain.models.benchmark_service import NeuralBenchmarkService
from indexes.informational_index import (
    AgentInformationalGraphIndex,
    QdrantGeodesicLifecycleMCPServer,
)

BASE_DIR = Path(__file__).resolve().parents[2]

brain_options_router = APIRouter()
_ACTIVE_BRAIN_SOCKETS: set[WebSocket] = set()
_ACTIVE_PROMOTED_SOCKETS: set[WebSocket] = set()


def _resolve_path(raw: str | Path, default_rel: str) -> Path:
    val = str(raw or default_rel).strip()
    p = Path(val)
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()


class AgentBrainOptionsRuntime:
    """
    Lazy, thread-safe runtime container for the migrated Agent Brain modules:
      1. `AntigravityBrainService` (Antigravity Brain conversations & Brain Artifacts)
      2. `NeuralBenchmarkService` (Neural Benchmarks leaderboard & Promoted Model Harness + FS CRUD)
      3. `AgentInformationalGraphIndex` & `QdrantGeodesicLifecycleMCPServer` (Staging Qdrant & Geodesic MCP)
    """

    def __init__(
        self,
        qdrant_storage_path: str | Path | None = None,
        brain_root: str | Path | None = None,
        brain_ledger_path: str | Path | None = None,
        brain_collection: str | None = None,
        neural_networks_repo_path: str | Path | None = None,
        benchmark_ledger_path: str | Path | None = None,
        benchmark_collection: str | None = None,
        staging_root: str | Path | None = None,
        idea_projects_root: str | Path | None = None,
        skill_dir_path: str | Path | None = None,
        informational_collection: str | None = None,
        watch_files: bool | None = None,
        prefer_memory: bool = False,
    ) -> None:
        self._lock = threading.RLock()
        self._watch_files = (
            bool(watch_files)
            if watch_files is not None
            else bool(get_setting("staging_qdrant.file_watcher_enabled", True))
            and not bool(os.environ.get("K_SERVICE"))
        )
        resolved_qdrant_path = (
            Path(qdrant_storage_path).resolve()
            if qdrant_storage_path is not None
            else _resolve_path(
                get_setting("staging_qdrant.qdrant_options_path", "data/.qdrant_options"),
                "data/.qdrant_options",
            )
        )
        self.qdrant_provider = QdrantChunkClientProvider(
            storage_path=resolved_qdrant_path,
            embedding_model=str(
                get_setting(
                    "staging_qdrant.embedding_model",
                    "sentence-transformers/all-MiniLM-L6-v2",
                )
            ),
            chunk_size=int(get_setting("staging_qdrant.chunk_size", 380)),
            chunk_overlap=int(get_setting("staging_qdrant.chunk_overlap", 64)),
            prefer_memory=prefer_memory,
        )

        self._brain_root = (
            Path(brain_root)
            if brain_root is not None
            else Path(
                str(
                    get_setting(
                        "antigravity.brain_path",
                        "/Users/jarek/.gemini/antigravity-cli/brain",
                    )
                )
            )
        )
        self._brain_ledger_path = (
            Path(brain_ledger_path)
            if brain_ledger_path is not None
            else _resolve_path(
                get_setting("antigravity.ledger_path", "data/antigravity_brain_ledger.json"),
                "data/antigravity_brain_ledger.json",
            )
        )
        self._brain_collection = str(
            brain_collection
            or get_setting("antigravity.collection", "portfolio_antigravity_brain")
        )
        self._brain_interval = int(get_setting("antigravity.watcher_interval_seconds", 20))

        self._nn_repo_path = (
            Path(neural_networks_repo_path)
            if neural_networks_repo_path is not None
            else Path(
                str(
                    get_setting(
                        "benchmarks.neural_networks_repo_path",
                        "/Users/jarek/IdeaProjects/neural_networks",
                    )
                )
            )
        )
        self._benchmark_ledger_path = (
            Path(benchmark_ledger_path)
            if benchmark_ledger_path is not None
            else _resolve_path(
                get_setting("benchmarks.ledger_path", "data/benchmark_history_ledger.json"),
                "data/benchmark_history_ledger.json",
            )
        )
        self._benchmark_collection = str(
            benchmark_collection
            or get_setting("benchmarks.collection", "portfolio_neural_benchmarks")
        )
        self._benchmark_interval = int(get_setting("benchmarks.poll_interval_seconds", 30))

        self._staging_root = (
            Path(staging_root)
            if staging_root is not None
            else Path(
                str(
                    get_setting(
                        "staging_qdrant.staging_path",
                        "/Users/jarek/IdeaProjects/qdrant_staging",
                    )
                )
            )
        )
        self._idea_projects_root = (
            Path(idea_projects_root)
            if idea_projects_root is not None
            else Path(
                str(
                    get_setting(
                        "staging_qdrant.idea_projects_root",
                        "/Users/jarek/IdeaProjects",
                    )
                )
            )
        )
        self._skill_dir_path = (
            Path(skill_dir_path)
            if skill_dir_path is not None
            else Path(
                str(
                    get_setting(
                        "staging_qdrant.skill_dir_path",
                        "/Users/jarek/.gemini/config/skills/pytools-toolset",
                    )
                )
            )
        )
        self._informational_collection = str(
            informational_collection
            or get_setting("staging_qdrant.collection", "agent_informational_graph_index")
        )

        self._antigravity_brain: AntigravityBrainService | None = None
        self._benchmarks: NeuralBenchmarkService | None = None
        self._informational_index: AgentInformationalGraphIndex | None = None
        self._geodesic_mcp: QdrantGeodesicLifecycleMCPServer | None = None

    @property
    def is_file_watcher_enabled(self) -> bool:
        return self._watch_files and not bool(os.environ.get("K_SERVICE"))

    @property
    def antigravity_brain(self) -> AntigravityBrainService:
        with self._lock:
            if self._antigravity_brain is None:
                self._antigravity_brain = AntigravityBrainService(
                    qdrant_graph=self.qdrant_provider,
                    brain_root=self._brain_root,
                    ledger_path=self._brain_ledger_path,
                    collection_name=self._brain_collection,
                    refresh_interval_seconds=self._brain_interval if self.is_file_watcher_enabled else 0,
                    watch_files=self.is_file_watcher_enabled,
                )
            return self._antigravity_brain

    @property
    def benchmarks(self) -> NeuralBenchmarkService:
        with self._lock:
            if self._benchmarks is None:
                self._benchmarks = NeuralBenchmarkService(
                    qdrant_graph=self.qdrant_provider,
                    repo_path=self._nn_repo_path,
                    ledger_path=self._benchmark_ledger_path,
                    collection_name=self._benchmark_collection,
                    watch_files=self.is_file_watcher_enabled,
                    refresh_interval_seconds=self._benchmark_interval if self.is_file_watcher_enabled else 0,
                    staging_root=self._staging_root,
                )
            return self._benchmarks

    @property
    def informational_index(self) -> AgentInformationalGraphIndex:
        with self._lock:
            if self._informational_index is None:
                self._informational_index = AgentInformationalGraphIndex(
                    staging_root=self._staging_root,
                    idea_projects_root=self._idea_projects_root,
                    skill_md_path=self._skill_dir_path / "SKILL.md",
                    collection_name=self._informational_collection,
                    auto_build=True,
                )
            return self._informational_index

    @property
    def geodesic_mcp(self) -> QdrantGeodesicLifecycleMCPServer:
        with self._lock:
            if self._geodesic_mcp is None:
                self._geodesic_mcp = QdrantGeodesicLifecycleMCPServer(
                    index=self.informational_index,
                    lazy_init=False,
                )
            return self._geodesic_mcp

    def get_slot_harness_benchmarks(
        self,
        slot: str = "",
        harness: str = "",
        dataset: str = "",
        query: str = "",
        limit: int = 200,
    ) -> dict[str, Any]:
        candidates = (
            self._staging_root
            / "storage"
            / "collection"
            / "unified_harness_benchmarks"
            / "storage.sqlite",
            self._staging_root
            / "collection"
            / "unified_harness_benchmarks"
            / "storage.sqlite",
        )
        db_path = next((p for p in candidates if p.is_file()), None)
        if db_path is None:
            return {
                "collection": "unified_harness_benchmarks",
                "storage_path": str(candidates[0]),
                "available": False,
                "total_points": 0,
                "unique_cells": 0,
                "slots": [],
                "harnesses": [],
                "datasets": [],
                "identical_splits_verified": True,
                "summary_matrix": [],
                "records": [],
            }

        raw_payloads: list[dict[str, Any]] = []
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
        try:
            rows = conn.execute("SELECT point FROM points").fetchall()
            for (blob,) in rows:
                pt = pickle.loads(blob)
                pl = getattr(pt, "payload", None)
                if isinstance(pl, dict):
                    raw_payloads.append(dict(pl))
        finally:
            conn.close()

        raw_payloads.sort(
            key=lambda r: (
                str(r.get("run_timestamp") or ""),
                str(r.get("model_slot_id") or ""),
                str(r.get("harness") or ""),
                str(r.get("test_id") or ""),
            ),
            reverse=True,
        )

        # Latest record per (model_slot_id, harness, test_id) for canonical cell matrix
        latest_by_cell: dict[tuple[str, str, str], dict[str, Any]] = {}
        slots_set: set[str] = set()
        harnesses_set: set[str] = set()
        datasets_set: set[str] = set()
        digests_by_run_test: dict[tuple[str, str], set[str]] = {}

        for rec in raw_payloads:
            s_id = str(rec.get("model_slot_id") or "")
            h_id = str(rec.get("harness") or "")
            t_id = str(rec.get("test_id") or "")
            d_id = str(rec.get("dataset") or "")
            r_id = str(rec.get("run_id") or "")
            if s_id:
                slots_set.add(s_id)
            if h_id:
                harnesses_set.add(h_id)
            if d_id:
                datasets_set.add(d_id)
            sp_dig = str(rec.get("split_digest") or "")
            if r_id and t_id and sp_dig:
                digests_by_run_test.setdefault((r_id, t_id), set()).add(sp_dig)
            cell_key = (s_id, h_id, t_id)
            if s_id and h_id and t_id and cell_key not in latest_by_cell:
                latest_by_cell[cell_key] = rec

        buckets: dict[tuple[str, str], dict[str, Any]] = {}
        for (s_id, h_id, _), rec in latest_by_cell.items():
            b_key = (s_id, h_id)
            b = buckets.get(b_key)
            if b is None:
                b = {
                    "model_slot_id": s_id,
                    "model_class": str(rec.get("model_class") or s_id),
                    "harness": h_id,
                    "param_count": int(rec.get("param_count") or 0),
                    "tests_evaluated": 0,
                    "tests_passed": 0,
                    "tests_learned": 0,
                    "pass_rate": 0.0,
                    "mean_accuracy": 0.0,
                    "mean_test_loss": 0.0,
                    "mean_coherent_io_ratio": 0.0,
                    "_coherent_count": 0,
                    "total_train_seconds": 0.0,
                    "latest_timestamp": str(rec.get("run_timestamp") or ""),
                }
                buckets[b_key] = b
            b["tests_evaluated"] += 1
            b["tests_passed"] += int(bool(rec.get("test_passed")))
            b["tests_learned"] += int(bool(rec.get("learned")))
            b["mean_accuracy"] += float(rec.get("accuracy") or 0.0)
            b["mean_test_loss"] += float(rec.get("test_loss") or 0.0)
            b["total_train_seconds"] += float(rec.get("train_seconds") or 0.0)
            if rec.get("coherent_io_ratio") is not None:
                b["mean_coherent_io_ratio"] += float(rec.get("coherent_io_ratio") or 0.0)
                b["_coherent_count"] += 1
            b["param_count"] = max(int(b["param_count"]), int(rec.get("param_count") or 0))
            ts = str(rec.get("run_timestamp") or "")
            if ts > str(b["latest_timestamp"]):
                b["latest_timestamp"] = ts

        summary_matrix: list[dict[str, Any]] = []
        for b in buckets.values():
            denom = max(1, int(b["tests_evaluated"]))
            coh_cnt = int(b.pop("_coherent_count", 0))
            b["pass_rate"] = round((int(b["tests_passed"]) / denom) * 100.0, 2)
            b["mean_accuracy"] = round(float(b["mean_accuracy"]) / denom, 4)
            b["mean_test_loss"] = round(float(b["mean_test_loss"]) / denom, 5)
            b["mean_coherent_io_ratio"] = (
                round(float(b["mean_coherent_io_ratio"]) / coh_cnt, 4) if coh_cnt > 0 else None
            )
            b["total_train_seconds"] = round(float(b["total_train_seconds"]), 3)
            summary_matrix.append(b)

        summary_matrix.sort(
            key=lambda row: (
                -float(row["pass_rate"]),
                -float(row["mean_accuracy"]),
                float(row["mean_test_loss"]),
                str(row["model_slot_id"]),
                str(row["harness"]),
            )
        )

        slot_q = slot.strip().lower()
        harness_q = harness.strip().lower()
        dataset_q = dataset.strip().lower()
        text_q = query.strip().lower()
        eff_limit = max(1, min(int(limit or 200), 1000))

        filtered_records: list[dict[str, Any]] = []
        for rec in latest_by_cell.values():
            if slot_q and str(rec.get("model_slot_id") or "").lower() != slot_q:
                continue
            if harness_q and str(rec.get("harness") or "").lower() != harness_q:
                continue
            if dataset_q and str(rec.get("dataset") or "").lower() != dataset_q:
                continue
            if text_q:
                haystack = " ".join(
                    str(rec.get(k) or "")
                    for k in (
                        "model_slot_id",
                        "model_class",
                        "harness",
                        "test_id",
                        "benchmark",
                        "dataset",
                        "subject",
                        "snippet",
                    )
                ).lower()
                if text_q not in haystack:
                    continue
            filtered_records.append(rec)

        filtered_records.sort(
            key=lambda r: (
                str(r.get("model_slot_id") or ""),
                str(r.get("harness") or ""),
                str(r.get("test_id") or ""),
            )
        )

        filtered_matrix = [
            row
            for row in summary_matrix
            if (not slot_q or str(row["model_slot_id"]).lower() == slot_q)
            and (not harness_q or str(row["harness"]).lower() == harness_q)
        ]

        return {
            "collection": "unified_harness_benchmarks",
            "storage_path": str(db_path),
            "available": True,
            "total_points": len(raw_payloads),
            "unique_cells": len(latest_by_cell),
            "slots": sorted(slots_set),
            "harnesses": sorted(harnesses_set),
            "datasets": sorted(datasets_set),
            "identical_splits_verified": all(len(v) <= 1 for v in digests_by_run_test.values()),
            "summary_matrix": filtered_matrix,
            "records": filtered_records[:eff_limit],
        }

    def get_benchmark_recall_spec(self, max_lines: int = 80) -> dict[str, Any]:
        spec_path = BASE_DIR / "models" / "benchmark_recall.yaml"
        if not spec_path.is_file():
            return {
                "path": str(spec_path),
                "exists": False,
                "size_bytes": 0,
                "line_count": 0,
                "preview_lines": [],
            }
        raw_text = spec_path.read_text(encoding="utf-8")
        lines = raw_text.splitlines()
        cap = max(1, min(int(max_lines or 80), 500))
        return {
            "path": str(spec_path),
            "exists": True,
            "size_bytes": spec_path.stat().st_size,
            "line_count": len(lines),
            "preview_lines": lines[:cap],
        }

    def close(self) -> None:
        with self._lock:
            self.qdrant_provider.close()


_RUNTIME_LOCK = threading.RLock()
_DEFAULT_RUNTIME: AgentBrainOptionsRuntime | None = None


def get_brain_options_runtime() -> AgentBrainOptionsRuntime:
    global _DEFAULT_RUNTIME
    with _RUNTIME_LOCK:
        if _DEFAULT_RUNTIME is None:
            _DEFAULT_RUNTIME = AgentBrainOptionsRuntime()
        return _DEFAULT_RUNTIME


def set_brain_options_runtime(runtime: AgentBrainOptionsRuntime | None) -> None:
    global _DEFAULT_RUNTIME
    with _RUNTIME_LOCK:
        if _DEFAULT_RUNTIME is not None and _DEFAULT_RUNTIME is not runtime:
            _DEFAULT_RUNTIME.close()
        _DEFAULT_RUNTIME = runtime


# ------------------------------------------------------------------
# 1. Antigravity Brain & Brain Artifacts Endpoints
# ------------------------------------------------------------------
@brain_options_router.get("/api/antigravity/overview", response_model=None)
def get_antigravity_overview():
    return get_brain_options_runtime().antigravity_brain.get_overview()


@brain_options_router.get("/api/antigravity/search", response_model=None)
def search_antigravity_brain(
    query: Optional[str] = "",
    entry_type: Optional[str] = None,
    workspace_project: Optional[str] = None,
    conversation_id: Optional[str] = None,
    time_range: Optional[str] = None,
    start_ts_ms: Optional[int] = None,
    end_ts_ms: Optional[int] = None,
    role_filter: Optional[str] = None,
    has_mermaid: Optional[bool] = None,
    has_code_blocks: Optional[bool] = None,
    sort_order: str = "asc",
    top_k: int = 80,
):
    return get_brain_options_runtime().antigravity_brain.search_brain(
        query=query or "",
        entry_type=entry_type,
        workspace_project=workspace_project,
        conversation_id=conversation_id,
        time_range=time_range,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        role_filter=role_filter,
        has_mermaid=has_mermaid,
        has_code_blocks=has_code_blocks,
        sort_order=sort_order,
        top_k=top_k,
    )


@brain_options_router.get("/api/antigravity/tree", response_model=None)
def get_antigravity_tree():
    return get_brain_options_runtime().antigravity_brain.get_brain_directory_tree()


@brain_options_router.get("/api/antigravity/artifacts", response_model=None)
def get_antigravity_artifacts(
    conversation_id: Optional[str] = None,
    query: Optional[str] = "",
    time_range: str = "all",
):
    return get_brain_options_runtime().antigravity_brain.get_artifacts_catalog(
        conversation_id=conversation_id,
        query=query or "",
        time_range=time_range,
    )


@brain_options_router.get("/api/antigravity/files/content", response_model=None)
def get_antigravity_file_content(path: str):
    res = get_brain_options_runtime().antigravity_brain.get_brain_file_content(path)
    if res is None:
        raise HTTPException(
            status_code=404,
            detail=f"Brain file not found or outside brain directory: {path}",
        )
    return res


@brain_options_router.get("/api/antigravity/files/download", response_model=None)
def download_antigravity_file(path: str):
    file_info = get_antigravity_file_content(path)
    filename = str(file_info.get("name") or "artifact.md")
    content = str(file_info.get("content") or "")
    media_type = str(file_info.get("media_type") or "text/markdown; charset=utf-8")
    return Response(
        content=content.encode("utf-8"),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@brain_options_router.post("/api/antigravity/chat/send", response_model=None)
def send_antigravity_chat_message(payload: Dict[str, Any] = Body(default_factory=dict)):
    conv_id = str(payload.get("conversation_id") or "")
    text = str(payload.get("text") or payload.get("message") or "")
    try:
        return get_brain_options_runtime().antigravity_brain.send_bidirectional_chat_message(
            conversation_id=conv_id,
            message_text=text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@brain_options_router.get("/api/antigravity/entries/{entry_id:path}", response_model=None)
def get_antigravity_entry(entry_id: str):
    entry = get_brain_options_runtime().antigravity_brain.get_entry_detail(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown Antigravity Brain entry: {entry_id}")
    return entry


@brain_options_router.get("/api/antigravity/conversations/{entry_id:path}", response_model=None)
def get_antigravity_conversation_alias(entry_id: str):
    return get_antigravity_entry(entry_id)


@brain_options_router.post("/api/antigravity/sync", response_model=None)
def sync_antigravity_brain(payload: Optional[Dict[str, Any]] = Body(default=None)):
    force = bool(payload.get("force", False)) if isinstance(payload, dict) else False
    svc = get_brain_options_runtime().antigravity_brain
    q_status = svc.scan_and_index(force_recreate=force)
    overview = svc.get_overview()
    return {
        "status": "synced",
        "qdrant_status": q_status,
        "total_entries": overview.get("total_entries"),
        "primary_conversations_count": overview.get("primary_conversations_count"),
        "artifacts_count": overview.get("artifacts_count"),
        "subagent_sessions_count": overview.get("subagent_sessions_count"),
        "fsbase_telemetry": overview.get("fsbase_telemetry"),
    }


# ------------------------------------------------------------------
# 2. Neural Benchmarks & Promoted Model Harness + FS CRUD Endpoints
# ------------------------------------------------------------------
@brain_options_router.get("/api/benchmarks/overview", response_model=None)
def get_benchmarks_overview():
    return get_brain_options_runtime().benchmarks.get_overview()


@brain_options_router.get("/api/benchmarks/history", response_model=None)
def get_benchmarks_history():
    return get_brain_options_runtime().benchmarks.get_history()


@brain_options_router.get("/api/benchmarks/slot-harnesses", response_model=None)
def get_benchmarks_slot_harnesses(
    slot: Optional[str] = "",
    harness: Optional[str] = "",
    dataset: Optional[str] = "",
    query: Optional[str] = "",
    limit: int = 200,
):
    return get_brain_options_runtime().get_slot_harness_benchmarks(
        slot=slot or "",
        harness=harness or "",
        dataset=dataset or "",
        query=query or "",
        limit=limit,
    )


@brain_options_router.get("/api/benchmarks/recall-spec", response_model=None)
def get_benchmarks_recall_spec(max_lines: int = 80):
    return get_brain_options_runtime().get_benchmark_recall_spec(max_lines=max_lines)


@brain_options_router.get("/api/benchmarks/search", response_model=None)
def search_benchmarks(
    query: Optional[str] = "",
    doc_type: Optional[str] = None,
    category: Optional[str] = None,
    top_k: int = 24,
):
    return get_brain_options_runtime().benchmarks.search_benchmarks(
        query=query or "",
        doc_type=doc_type,
        category=category,
        top_k=top_k,
    )


@brain_options_router.get("/api/benchmarks/models/{model_id_or_name:path}", response_model=None)
def get_benchmark_model(model_id_or_name: str):
    model = get_brain_options_runtime().benchmarks.get_model_detail(model_id_or_name)
    if model is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown neural benchmark model: {model_id_or_name}",
        )
    return model


@brain_options_router.get("/api/benchmarks/tasks/{benchmark_id_or_name:path}", response_model=None)
def get_benchmark_task(benchmark_id_or_name: str):
    task = get_brain_options_runtime().benchmarks.get_task_detail(benchmark_id_or_name)
    if task is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown neural benchmark task: {benchmark_id_or_name}",
        )
    return task


@brain_options_router.post("/api/benchmarks/save", response_model=None)
def save_benchmark_run(payload: Dict[str, Any] = Body(default_factory=dict)):
    return get_brain_options_runtime().benchmarks.save_benchmark_run(payload)


@brain_options_router.post("/api/benchmarks/build", response_model=None)
def build_and_scrape_benchmarks(payload: Optional[Dict[str, Any]] = Body(default=None)):
    include_git = True
    if isinstance(payload, dict) and "include_git_history" in payload:
        include_git = bool(payload["include_git_history"])
    svc = get_brain_options_runtime().benchmarks
    q_status = svc.scrape_and_index(include_git_history=include_git)
    overview = svc.get_overview()
    return {
        "status": "scraped_and_indexed",
        "qdrant_status": q_status,
        "best_model": overview.get("best_model"),
        "total_models": overview.get("total_models"),
        "total_benchmarks": overview.get("total_benchmarks"),
        "total_runs": overview.get("total_runs"),
    }


@brain_options_router.post("/api/benchmarks/scrape", response_model=None)
def scrape_benchmarks_alias(payload: Optional[Dict[str, Any]] = Body(default=None)):
    return build_and_scrape_benchmarks(payload=payload)


@brain_options_router.post("/api/benchmarks/ingest", response_model=None)
def ingest_realtime_benchmarks(payload: Optional[Dict[str, Any]] = Body(default=None)):
    return get_brain_options_runtime().benchmarks.ingest_realtime_update(payload or {})


@brain_options_router.get("/api/benchmarks/promoted/status", response_model=None)
def get_promoted_model_status():
    return get_brain_options_runtime().benchmarks.get_promoted_model_status()


@brain_options_router.post("/api/benchmarks/promoted/run", response_model=None)
def run_promoted_model_tool(payload: Optional[Dict[str, Any]] = Body(default=None)):
    return get_brain_options_runtime().benchmarks.run_promoted_model_tool(payload or {})


@brain_options_router.get("/api/benchmarks/promoted/chat", response_model=None)
def get_promoted_chat_history(conversation_id: Optional[str] = None):
    return get_brain_options_runtime().benchmarks.get_promoted_chat_history(
        conversation_id=conversation_id
    )


@brain_options_router.post("/api/benchmarks/promoted/chat", response_model=None)
def send_promoted_chat_message(payload: Optional[Dict[str, Any]] = Body(default=None)):
    body = payload or {}
    text = str(body.get("text") or body.get("message") or body.get("prompt") or "")
    if not text.strip():
        raise HTTPException(status_code=400, detail="Message text cannot be empty")
    return get_brain_options_runtime().benchmarks.send_promoted_chat_message(
        message_text=text,
        action=str(body.get("action") or "learn"),
        steps=int(body.get("steps", 3)),
        max_new_tokens=int(body.get("max_new_tokens", 36)),
        temperature=float(body.get("temperature", 0.7)),
        top_k=int(body.get("top_k", 8)),
        conversation_id=str(body.get("conversation_id") or "").strip() or None,
        model_id=str(body.get("model_id") or "").strip() or None,
    )


@brain_options_router.post("/api/benchmarks/promoted/select-model", response_model=None)
def select_promoted_chat_model(payload: Optional[Dict[str, Any]] = Body(default=None)):
    body = payload or {}
    model_id = str(body.get("model_id") or body.get("model_name") or "").strip()
    return get_brain_options_runtime().benchmarks.select_promoted_chat_model(model_id)


@brain_options_router.post("/api/benchmarks/promoted/reset-knowledge", response_model=None)
def reset_promoted_knowledge():
    return get_brain_options_runtime().benchmarks.reset_promoted_knowledge()


@brain_options_router.get("/api/benchmarks/promoted/conversations", response_model=None)
def list_promoted_conversations():
    return get_brain_options_runtime().benchmarks.list_promoted_conversations()


@brain_options_router.post("/api/benchmarks/promoted/conversations", response_model=None)
def manage_promoted_conversations(payload: Optional[Dict[str, Any]] = Body(default=None)):
    body = payload or {}
    action = str(body.get("action") or "create").strip().lower()
    conversation_id = str(body.get("conversation_id") or "").strip()
    title = str(body.get("title") or "").strip()
    model_id = str(body.get("model_id") or "").strip()
    svc = get_brain_options_runtime().benchmarks
    if action == "open" and conversation_id:
        return svc.open_promoted_conversation(conversation_id=conversation_id)
    if action == "list":
        return svc.list_promoted_conversations()
    return svc.create_promoted_conversation(title=title, model_id=model_id)


@brain_options_router.get("/api/benchmarks/promoted/fs", response_model=None)
def get_promoted_fs(action: str = "list", path: str = "", scope: str = "workspace"):
    return get_brain_options_runtime().benchmarks.execute_promoted_fs_crud(
        action=action, path=path, scope=scope
    )


@brain_options_router.post("/api/benchmarks/promoted/fs", response_model=None)
def post_promoted_fs(payload: Optional[Dict[str, Any]] = Body(default=None)):
    body = payload or {}
    action = str(body.get("action") or "list").strip().lower()
    return get_brain_options_runtime().benchmarks.execute_promoted_fs_crud(
        action=action,
        path=str(body.get("path") or ""),
        content=body.get("content"),
        base64_content=body.get("base64_content"),
        mime_type=str(body.get("mime_type") or ""),
        scope=str(body.get("scope") or "workspace"),
    )


@brain_options_router.get("/api/benchmarks/promoted/fs/download")
def download_promoted_fs_file(path: str, scope: str = "workspace"):
    res = get_brain_options_runtime().benchmarks.execute_promoted_fs_crud(
        action="read", path=path, scope=scope
    )
    if res.get("status") != "ok":
        return Response(content=b"File not found", status_code=404)
    mime_type = str(res.get("mime_type") or "application/octet-stream")
    if res.get("base64_content"):
        raw_bytes = base64.b64decode(res["base64_content"])
    else:
        raw_bytes = str(res.get("content") or "").encode("utf-8")
    return Response(content=raw_bytes, media_type=mime_type)


@brain_options_router.get("/api/fs-crud", response_model=None)
def get_reusable_fs_crud(action: str = "list", path: str = "", scope: str = "workspace"):
    return get_brain_options_runtime().benchmarks.execute_promoted_fs_crud(
        action=action, path=path, scope=scope
    )


@brain_options_router.post("/api/fs-crud", response_model=None)
def post_reusable_fs_crud(payload: Optional[Dict[str, Any]] = Body(default=None)):
    return post_promoted_fs(payload=payload)


@brain_options_router.get(
    "/api/benchmarks/promoted/artifacts/{artifact_id:path}",
    response_model=None,
)
def get_promoted_artifact_detail(artifact_id: str):
    detail = get_brain_options_runtime().benchmarks.get_promoted_artifact_detail(artifact_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Unknown promoted artifact: {artifact_id}")
    return detail


# ------------------------------------------------------------------
# 3. Unified Qdrant Staging, Informational Graph & Geodesic MCP Endpoints
# ------------------------------------------------------------------
@brain_options_router.get("/api/qdrant/staging/overview", response_model=None)
def get_qdrant_staging_overview():
    return get_brain_options_runtime().informational_index.get_staging_overview()


@brain_options_router.post("/api/qdrant/staging/sync", response_model=None)
def sync_qdrant_staging(payload: Optional[Dict[str, Any]] = Body(default=None)):
    req = payload or {}
    collections = req.get("collections")
    return get_brain_options_runtime().informational_index.sync_staging(
        target_environment=req.get("target_environment"),
        direction=str(req.get("direction") or "bidirectional"),
        collections=collections if isinstance(collections, list) else None,
        rebuild_graph=bool(req.get("rebuild_graph", True)),
    )


@brain_options_router.get("/api/qdrant/staging/map", response_model=None)
def query_informational_map(
    query: Optional[str] = "",
    node_type: Optional[str] = None,
    repo_name: Optional[str] = None,
    keep_in_place: Optional[bool] = None,
    top_k: int = 25,
):
    return get_brain_options_runtime().informational_index.query_juxtaposed_map(
        query=query or "",
        node_type=node_type,
        repo_name=repo_name,
        keep_in_place=keep_in_place,
        top_k=top_k,
    )


@brain_options_router.get("/api/qdrant/staging/trace", response_model=None)
def trace_informational_wayfinding(start: str, target: str):
    return get_brain_options_runtime().informational_index.trace_wayfinding_path(
        start_query=start,
        target_query=target,
    )


@brain_options_router.get("/api/qdrant/staging/systems", response_model=None)
def list_qdrant_systems():
    return get_brain_options_runtime().informational_index.list_systems(
        include_collections=True,
        include_lifecycle=True,
    )


@brain_options_router.get("/api/qdrant/staging/juxtaposition", response_model=None)
def get_design_compiler_code_juxtaposition(
    query: str = "PolyphonicStageCompilerCascadeModel",
    system_id: Optional[str] = None,
    include_geodesic_trace: bool = True,
):
    return get_brain_options_runtime().informational_index.get_design_compiler_code_juxtaposition(
        query=query,
        system_id=system_id,
        include_geodesic_trace=include_geodesic_trace,
    )


@brain_options_router.get("/api/qdrant/staging/geodesic-code-path", response_model=None)
def trace_full_geodesic_code_path(
    start: str = "",
    target: str = "",
    system_id: Optional[str] = None,
    include_source_excerpts: bool = True,
):
    return get_brain_options_runtime().informational_index.trace_full_geodesic_code_path(
        start_query=start,
        target_query=target,
        system_id=system_id,
        include_source_excerpts=include_source_excerpts,
    )


@brain_options_router.post("/api/qdrant/staging/lifecycle", response_model=None)
def manage_qdrant_system_lifecycle(payload: Optional[Dict[str, Any]] = Body(default=None)):
    body = payload or {}
    return get_brain_options_runtime().informational_index.manage_system_lifecycle(
        action=str(body.get("action") or "status"),
        system_id=str(body.get("system_id") or "all"),
        target_environment=body.get("target_environment"),
        direction=str(body.get("direction") or "bidirectional"),
        options=body.get("options"),
    )


@brain_options_router.get("/api/wiki/filesystem", response_model=None)
def get_wiki_filesystem():
    return get_brain_options_runtime().informational_index.get_wiki_filesystem_tree()


@brain_options_router.get("/api/wiki/files/content", response_model=None)
def get_wiki_file_content(path: str = "", max_lines: int = 500):
    try:
        return get_brain_options_runtime().informational_index.get_wiki_file_content(
            file_path=path,
            max_lines=max_lines,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@brain_options_router.get("/api/wiki/geodesic-line-trace", response_model=None)
def trace_wiki_geodesic_from_line(
    path: str = "",
    line: int = 1,
    symbol: Optional[str] = "",
    target: Optional[str] = "ui:components:AsyncChatWindow",
):
    try:
        return get_brain_options_runtime().informational_index.trace_geodesic_from_file_line(
            file_path=path,
            line_number=line,
            symbol=symbol or "",
            target_query=target or "ui:components:AsyncChatWindow",
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@brain_options_router.post("/api/mcp/geodesic", response_model=None)
def handle_geodesic_mcp_jsonrpc(payload: Any = Body(...)):
    return get_brain_options_runtime().geodesic_mcp.handle_jsonrpc_message(payload)


@brain_options_router.get("/api/mcp/geodesic", response_model=None)
def get_geodesic_mcp_info():
    mcp = get_brain_options_runtime().geodesic_mcp
    return {
        "server_name": "qdrant-geodesic-lifecycle",
        "version": "1.0.0",
        "tools": mcp.get_tools_manifest(),
        "resources": mcp.get_resources_manifest(),
        "prompts": mcp.get_prompts_manifest(),
        "endpoint": "POST /api/mcp/geodesic (JSON-RPC 2.0)",
    }


@brain_options_router.get("/api/mcp/geodesic/tools", response_model=None)
def get_geodesic_mcp_tools():
    return get_geodesic_mcp_info()


@brain_options_router.post("/api/mcp/geodesic/call", response_model=None)
def call_geodesic_mcp_tool(payload: Dict[str, Any] = Body(...)):
    name = str(payload.get("name") or payload.get("tool") or "")
    arguments = payload.get("arguments") or {}
    return get_brain_options_runtime().geodesic_mcp.call_tool(name, arguments)


# ------------------------------------------------------------------
# 4. Bidirectional WebSocket Streaming Endpoints
# ------------------------------------------------------------------
@brain_options_router.websocket("/api/antigravity/ws/chat")
async def antigravity_chat_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    _ACTIVE_BRAIN_SOCKETS.add(websocket)
    runtime = get_brain_options_runtime()

    sub_state: Dict[str, Any] = {
        "conversation_id": websocket.query_params.get("conversation_id") or None,
        "query": websocket.query_params.get("query") or "",
        "time_range": websocket.query_params.get("time_range") or "all",
        "role_filter": websocket.query_params.get("role_filter") or "ALL",
        "sort_order": websocket.query_params.get("sort_order") or "asc",
        "top_k": int(websocket.query_params.get("top_k") or 120),
    }
    last_sent_sig = ""

    async def _send_snapshot(frame_type: str = "snapshot") -> None:
        nonlocal last_sent_sig
        res = await asyncio.to_thread(
            runtime.antigravity_brain.search_brain,
            query=str(sub_state.get("query") or ""),
            conversation_id=sub_state.get("conversation_id"),
            time_range=sub_state.get("time_range"),
            role_filter=sub_state.get("role_filter"),
            sort_order=str(sub_state.get("sort_order") or "asc"),
            top_k=int(sub_state.get("top_k") or 120),
        )
        live_sig = str((res.get("qdrant_status") or {}).get("live_signature") or "")
        results = res.get("results", [])
        last_sent_sig = live_sig
        await websocket.send_json(
            {
                "type": frame_type,
                "conversation_id": sub_state.get("conversation_id") or "All",
                "query": sub_state.get("query") or "",
                "time_range": sub_state.get("time_range") or "all",
                "role_filter": sub_state.get("role_filter") or "ALL",
                "sort_order": sub_state.get("sort_order") or "asc",
                "total_results": res.get("total_results", len(results)),
                "messages": results,
                "live_signature": live_sig,
                "timestamp": round(time.time(), 3),
            }
        )

    async def _watch_brain_deltas() -> None:
        sleep_seconds = max(5.0, float(get_setting("antigravity.watcher_interval_seconds", 20)))
        try:
            while runtime.is_file_watcher_enabled:
                await asyncio.sleep(sleep_seconds)
                if not runtime.is_file_watcher_enabled:
                    return
                curr_sig = await asyncio.to_thread(
                    runtime.antigravity_brain.ledger.get_live_brain_signature
                )
                if curr_sig and curr_sig != last_sent_sig:
                    await _send_snapshot(frame_type="stream_delta")
        except (asyncio.CancelledError, Exception):
            return

    watcher_task: asyncio.Task[None] | None = (
        asyncio.create_task(_watch_brain_deltas())
        if runtime.is_file_watcher_enabled
        else None
    )
    try:
        await _send_snapshot(frame_type="snapshot")
        while True:
            raw_msg = await websocket.receive_json()
            if not isinstance(raw_msg, dict):
                continue
            msg_type = str(raw_msg.get("type") or "").strip().lower()
            if msg_type == "ping":
                await websocket.send_json({"type": "pong", "ts": round(time.time(), 3)})
            elif msg_type == "subscribe":
                for key in (
                    "conversation_id",
                    "query",
                    "time_range",
                    "role_filter",
                    "sort_order",
                    "top_k",
                ):
                    if key in raw_msg:
                        sub_state[key] = raw_msg[key]
                await _send_snapshot(frame_type="snapshot")
            elif msg_type == "user_message":
                conv_id = str(
                    raw_msg.get("conversation_id") or sub_state.get("conversation_id") or ""
                )
                text = str(raw_msg.get("text") or raw_msg.get("message") or "").strip()
                if not text:
                    continue
                chat_res = await asyncio.to_thread(
                    runtime.antigravity_brain.send_bidirectional_chat_message,
                    conv_id,
                    text,
                )
                user_card = chat_res["user_message"]
                agent_card = chat_res["agent_message"]
                chunks = chat_res.get("stream_chunks") or [agent_card.get("message_text", "")]

                await websocket.send_json(
                    {
                        "type": "message_ack",
                        "conversation_id": chat_res["conversation_id"],
                        "user_message": user_card,
                        "agent_message_id": agent_card["entry_id"],
                    }
                )

                accumulated = ""
                for piece in chunks:
                    accumulated += piece
                    partial_card = {
                        **agent_card,
                        "message_text": accumulated,
                        "snippet": accumulated[:200],
                    }
                    await websocket.send_json(
                        {
                            "type": "stream_chunk",
                            "conversation_id": chat_res["conversation_id"],
                            "entry_id": agent_card["entry_id"],
                            "delta": piece,
                            "accumulated_text": accumulated,
                            "message": partial_card,
                        }
                    )
                    await asyncio.sleep(0.015)

                complete_frame = {
                    "type": "stream_complete",
                    "conversation_id": chat_res["conversation_id"],
                    "user_message": user_card,
                    "agent_message": agent_card,
                }
                for peer in list(_ACTIVE_BRAIN_SOCKETS):
                    try:
                        await peer.send_json(complete_frame)
                    except Exception:
                        pass
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if watcher_task is not None:
            watcher_task.cancel()
        _ACTIVE_BRAIN_SOCKETS.discard(websocket)


@brain_options_router.websocket("/api/benchmarks/promoted/ws/chat")
async def promoted_model_chat_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    _ACTIVE_PROMOTED_SOCKETS.add(websocket)
    runtime = get_brain_options_runtime()

    sub_state: Dict[str, Any] = {
        "conversation_id": websocket.query_params.get("conversation_id")
        or "promoted_model_harness",
        "action": websocket.query_params.get("action") or "learn",
        "steps": int(websocket.query_params.get("steps") or 3),
        "max_new_tokens": int(websocket.query_params.get("max_new_tokens") or 36),
        "temperature": float(websocket.query_params.get("temperature") or 0.7),
        "model_id": websocket.query_params.get("model_id") or "",
    }

    async def _send_promoted_snapshot(frame_type: str = "snapshot") -> None:
        conv_id = str(sub_state.get("conversation_id") or "").strip() or None
        hist = await asyncio.to_thread(
            runtime.benchmarks.get_promoted_chat_history, conv_id
        )
        messages = hist.get("messages", [])
        await websocket.send_json(
            {
                "type": frame_type,
                "conversation_id": hist.get("active_conversation_id")
                or "promoted_model_harness",
                "active_conversation": hist.get("active_conversation"),
                "conversations": hist.get("conversations", []),
                "available_models": hist.get("available_models", []),
                "active_chat_model": hist.get("active_chat_model"),
                "fs_tree": hist.get("fs_tree", []),
                "total_results": len(messages),
                "messages": messages,
                "harness_status": hist.get("harness_status"),
                "qdrant_lookup": hist.get("qdrant_lookup"),
                "promoted_model": hist.get("promoted_model"),
                "timestamp": round(time.time(), 3),
            }
        )

    try:
        await _send_promoted_snapshot(frame_type="snapshot")
        while True:
            raw_msg = await websocket.receive_json()
            if not isinstance(raw_msg, dict):
                continue
            msg_type = str(raw_msg.get("type") or "").strip().lower()
            if msg_type == "ping":
                await websocket.send_json({"type": "pong", "ts": round(time.time(), 3)})
            elif msg_type == "subscribe":
                for key in (
                    "conversation_id",
                    "action",
                    "steps",
                    "max_new_tokens",
                    "temperature",
                    "model_id",
                ):
                    if key in raw_msg:
                        sub_state[key] = raw_msg[key]
                await _send_promoted_snapshot(frame_type="snapshot")
            elif msg_type == "select_model":
                model_id = str(raw_msg.get("model_id") or "").strip()
                if model_id:
                    await asyncio.to_thread(
                        runtime.benchmarks.select_promoted_chat_model, model_id
                    )
                    sub_state["model_id"] = model_id
                await _send_promoted_snapshot(frame_type="snapshot")
            elif msg_type == "open_conversation":
                conv_id = str(raw_msg.get("conversation_id") or "").strip()
                if conv_id:
                    await asyncio.to_thread(
                        runtime.benchmarks.open_promoted_conversation, conv_id
                    )
                    sub_state["conversation_id"] = conv_id
                await _send_promoted_snapshot(frame_type="snapshot")
            elif msg_type == "new_conversation":
                title = str(raw_msg.get("title") or "").strip()
                model_id = str(
                    raw_msg.get("model_id") or sub_state.get("model_id") or ""
                ).strip()
                created = await asyncio.to_thread(
                    runtime.benchmarks.create_promoted_conversation, title, model_id
                )
                sub_state["conversation_id"] = (
                    created.get("active_conversation_id") or "promoted_model_harness"
                )
                await _send_promoted_snapshot(frame_type="snapshot")
            elif msg_type == "reset_knowledge":
                await asyncio.to_thread(runtime.benchmarks.reset_promoted_knowledge)
                await _send_promoted_snapshot(frame_type="snapshot")
            elif msg_type == "user_message":
                text = str(raw_msg.get("text") or raw_msg.get("message") or "").strip()
                if not text:
                    continue
                action = str(raw_msg.get("action") or sub_state.get("action") or "learn")
                steps = int(raw_msg.get("steps") or sub_state.get("steps") or 3)
                max_new_tokens = int(
                    raw_msg.get("max_new_tokens")
                    or sub_state.get("max_new_tokens")
                    or 36
                )
                temperature = float(
                    raw_msg.get("temperature") or sub_state.get("temperature") or 0.7
                )
                conv_id = str(
                    raw_msg.get("conversation_id")
                    or sub_state.get("conversation_id")
                    or ""
                ).strip()
                model_id = str(
                    raw_msg.get("model_id") or sub_state.get("model_id") or ""
                ).strip()
                chat_res = await asyncio.to_thread(
                    runtime.benchmarks.send_promoted_chat_message,
                    message_text=text,
                    action=action,
                    steps=steps,
                    max_new_tokens=max_new_tokens,
                    temperature=temperature,
                    conversation_id=conv_id or None,
                    model_id=model_id or None,
                )
                user_card = chat_res["user_message"]
                agent_card = chat_res["agent_message"]
                active_conv_id = (
                    chat_res.get("conversation_id")
                    or conv_id
                    or "promoted_model_harness"
                )
                sub_state["conversation_id"] = active_conv_id
                chunks = chat_res.get("stream_chunks") or [
                    agent_card.get("message_text", "")
                ]

                await websocket.send_json(
                    {
                        "type": "message_ack",
                        "conversation_id": active_conv_id,
                        "user_message": user_card,
                        "agent_message_id": agent_card["entry_id"],
                    }
                )

                accumulated = ""
                for piece in chunks:
                    accumulated += piece
                    partial_card = {
                        **agent_card,
                        "message_text": accumulated,
                        "snippet": accumulated[:200],
                    }
                    await websocket.send_json(
                        {
                            "type": "stream_chunk",
                            "conversation_id": active_conv_id,
                            "entry_id": agent_card["entry_id"],
                            "delta": piece,
                            "accumulated_text": accumulated,
                            "message": partial_card,
                        }
                    )
                    await asyncio.sleep(0.015)

                complete_frame = {
                    "type": "stream_complete",
                    "conversation_id": active_conv_id,
                    "user_message": user_card,
                    "agent_message": agent_card,
                    "conversations": chat_res.get("conversations", []),
                    "active_chat_model": chat_res.get("active_chat_model"),
                    "fs_tree": chat_res.get("fs_tree", []),
                    "harness_status": chat_res.get("harness_status"),
                }
                for peer in list(_ACTIVE_PROMOTED_SOCKETS):
                    try:
                        await peer.send_json(complete_frame)
                    except Exception:
                        pass
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _ACTIVE_PROMOTED_SOCKETS.discard(websocket)
