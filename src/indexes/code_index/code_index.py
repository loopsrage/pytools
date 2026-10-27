from __future__ import annotations

import ast
import datetime as dt
import hashlib
import heapq
import io
import json
import math
import threading
import time
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, Field

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.datasource_index.datasource_index import Datasource
from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.queueController import QueueController
from service_controller.service_controller import ServiceController
from simple_functions.simple_functions import uuid5_from_args
from stats_collector.stats_collector import new_stats
from superlative_times.superlative_times import SuperlativeTimes
from thread_safe.index import Index
from thread_safe.tslist import TsList

CODE_INDEX_NAMESPACE_UUID = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"

DEFAULT_EXCLUDE_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        "dist",
        "build",
        ".qdrant_data",
        ".idea",
        "checkpoints",
    }
)


def _deterministic_vector_384(text: str, dim: int = 384) -> list[float]:
    """Deterministic unit-norm embedding vector fallback when sentence-transformers is not used."""
    cleaned = text.strip() or "empty"
    vecs: list[float] = []
    block_idx = 0
    while len(vecs) < dim:
        digest = hashlib.sha256(f"{block_idx}:{cleaned}".encode("utf-8")).digest()
        for b in digest:
            if len(vecs) >= dim:
                break
            vecs.append((b / 127.5) - 1.0)
        block_idx += 1
    norm = math.sqrt(sum(x * x for x in vecs)) or 1.0
    return [round(x / norm, 6) for x in vecs]


def project_to_poincare_ball(
    vec: list[float],
    curvature: float = 1.0,
    poincare_dim: int = 16,
) -> list[float]:
    """
    Projects a Euclidean embedding vector onto the Poincare ball B^d_c (||x|| < 1/sqrt(c))
    via block-pooled exponential map at the origin.
    """
    if not vec:
        return [0.0] * poincare_dim
    stride = max(1, len(vec) // poincare_dim)
    pooled: list[float] = []
    for i in range(poincare_dim):
        start = i * stride
        chunk = vec[start : start + stride] or [0.0]
        pooled.append(sum(chunk) / len(chunk))
    norm = math.sqrt(sum(x * x for x in pooled))
    if norm < 1e-12:
        return [0.0] * poincare_dim
    sqrt_c = math.sqrt(max(curvature, 1e-6))
    scale = math.tanh(sqrt_c * norm * 0.85) / (sqrt_c * norm)
    max_r = (1.0 / sqrt_c) - 1e-5
    cur_r = norm * scale
    if cur_r >= max_r:
        scale = max_r / norm
    return [round(x * scale, 6) for x in pooled]


def poincare_geodesic_distance(
    u: list[float],
    v: list[float],
    curvature: float = 1.0,
) -> float:
    """
    Computes exact hyperbolic geodesic distance on the Poincare ball B^d_c:
      d_B(u, v) = (1/sqrt(c)) * arcosh(1 + 2*c*||u - v||^2 / ((1 - c*||u||^2)*(1 - c*||v||^2)))
    """
    if not u or not v:
        return 0.0
    dim = min(len(u), len(v))
    diff_sq = sum((u[i] - v[i]) ** 2 for i in range(dim))
    norm_u_sq = sum(u[i] ** 2 for i in range(dim))
    norm_v_sq = sum(v[i] ** 2 for i in range(dim))
    c = max(curvature, 1e-6)
    denom = max(1.0 - c * norm_u_sq, 1e-6) * max(1.0 - c * norm_v_sq, 1e-6)
    arg = max(1.0, 1.0 + (2.0 * c * diff_sq) / denom)
    return round(math.acosh(arg) / math.sqrt(c), 6)


def poincare_midpoint(
    u: list[float],
    v: list[float],
    curvature: float = 1.0,
) -> list[float]:
    """Computes conformal Lorentz-factor midpoint on the Poincare ball B^d_c."""
    if not u or not v:
        return []
    dim = min(len(u), len(v))
    c = max(curvature, 1e-6)
    norm_u_sq = sum(u[i] ** 2 for i in range(dim))
    norm_v_sq = sum(v[i] ** 2 for i in range(dim))
    gamma_u = 2.0 / max(1.0 - c * norm_u_sq, 1e-6)
    gamma_v = 2.0 / max(1.0 - c * norm_v_sq, 1e-6)
    denom = max(gamma_u + gamma_v - 1.0, 1e-6)
    raw = [((gamma_u * u[i] + gamma_v * v[i]) / denom) for i in range(dim)]
    norm_r = math.sqrt(sum(x * x for x in raw))
    max_r = (1.0 / math.sqrt(c)) - 1e-5
    if norm_r >= max_r and norm_r > 0:
        raw = [x * (max_r / norm_r) for x in raw]
    return [round(x, 6) for x in raw]


class CodeSymbolMetadata(BaseModel):
    name: str
    symbol_type: str  # "class" | "function" | "method"
    lineno: int = 1
    end_lineno: int = 1
    bases: list[str] = Field(default_factory=list)
    methods: list[str] = Field(default_factory=list)
    calls: list[str] = Field(default_factory=list)
    docstring: str = ""


class CodeGraphEdge(BaseModel):
    source: str
    target: str
    edge_type: str  # "imports" | "inherits" | "calls" | "sibling_module"
    weight: float = 1.0
    geodesic_distance: float = 0.0
    shared_symbols: list[str] = Field(default_factory=list)


class CodeFileRecord(BaseModel):
    doc_id: str
    rel_path: str
    module_name: str
    sha256: str
    previous_sha256: str = ""
    temporal_version: int = 1
    is_head: bool = True
    size_bytes: int = 0
    mtime_ns: int = 0
    line_count: int = 0
    recorded_at: str
    classes: list[str] = Field(default_factory=list)
    functions: list[str] = Field(default_factory=list)
    methods: list[str] = Field(default_factory=list)
    imports: list[str] = Field(default_factory=list)
    imported_files: list[str] = Field(default_factory=list)
    called_symbols: list[str] = Field(default_factory=list)
    base_classes: list[str] = Field(default_factory=list)
    symbols: list[CodeSymbolMetadata] = Field(default_factory=list)
    poincare_coords: list[float] = Field(default_factory=list)
    chunk_count: int = 1
    content: str = ""


def _rel_path_to_module(rel_path: str) -> str:
    clean = rel_path.strip("/").replace("\\", "/")
    if clean.endswith(".py"):
        clean = clean[:-3]
    if clean.endswith("/__init__"):
        clean = clean[: -len("/__init__")]
    return clean.replace("/", ".")


def _parse_python_ast_metadata(source: str, rel_path: str) -> dict[str, Any]:
    module_name = _rel_path_to_module(rel_path)
    classes: list[str] = []
    functions: list[str] = []
    methods: list[str] = []
    imports: list[str] = []
    called_symbols: list[str] = []
    base_classes: list[str] = []
    symbols: list[CodeSymbolMetadata] = []

    try:
        tree = ast.parse(source)
    except Exception:
        return {
            "module_name": module_name,
            "classes": classes,
            "functions": functions,
            "methods": methods,
            "imports": imports,
            "called_symbols": called_symbols,
            "base_classes": base_classes,
            "symbols": symbols,
        }

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name and alias.name not in imports:
                    imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            base_mod = node.module or ""
            if base_mod and base_mod not in imports:
                imports.append(base_mod)
            for alias in node.names:
                full_imp = f"{base_mod}.{alias.name}" if base_mod else alias.name
                if full_imp and full_imp not in imports:
                    imports.append(full_imp)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id not in called_symbols:
                    called_symbols.append(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                if node.func.attr not in called_symbols:
                    called_symbols.append(node.func.attr)

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            classes.append(node.name)
            cls_bases: list[str] = []
            for b in node.bases:
                if isinstance(b, ast.Name):
                    cls_bases.append(b.id)
                    if b.id not in base_classes:
                        base_classes.append(b.id)
                elif isinstance(b, ast.Attribute):
                    cls_bases.append(b.attr)
                    if b.attr not in base_classes:
                        base_classes.append(b.attr)
            cls_methods: list[str] = []
            cls_calls: list[str] = []
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    cls_methods.append(sub.name)
                    if sub.name not in methods:
                        methods.append(sub.name)
                    for inner in ast.walk(sub):
                        if isinstance(inner, ast.Call):
                            if isinstance(inner.func, ast.Name) and inner.func.id not in cls_calls:
                                cls_calls.append(inner.func.id)
                            elif isinstance(inner.func, ast.Attribute) and inner.func.attr not in cls_calls:
                                cls_calls.append(inner.func.attr)
            symbols.append(
                CodeSymbolMetadata(
                    name=node.name,
                    symbol_type="class",
                    lineno=int(getattr(node, "lineno", 1)),
                    end_lineno=int(getattr(node, "end_lineno", getattr(node, "lineno", 1))),
                    bases=cls_bases,
                    methods=cls_methods,
                    calls=cls_calls,
                    docstring=(ast.get_docstring(node) or "")[:400],
                )
            )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(node.name)
            fn_calls: list[str] = []
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    if isinstance(inner.func, ast.Name) and inner.func.id not in fn_calls:
                        fn_calls.append(inner.func.id)
                    elif isinstance(inner.func, ast.Attribute) and inner.func.attr not in fn_calls:
                        fn_calls.append(inner.func.attr)
            symbols.append(
                CodeSymbolMetadata(
                    name=node.name,
                    symbol_type="function",
                    lineno=int(getattr(node, "lineno", 1)),
                    end_lineno=int(getattr(node, "end_lineno", getattr(node, "lineno", 1))),
                    calls=fn_calls,
                    docstring=(ast.get_docstring(node) or "")[:400],
                )
            )

    return {
        "module_name": module_name,
        "classes": classes,
        "functions": functions,
        "methods": methods,
        "imports": imports,
        "called_symbols": called_symbols,
        "base_classes": base_classes,
        "symbols": symbols,
    }


class CodeCorpusIndex(Datasource):
    """
    Reusable FSBase + Qdrant Temporal Code Corpus Index with:
      1. Qdrant SHA-256 payload index lookup BEFORE saving (`has_sha256_in_qdrant` / `search_sha256_in_qdrant`)
         so embeddings and writes only occur when code actually changes.
      2. Full CRUD operations (`create_file` / `upsert_file`, `get_file`, `get_by_sha256`,
         `update_file`, `delete_file`, `list_files`, `rejoin_file`).
      3. Temporal version history tracking (`TEMPORAL_HISTORY` + Qdrant `temporal_version` / `previous_sha256`)
         and `verify_code_unchanged()` gate for benchmark/model promotion pipelines.
      4. AST Code Graph construction (`imports`, `inherits`, `calls`, `sibling_module`) with
         Poincare ball hyperbolic coordinates and `get_geodesic_code_trace(start, target)`
         for computing shortest hyperbolic geodesic code traces across the codebase.
      5. Model architecture & parameter fingerprint indexing (`index_model_artifact` / `list_indexed_models`).
    """

    def __init__(
        self,
        repo_path: str | Path,
        qdrant_path: str | Path | None = None,
        qdrant_client: Any | None = None,
        collection_name: str = "code_corpus_temporal_index",
        ledger_path: str | Path | None = None,
        include_globs: tuple[str, ...] = ("*.py",),
        exclude_dirs: frozenset[str] = DEFAULT_EXCLUDE_DIRS,
        chunk_size: int = 600,
        chunk_overlap: int = 80,
        curvature: float = 1.0,
        use_fast_hash_embeddings: bool = True,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> None:
        super().__init__()
        self.repo_path = Path(repo_path).resolve()
        self.qdrant_path = Path(qdrant_path).resolve() if qdrant_path is not None else None
        self._external_client = qdrant_client
        self.collection_name = collection_name
        self.ledger_path = Path(ledger_path).resolve() if ledger_path is not None else None
        self.include_globs = include_globs
        self.exclude_dirs = exclude_dirs
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.curvature = curvature
        self.use_fast_hash_embeddings = use_fast_hash_embeddings
        self.embedding_model = embedding_model

        self._fs = FSBase(filesystem="file")
        self._lock = threading.RLock()
        self._index = Index()
        self._index.new("FILES")
        self._index.new("SHA256_MAP")
        self._index.new("CHUNKS_BY_FILE")
        self._index.new("GRAPH_NODES")
        self._index.new("GRAPH_EDGES")
        self._index.new("TEMPORAL_HISTORY")
        self._index.new("MODELS")
        self._history_list = TsList()
        self._baseline_corpus_digest: str = ""
        self._last_synced_at: str = ""

        if self.ledger_path is not None and self.ledger_path.exists():
            self._load_persisted_ledger()

    # ------------------------------------------------------------------
    # Internal FSBase & Qdrant Client Helpers
    # ------------------------------------------------------------------
    def _read_text_via_fsbase(self, abs_path: Path) -> str:
        buf = io.BytesIO()
        self._fs.read(str(abs_path), buf)
        return buf.getvalue().decode("utf-8", errors="replace")

    def _write_text_via_fsbase(self, abs_path: Path, content: str) -> None:
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        buf = io.BytesIO(content.encode("utf-8"))
        self._fs.write(str(abs_path), buf)

    def _open_qdrant_client(self) -> tuple[Any | None, bool]:
        if self._external_client is not None:
            return self._external_client, False
        if self.qdrant_path is None:
            return None, False
        from qdrant_client import QdrantClient

        self.qdrant_path.mkdir(parents=True, exist_ok=True)
        try:
            client = QdrantClient(path=str(self.qdrant_path))
            return client, True
        except Exception:
            return None, False

    def _ensure_collection_and_sha256_index(self, client: Any) -> None:
        from qdrant_client.models import Distance, PayloadSchemaType, VectorParams

        if not client.collection_exists(self.collection_name):
            client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            )
            inner_cls = type(getattr(client, "_client", None)).__name__
            if "Local" not in inner_cls:
                for field_name, schema_type in (
                    ("sha256", PayloadSchemaType.KEYWORD),
                    ("rel_path", PayloadSchemaType.KEYWORD),
                    ("doc_type", PayloadSchemaType.KEYWORD),
                    ("is_head", PayloadSchemaType.BOOL),
                ):
                    try:
                        client.create_payload_index(
                            collection_name=self.collection_name,
                            field_name=field_name,
                            field_schema=schema_type,
                        )
                    except Exception:
                        pass

    def _encode_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if not self.use_fast_hash_embeddings:
            try:
                from sentence_transformers import SentenceTransformer

                model = SentenceTransformer(self.embedding_model, device="cpu", local_files_only=True)
                arr = model.encode(
                    texts,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                    convert_to_numpy=True,
                )
                return [[round(float(x), 6) for x in row.tolist()] for row in arr]
            except Exception:
                pass
        return [_deterministic_vector_384(t, dim=384) for t in texts]

    def _chunk_source(self, text: str) -> list[dict[str, Any]]:
        if not text:
            return [
                {
                    "chunk_index": 0,
                    "char_start": 0,
                    "char_end": 0,
                    "core_start": 0,
                    "core_end": 0,
                    "overlap_prefix_len": 0,
                    "chunk_text": "",
                    "non_overlapping_slice": "",
                }
            ]
        step = max(64, self.chunk_size - self.chunk_overlap)
        chunks: list[dict[str, Any]] = []
        pos = 0
        idx = 0
        text_len = len(text)
        while pos < text_len:
            c_start = max(0, pos - self.chunk_overlap) if pos > 0 else 0
            c_end = min(text_len, pos + step)
            chunks.append(
                {
                    "chunk_index": idx,
                    "char_start": c_start,
                    "char_end": c_end,
                    "core_start": pos,
                    "core_end": c_end,
                    "overlap_prefix_len": pos - c_start,
                    "chunk_text": text[c_start:c_end],
                    "non_overlapping_slice": text[pos:c_end],
                }
            )
            if c_end >= text_len:
                break
            pos = c_end
            idx += 1
        return chunks

    # ------------------------------------------------------------------
    # Qdrant SHA-256 Pre-Save Lookup
    # ------------------------------------------------------------------
    def search_sha256_in_qdrant(
        self,
        sha256_hash: str,
        rel_path: str | None = None,
        only_head: bool = False,
        client: Any | None = None,
    ) -> list[dict[str, Any]]:
        """
        Searches the Qdrant `sha256` index for matching code points BEFORE saving.
        Falls back to the thread-safe in-memory `SHA256_MAP` index when Qdrant is offline/locked.
        """
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        owns = False
        active_client = client
        if active_client is None:
            active_client, owns = self._open_qdrant_client()

        matches: list[dict[str, Any]] = []
        if active_client is not None:
            try:
                self._ensure_collection_and_sha256_index(active_client)
                must_conds = [
                    FieldCondition(key="sha256", match=MatchValue(value=sha256_hash)),
                ]
                if rel_path is not None:
                    must_conds.append(
                        FieldCondition(key="rel_path", match=MatchValue(value=rel_path))
                    )
                if only_head:
                    must_conds.append(
                        FieldCondition(key="is_head", match=MatchValue(value=True))
                    )
                scrolled, _ = active_client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=Filter(must=must_conds),
                    limit=64,
                    with_payload=True,
                    with_vectors=False,
                )
                for pt in scrolled:
                    if pt.payload:
                        matches.append(dict(pt.payload))
            finally:
                if owns and hasattr(active_client, "close"):
                    try:
                        active_client.close()
                    except Exception:
                        pass

        if not matches:
            cached = self._index.load_from_index("SHA256_MAP", sha256_hash)
            if isinstance(cached, list):
                for item in cached:
                    if rel_path is not None and item.get("rel_path") != rel_path:
                        continue
                    if only_head and not item.get("is_head", True):
                        continue
                    matches.append(dict(item))
        return matches

    def has_sha256_in_qdrant(
        self,
        sha256_hash: str,
        rel_path: str | None = None,
        only_head: bool = True,
        client: Any | None = None,
    ) -> bool:
        """Returns True if `sha256_hash` is already indexed in Qdrant (checked before saving)."""
        return len(
            self.search_sha256_in_qdrant(
                sha256_hash=sha256_hash,
                rel_path=rel_path,
                only_head=only_head,
                client=client,
            )
        ) > 0

    # ------------------------------------------------------------------
    # CRUD Operations
    # ------------------------------------------------------------------
    def upsert_file(
        self,
        rel_path: str,
        content: str | None = None,
        write_to_fs: bool = False,
        client: Any | None = None,
        rebuild_graph: bool = True,
    ) -> dict[str, Any]:
        """
        CREATE / UPDATE operation:
        1. Reads file via FSBase (or writes `content` to FSBase if `write_to_fs=True`).
        2. Computes SHA-256 hash and queries Qdrant's `sha256` index BEFORE saving.
        3. If the SHA-256 hash already exists as head for `rel_path`, skips re-saving (`status="unchanged_sha256"`).
        4. Otherwise parses AST graph metadata, computes Poincare ball coordinates, increments
           `temporal_version`, and upserts chunks into Qdrant and `Index`.
        """
        from qdrant_client.models import FieldCondition, Filter, MatchValue, PointStruct

        with self._lock:
            norm_rel = rel_path.strip("/").replace("\\", "/")
            abs_path = self.repo_path / norm_rel
            if content is not None and write_to_fs:
                self._write_text_via_fsbase(abs_path, content)

            if content is None:
                if not abs_path.is_file():
                    raise FileNotFoundError(f"Code file not found: {abs_path}")
                content = self._read_text_via_fsbase(abs_path)

            stat = abs_path.stat() if abs_path.exists() else None
            size_bytes = int(stat.st_size) if stat is not None else len(content.encode("utf-8"))
            mtime_ns = int(stat.st_mtime_ns) if stat is not None else time.time_ns()
            sha256_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

            owns = False
            active_client = client
            if active_client is None:
                active_client, owns = self._open_qdrant_client()

            try:
                # 1. Search Qdrant for SHA-256 hash BEFORE saving
                existing_in_qdrant = self.has_sha256_in_qdrant(
                    sha256_hash=sha256_hash,
                    rel_path=norm_rel,
                    only_head=True,
                    client=active_client,
                )
                existing_rec = self._index.load_from_index("FILES", norm_rel)
                if existing_in_qdrant and existing_rec is not None and existing_rec.get("sha256") == sha256_hash:
                    return {
                        "status": "unchanged_sha256",
                        "changed": False,
                        "rel_path": norm_rel,
                        "sha256": sha256_hash,
                        "temporal_version": int(existing_rec.get("temporal_version", 1)),
                    }

                prev_sha = str(existing_rec.get("sha256", "")) if isinstance(existing_rec, dict) else ""
                prev_ver = int(existing_rec.get("temporal_version", 0)) if isinstance(existing_rec, dict) else 0
                new_ver = prev_ver + 1 if prev_sha and prev_sha != sha256_hash else max(1, prev_ver)
                recorded_at = dt.datetime.now(dt.timezone.utc).isoformat()
                doc_id = f"code_{uuid5_from_args(CODE_INDEX_NAMESPACE_UUID, norm_rel)[:16]}"

                ast_meta = _parse_python_ast_metadata(content, norm_rel)
                summary_embed_input = (
                    f"module {ast_meta['module_name']} path {norm_rel} "
                    f"classes {' '.join(ast_meta['classes'])} "
                    f"functions {' '.join(ast_meta['functions'])} "
                    f"imports {' '.join(ast_meta['imports'])}\n{content[:1200]}"
                )
                file_vec = self._encode_texts([summary_embed_input])[0]
                poincare_coords = project_to_poincare_ball(
                    file_vec, curvature=self.curvature, poincare_dim=16
                )

                chunks = self._chunk_source(content)
                total_c = len(chunks)
                point_ids = [
                    uuid5_from_args(
                        CODE_INDEX_NAMESPACE_UUID,
                        self.collection_name,
                        norm_rel,
                        sha256_hash,
                        idx,
                    )
                    for idx in range(total_c)
                ]

                chunk_texts = [
                    f"[{norm_rel}#chunk={c['chunk_index']}] {c['chunk_text']}"
                    for c in chunks
                ]
                chunk_vecs = self._encode_texts(chunk_texts)

                chunk_payloads: list[dict[str, Any]] = []
                for idx, c in enumerate(chunks):
                    payload = {
                        "point_id": point_ids[idx],
                        "doc_id": doc_id,
                        "doc_type": "code_file_chunk",
                        "rel_path": norm_rel,
                        "module_name": ast_meta["module_name"],
                        "sha256": sha256_hash,
                        "previous_sha256": prev_sha,
                        "temporal_version": new_ver,
                        "is_head": True,
                        "size_bytes": size_bytes,
                        "mtime_ns": mtime_ns,
                        "line_count": len(content.splitlines()),
                        "recorded_at": recorded_at,
                        "classes": ast_meta["classes"],
                        "functions": ast_meta["functions"],
                        "methods": ast_meta["methods"],
                        "imports": ast_meta["imports"],
                        "called_symbols": ast_meta["called_symbols"],
                        "base_classes": ast_meta["base_classes"],
                        "poincare_coords": poincare_coords,
                        "chunk_index": c["chunk_index"],
                        "total_chunks": total_c,
                        "char_start": c["char_start"],
                        "char_end": c["char_end"],
                        "core_start": c["core_start"],
                        "core_end": c["core_end"],
                        "overlap_prefix_len": c["overlap_prefix_len"],
                        "chunk_text": c["chunk_text"],
                        "non_overlapping_slice": c["non_overlapping_slice"],
                        "prev_chunk_id": point_ids[idx - 1] if idx > 0 else None,
                        "next_chunk_id": point_ids[idx + 1] if idx + 1 < total_c else None,
                    }
                    chunk_payloads.append(payload)

                if active_client is not None:
                    self._ensure_collection_and_sha256_index(active_client)
                    if prev_sha and prev_sha != sha256_hash:
                        # Mark previous head chunks for this rel_path as non-head in Qdrant
                        try:
                            active_client.set_payload(
                                collection_name=self.collection_name,
                                payload={"is_head": False},
                                points=Filter(
                                    must=[
                                        FieldCondition(key="rel_path", match=MatchValue(value=norm_rel)),
                                        FieldCondition(key="is_head", match=MatchValue(value=True)),
                                    ]
                                ),
                            )
                        except Exception:
                            pass
                    points = [
                        PointStruct(
                            id=pl["point_id"],
                            vector=vec,
                            payload=pl,
                        )
                        for pl, vec in zip(chunk_payloads, chunk_vecs)
                    ]
                    if points:
                        active_client.upsert(collection_name=self.collection_name, points=points)

                # Update in-memory SHA256_MAP & mark previous sha as non-head
                if prev_sha and prev_sha != sha256_hash:
                    old_entries = self._index.load_from_index("SHA256_MAP", prev_sha) or []
                    for oe in old_entries:
                        if oe.get("rel_path") == norm_rel:
                            oe["is_head"] = False
                    self._index.store_in_index("SHA256_MAP", prev_sha, old_entries)

                record = CodeFileRecord(
                    doc_id=doc_id,
                    rel_path=norm_rel,
                    module_name=ast_meta["module_name"],
                    sha256=sha256_hash,
                    previous_sha256=prev_sha,
                    temporal_version=new_ver,
                    is_head=True,
                    size_bytes=size_bytes,
                    mtime_ns=mtime_ns,
                    line_count=len(content.splitlines()),
                    recorded_at=recorded_at,
                    classes=ast_meta["classes"],
                    functions=ast_meta["functions"],
                    methods=ast_meta["methods"],
                    imports=ast_meta["imports"],
                    called_symbols=ast_meta["called_symbols"],
                    base_classes=ast_meta["base_classes"],
                    symbols=ast_meta["symbols"],
                    poincare_coords=poincare_coords,
                    chunk_count=total_c,
                    content=content,
                )
                rec_dump = record.model_dump()
                self._index.store_in_index("FILES", norm_rel, rec_dump)
                self._index.store_in_index("CHUNKS_BY_FILE", norm_rel, chunk_payloads)

                sha_entries = self._index.load_from_index("SHA256_MAP", sha256_hash) or []
                sha_entries = [e for e in sha_entries if e.get("rel_path") != norm_rel]
                sha_entries.append(
                    {
                        "doc_id": doc_id,
                        "rel_path": norm_rel,
                        "module_name": ast_meta["module_name"],
                        "sha256": sha256_hash,
                        "previous_sha256": prev_sha,
                        "temporal_version": new_ver,
                        "is_head": True,
                        "recorded_at": recorded_at,
                    }
                )
                self._index.store_in_index("SHA256_MAP", sha256_hash, sha_entries)

                hist_Event = {
                    "rel_path": norm_rel,
                    "sha256": sha256_hash,
                    "previous_sha256": prev_sha,
                    "temporal_version": new_ver,
                    "recorded_at": recorded_at,
                }
                file_hist = self._index.load_from_index("TEMPORAL_HISTORY", norm_rel) or []
                file_hist.append(hist_Event)
                self._index.store_in_index("TEMPORAL_HISTORY", norm_rel, file_hist)
                self._history_list.append(hist_Event)

                if rebuild_graph:
                    self._rebuild_code_graph()

                return {
                    "status": "indexed",
                    "changed": True,
                    "rel_path": norm_rel,
                    "sha256": sha256_hash,
                    "previous_sha256": prev_sha,
                    "temporal_version": new_ver,
                    "chunk_count": total_c,
                }
            finally:
                if owns and active_client is not None and hasattr(active_client, "close"):
                    try:
                        active_client.close()
                    except Exception:
                        pass

    def create_file(
        self,
        rel_path: str,
        content: str,
        write_to_fs: bool = True,
    ) -> dict[str, Any]:
        """CRUD CREATE alias for `upsert_file`."""
        return self.upsert_file(rel_path=rel_path, content=content, write_to_fs=write_to_fs)

    def get_file(
        self,
        rel_path: str,
        rejoin_from_qdrant: bool = False,
    ) -> dict[str, Any] | None:
        """CRUD READ: Retrieve an indexed file record by relative path, optionally rejoining chunks."""
        norm_rel = rel_path.strip("/").replace("\\", "/")
        rec = self._index.load_from_index("FILES", norm_rel)
        if rec is None:
            return None
        out = dict(rec)
        if rejoin_from_qdrant:
            out["rejoined"] = self.rejoin_file(norm_rel)
        out["temporal_history"] = self._index.load_from_index("TEMPORAL_HISTORY", norm_rel) or []
        return out

    def get_by_sha256(self, sha256_hash: str) -> list[dict[str, Any]]:
        """CRUD READ by SHA-256: Query Qdrant's `sha256` payload index and return matching records."""
        return self.search_sha256_in_qdrant(sha256_hash=sha256_hash, only_head=False)

    def update_file(
        self,
        rel_path: str,
        new_content: str,
        write_to_fs: bool = True,
    ) -> dict[str, Any]:
        """CRUD UPDATE: Modify a code file and incrementally index the new temporal version."""
        return self.upsert_file(rel_path=rel_path, content=new_content, write_to_fs=write_to_fs)

    def delete_file(
        self,
        rel_path: str,
        delete_from_fs: bool = False,
    ) -> dict[str, Any]:
        """CRUD DELETE: Remove a file from `Index`, Qdrant, and optionally disk via `FSBase`."""
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        with self._lock:
            norm_rel = rel_path.strip("/").replace("\\", "/")
            existing = self._index.load_from_index("FILES", norm_rel)
            self._index.delete_from_index("FILES", norm_rel)
            self._index.delete_from_index("CHUNKS_BY_FILE", norm_rel)
            self._index.delete_from_index("GRAPH_NODES", norm_rel)
            if isinstance(existing, dict) and existing.get("sha256"):
                sha = str(existing["sha256"])
                entries = self._index.load_from_index("SHA256_MAP", sha) or []
                entries = [e for e in entries if e.get("rel_path") != norm_rel]
                self._index.store_in_index("SHA256_MAP", sha, entries)

            client, owns = self._open_qdrant_client()
            if client is not None:
                try:
                    if client.collection_exists(self.collection_name):
                        client.delete(
                            collection_name=self.collection_name,
                            points_selector=Filter(
                                must=[FieldCondition(key="rel_path", match=MatchValue(value=norm_rel))]
                            ),
                        )
                finally:
                    if owns and hasattr(client, "close"):
                        try:
                            client.close()
                        except Exception:
                            pass

            abs_path = self.repo_path / norm_rel
            if delete_from_fs and abs_path.is_file():
                abs_path.unlink()

            self._rebuild_code_graph()
            return {
                "status": "deleted",
                "rel_path": norm_rel,
                "existed": existing is not None,
            }

    def list_files(self) -> list[dict[str, Any]]:
        """List all indexed code files ordered by `rel_path`."""
        items: list[dict[str, Any]] = []
        for _, rec in self._index.range_index("FILES"):
            summary = dict(rec)
            summary.pop("content", None)
            items.append(summary)
        items.sort(key=lambda r: str(r.get("rel_path", "")))
        return items

    def rejoin_file(self, rel_path: str) -> dict[str, Any]:
        """Losslessly rejoin overlapping chunks for `rel_path` and verify SHA-256 integrity."""
        norm_rel = rel_path.strip("/").replace("\\", "/")
        chunks = list(self._index.load_from_index("CHUNKS_BY_FILE", norm_rel) or [])
        chunks.sort(key=lambda c: int(c.get("chunk_index", 0)))
        if not chunks:
            return {"rel_path": norm_rel, "integrity_verified": False, "rejoined_text": ""}
        rejoined = "".join(str(c.get("non_overlapping_slice", "")) for c in chunks)
        rejoined_sha = hashlib.sha256(rejoined.encode("utf-8")).hexdigest()
        expected_sha = str(chunks[0].get("sha256", ""))
        return {
            "rel_path": norm_rel,
            "chunk_count": len(chunks),
            "expected_sha256": expected_sha,
            "rejoined_sha256": rejoined_sha,
            "integrity_verified": rejoined_sha == expected_sha,
            "rejoined_text": rejoined,
        }

    # ------------------------------------------------------------------
    # Full Corpus Scan & Temporal Unchanged Verification
    # ------------------------------------------------------------------
    def _discover_repo_code_files(self, subdirs: list[str] | None = None) -> list[Path]:
        roots = [self.repo_path / sd for sd in subdirs] if subdirs else [self.repo_path]
        discovered: list[Path] = []
        seen: set[Path] = set()
        for root in roots:
            if not root.exists():
                continue
            for pattern in self.include_globs:
                for p in sorted(root.rglob(pattern)):
                    if not p.is_file():
                        continue
                    rel_parts = p.relative_to(self.repo_path).parts
                    if any(part in self.exclude_dirs or part.startswith(".") for part in rel_parts):
                        continue
                    if p not in seen:
                        seen.add(p)
                        discovered.append(p)
        return discovered

    def sync_corpus(
        self,
        subdirs: list[str] | None = None,
        record_as_baseline: bool = False,
    ) -> dict[str, Any]:
        """
        Scans the repository via `FSBase`, checks Qdrant's `sha256` index before saving each file,
        only embeds/upserts changed files, rebuilds the AST + Poincare geodesic code graph,
        and persists the ledger if `ledger_path` is configured.
        """
        with self._lock:
            t0 = time.perf_counter()
            files = self._discover_repo_code_files(subdirs=subdirs)
            client, owns = self._open_qdrant_client()
            indexed_count = 0
            unchanged_count = 0
            changed_paths: list[str] = []

            try:
                if client is not None:
                    self._ensure_collection_and_sha256_index(client)
                for abs_p in files:
                    rel_p = abs_p.relative_to(self.repo_path).as_posix()
                    res = self.upsert_file(
                        rel_path=rel_p,
                        client=client,
                        rebuild_graph=False,
                    )
                    if res.get("changed"):
                        indexed_count += 1
                        changed_paths.append(rel_p)
                    else:
                        unchanged_count += 1
                self._rebuild_code_graph()
            finally:
                if owns and client is not None and hasattr(client, "close"):
                    try:
                        client.close()
                    except Exception:
                        pass

            corpus_digest = self.compute_corpus_digest()
            if record_as_baseline or not self._baseline_corpus_digest:
                self._baseline_corpus_digest = corpus_digest
            self._last_synced_at = dt.datetime.now(dt.timezone.utc).isoformat()

            if self.ledger_path is not None:
                self.persist_ledger(self.ledger_path)

            return {
                "status": "synced",
                "total_files": len(files),
                "changed_files_indexed": indexed_count,
                "unchanged_files_skipped": unchanged_count,
                "changed_paths": changed_paths,
                "corpus_digest": corpus_digest,
                "baseline_corpus_digest": self._baseline_corpus_digest,
                "graph_nodes_count": len(list(self._index.range_index("GRAPH_NODES"))),
                "graph_edges_count": len(list(self._index.range_index("GRAPH_EDGES"))),
                "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

    def compute_corpus_digest(self) -> str:
        """Computes a canonical aggregate SHA-256 digest over all indexed `(rel_path, sha256)` pairs."""
        pairs: list[str] = []
        for rel_p, rec in sorted(self._index.range_index("FILES"), key=lambda kv: kv[0]):
            pairs.append(f"{rel_p}:{rec.get('sha256', '')}")
        return hashlib.sha256("\n".join(pairs).encode("utf-8")).hexdigest()

    def snapshot_temporal_baseline(self, subdirs: list[str] | None = None) -> dict[str, Any]:
        """Indexes the current code corpus into Qdrant and locks the current state as the temporal baseline."""
        return self.sync_corpus(subdirs=subdirs, record_as_baseline=True)

    def verify_code_unchanged(
        self,
        subdirs: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Verifies via `FSBase` + Qdrant `sha256` lookup whether the existing code on disk
        is 100% unchanged relative to the indexed Qdrant temporal state.
        Used as Condition #1 of the model promotion gate (`unchanged == True`).
        """
        with self._lock:
            files = self._discover_repo_code_files(subdirs=subdirs)
            client, owns = self._open_qdrant_client()
            modified_files: list[str] = []
            unindexed_files: list[str] = []
            verified_files: int = 0

            try:
                for abs_p in files:
                    rel_p = abs_p.relative_to(self.repo_path).as_posix()
                    content = self._read_text_via_fsbase(abs_p)
                    disk_sha = hashlib.sha256(content.encode("utf-8")).hexdigest()

                    indexed_rec = self._index.load_from_index("FILES", rel_p)
                    in_qdrant = self.has_sha256_in_qdrant(
                        sha256_hash=disk_sha,
                        rel_path=rel_p,
                        only_head=True,
                        client=client,
                    )
                    if indexed_rec is None and not in_qdrant:
                        unindexed_files.append(rel_p)
                    elif (indexed_rec is not None and indexed_rec.get("sha256") != disk_sha) or not in_qdrant:
                        modified_files.append(rel_p)
                    else:
                        verified_files += 1
            finally:
                if owns and client is not None and hasattr(client, "close"):
                    try:
                        client.close()
                    except Exception:
                        pass

            current_digest = self.compute_corpus_digest()
            baseline_matches = (
                not self._baseline_corpus_digest
                or current_digest == self._baseline_corpus_digest
            )
            unchanged = (
                len(modified_files) == 0
                and len(unindexed_files) == 0
                and baseline_matches
                and verified_files > 0
            )
            return {
                "unchanged": unchanged,
                "verified_files_count": verified_files,
                "modified_files": modified_files,
                "unindexed_files": unindexed_files,
                "corpus_digest": current_digest,
                "baseline_corpus_digest": self._baseline_corpus_digest,
            }

    # ------------------------------------------------------------------
    # AST Code Graph & Hyperbolic Geodesic Code Traces
    # ------------------------------------------------------------------
    def _rebuild_code_graph(self) -> None:
        self._index.delete_index("GRAPH_NODES")
        self._index.new("GRAPH_NODES")
        self._index.delete_index("GRAPH_EDGES")
        self._index.new("GRAPH_EDGES")

        all_files: dict[str, dict[str, Any]] = {
            rel_p: dict(rec) for rel_p, rec in self._index.range_index("FILES")
        }
        mod_to_rel: dict[str, str] = {}
        class_to_rel: dict[str, str] = {}
        func_to_rel: dict[str, str] = {}

        for rel_p, rec in all_files.items():
            mod = str(rec.get("module_name", ""))
            if mod:
                mod_to_rel[mod] = rel_p
                short_mod = mod.split(".")[-1]
                mod_to_rel.setdefault(short_mod, rel_p)
            for cls_name in rec.get("classes", []):
                class_to_rel[cls_name] = rel_p
            for fn_name in rec.get("functions", []):
                func_to_rel[fn_name] = rel_p

            self._index.store_in_index(
                "GRAPH_NODES",
                rel_p,
                {
                    "node_id": rel_p,
                    "module_name": mod,
                    "sha256": rec.get("sha256", ""),
                    "classes": rec.get("classes", []),
                    "functions": rec.get("functions", []),
                    "poincare_coords": rec.get("poincare_coords", []),
                },
            )

        for rel_p, rec in all_files.items():
            u_coords = list(rec.get("poincare_coords", []))
            resolved_imports: list[str] = []

            # 1. Import edges
            for imp in rec.get("imports", []):
                target_rel = mod_to_rel.get(imp)
                if target_rel is None and "." in imp:
                    prefix = ".".join(imp.split(".")[:-1])
                    last_tok = imp.split(".")[-1]
                    target_rel = mod_to_rel.get(prefix) or class_to_rel.get(last_tok) or func_to_rel.get(last_tok)
                if target_rel and target_rel != rel_p:
                    if target_rel not in resolved_imports:
                        resolved_imports.append(target_rel)
                    v_coords = list(all_files[target_rel].get("poincare_coords", []))
                    geo_d = max(0.01, poincare_geodesic_distance(u_coords, v_coords, self.curvature))
                    edge_key = f"{rel_p}->{target_rel}:imports"
                    self._index.store_in_index(
                        "GRAPH_EDGES",
                        edge_key,
                        CodeGraphEdge(
                            source=rel_p,
                            target=target_rel,
                            edge_type="imports",
                            weight=geo_d,
                            geodesic_distance=geo_d,
                            shared_symbols=[imp],
                        ).model_dump(),
                    )

            # 2. Inheritance edges
            for base_cls in rec.get("base_classes", []):
                target_rel = class_to_rel.get(base_cls)
                if target_rel and target_rel != rel_p:
                    v_coords = list(all_files[target_rel].get("poincare_coords", []))
                    geo_d = max(0.01, poincare_geodesic_distance(u_coords, v_coords, self.curvature))
                    edge_key = f"{rel_p}->{target_rel}:inherits"
                    self._index.store_in_index(
                        "GRAPH_EDGES",
                        edge_key,
                        CodeGraphEdge(
                            source=rel_p,
                            target=target_rel,
                            edge_type="inherits",
                            weight=geo_d * 0.85,
                            geodesic_distance=geo_d,
                            shared_symbols=[base_cls],
                        ).model_dump(),
                    )

            # 3. Call edges
            for called in rec.get("called_symbols", []):
                target_rel = class_to_rel.get(called) or func_to_rel.get(called)
                if target_rel and target_rel != rel_p:
                    v_coords = list(all_files[target_rel].get("poincare_coords", []))
                    geo_d = max(0.01, poincare_geodesic_distance(u_coords, v_coords, self.curvature))
                    edge_key = f"{rel_p}->{target_rel}:calls"
                    existing_e = self._index.load_from_index("GRAPH_EDGES", edge_key)
                    if existing_e is None:
                        self._index.store_in_index(
                            "GRAPH_EDGES",
                            edge_key,
                            CodeGraphEdge(
                                source=rel_p,
                                target=target_rel,
                                edge_type="calls",
                                weight=geo_d,
                                geodesic_distance=geo_d,
                                shared_symbols=[called],
                            ).model_dump(),
                        )
                    else:
                        shared = list(existing_e.get("shared_symbols", []))
                        if called not in shared:
                            shared.append(called)
                            existing_e["shared_symbols"] = shared
                            self._index.store_in_index("GRAPH_EDGES", edge_key, existing_e)

            rec["imported_files"] = resolved_imports
            self._index.store_in_index("FILES", rel_p, rec)

    def _resolve_node_identifier(self, identifier: str) -> str | None:
        cleaned = identifier.strip().strip("/")
        if self._index.load_from_index("FILES", cleaned) is not None:
            return cleaned
        for rel_p, rec in self._index.range_index("FILES"):
            if (
                rel_p.endswith(cleaned)
                or rec.get("module_name") == cleaned
                or cleaned in rec.get("classes", [])
                or cleaned in rec.get("functions", [])
            ):
                return rel_p
        return None

    def get_geodesic_code_trace(
        self,
        start: str,
        target: str,
        max_hops: int = 12,
    ) -> dict[str, Any]:
        """
        Computes the minimum-hyperbolic-length geodesic code trace between `start` and `target`
        (accepting file paths, module names, class names, or function names) across the AST
        code graph embedded on the Poincare ball B^d_c.
        """
        start_rel = self._resolve_node_identifier(start)
        target_rel = self._resolve_node_identifier(target)
        if start_rel is None or target_rel is None:
            raise KeyError(f"Could not resolve start={start!r} ({start_rel}) or target={target!r} ({target_rel})")

        all_nodes = {k: v for k, v in self._index.range_index("GRAPH_NODES")}
        adj: dict[str, list[dict[str, Any]]] = {k: [] for k in all_nodes}
        for _, edge in self._index.range_index("GRAPH_EDGES"):
            src = str(edge["source"])
            dst = str(edge["target"])
            if src in adj and dst in adj:
                adj[src].append(edge)
                # Also add reverse traversal edge with slight penalty so any connected component is traceable
                rev_edge = dict(edge)
                rev_edge["source"] = dst
                rev_edge["target"] = src
                rev_edge["edge_type"] = f"rev_{edge['edge_type']}"
                rev_edge["weight"] = round(float(edge["weight"]) * 1.15, 6)
                adj[dst].append(rev_edge)

        # Dijkstra shortest hyperbolic geodesic path
        dist_map: dict[str, float] = {start_rel: 0.0}
        prev_map: dict[str, tuple[str, dict[str, Any]]] = {}
        pq: list[tuple[float, int, str]] = [(0.0, 0, start_rel)]

        while pq:
            cur_d, hops_used, u = heapq.heappop(pq)
            if u == target_rel:
                break
            if cur_d > dist_map.get(u, float("inf")) or hops_used >= max_hops:
                continue
            for edge in adj.get(u, []):
                v = str(edge["target"])
                w = float(edge["weight"])
                cand_d = round(cur_d + w, 6)
                if cand_d < dist_map.get(v, float("inf")):
                    dist_map[v] = cand_d
                    prev_map[v] = (u, edge)
                    heapq.heappush(pq, (cand_d, hops_used + 1, v))

        start_coords = list(all_nodes[start_rel].get("poincare_coords", []))
        target_coords = list(all_nodes[target_rel].get("poincare_coords", []))
        direct_geo = poincare_geodesic_distance(start_coords, target_coords, self.curvature)

        path_nodes: list[str] = []
        hops: list[dict[str, Any]] = []
        if target_rel == start_rel:
            path_nodes = [start_rel]
        elif target_rel in prev_map:
            cur = target_rel
            rev_nodes = [cur]
            rev_hops: list[dict[str, Any]] = []
            while cur in prev_map:
                parent, edge_used = prev_map[cur]
                u_c = list(all_nodes[parent].get("poincare_coords", []))
                v_c = list(all_nodes[cur].get("poincare_coords", []))
                rev_hops.append(
                    {
                        "from_node": parent,
                        "to_node": cur,
                        "edge_type": edge_used["edge_type"],
                        "geodesic_distance": float(edge_used["geodesic_distance"]),
                        "shared_symbols": list(edge_used.get("shared_symbols", [])),
                        "poincare_midpoint": poincare_midpoint(u_c, v_c, self.curvature),
                    }
                )
                rev_nodes.append(parent)
                cur = parent
            path_nodes = list(reversed(rev_nodes))
            hops = list(reversed(rev_hops))
        else:
            # Geodesic bridge when nodes lie in separate AST import components
            path_nodes = [start_rel, target_rel]
            hops = [
                {
                    "from_node": start_rel,
                    "to_node": target_rel,
                    "edge_type": "poincare_geodesic_bridge",
                    "geodesic_distance": direct_geo,
                    "shared_symbols": [],
                    "poincare_midpoint": poincare_midpoint(
                        start_coords, target_coords, self.curvature
                    ),
                }
            ]

        total_geo = round(sum(float(h["geodesic_distance"]) for h in hops), 6)
        efficiency = round(direct_geo / max(total_geo, 1e-6), 4) if total_geo > 0 else 1.0
        return {
            "start_node": start_rel,
            "target_node": target_rel,
            "path_nodes": path_nodes,
            "hop_count": len(hops),
            "hops": hops,
            "total_geodesic_distance": total_geo,
            "direct_geodesic_distance": direct_geo,
            "geodesic_efficiency": min(1.0, efficiency),
        }

    # ------------------------------------------------------------------
    # Model Artifact Indexing in Qdrant
    # ------------------------------------------------------------------
    def index_model_artifact(
        self,
        model_id: str,
        model_name: str,
        class_name: str,
        source_file: str,
        num_params: int,
        metrics: dict[str, Any] | None = None,
        weight_signature_sha256: str = "",
        is_promoted: bool = False,
    ) -> dict[str, Any]:
        """
        Indexes a neural model architecture + parameter/source SHA-256 fingerprint into Qdrant
        (checking `sha256` first before saving).
        """
        from qdrant_client.models import PointStruct

        with self._lock:
            norm_src = source_file.strip("/").replace("\\", "/")
            file_rec = self._index.load_from_index("FILES", norm_src)
            if file_rec is None and (self.repo_path / norm_src).is_file():
                self.upsert_file(norm_src, rebuild_graph=False)
                file_rec = self._index.load_from_index("FILES", norm_src)

            code_sha = str((file_rec or {}).get("sha256", ""))
            combined_sha = hashlib.sha256(
                f"{model_id}:{class_name}:{code_sha}:{num_params}:{weight_signature_sha256}:{json.dumps(metrics or {}, sort_keys=True)}".encode(
                    "utf-8"
                )
            ).hexdigest()

            client, owns = self._open_qdrant_client()
            try:
                if self.has_sha256_in_qdrant(
                    sha256_hash=combined_sha,
                    rel_path=f"model://{model_id}",
                    only_head=True,
                    client=client,
                ):
                    existing_m = self._index.load_from_index("MODELS", model_id)
                    if existing_m is not None:
                        return {
                            "status": "unchanged_sha256",
                            "changed": False,
                            "model_id": model_id,
                            "sha256": combined_sha,
                        }

                recorded_at = dt.datetime.now(dt.timezone.utc).isoformat()
                point_id = uuid5_from_args(
                    CODE_INDEX_NAMESPACE_UUID,
                    self.collection_name,
                    "model",
                    model_id,
                    combined_sha,
                )
                embed_text = (
                    f"model {model_id} {model_name} class {class_name} "
                    f"source {norm_src} params {num_params} metrics {json.dumps(metrics or {}, sort_keys=True)}"
                )
                vec = self._encode_texts([embed_text])[0]
                poincare_coords = project_to_poincare_ball(
                    vec, curvature=self.curvature, poincare_dim=16
                )
                payload = {
                    "point_id": point_id,
                    "doc_id": f"model_artifact_{model_id}",
                    "doc_type": "indexed_model",
                    "rel_path": f"model://{model_id}",
                    "model_id": model_id,
                    "model_name": model_name,
                    "class_name": class_name,
                    "source_file": norm_src,
                    "code_sha256": code_sha,
                    "weight_signature_sha256": weight_signature_sha256,
                    "sha256": combined_sha,
                    "num_params": int(num_params),
                    "is_promoted": bool(is_promoted),
                    "is_head": True,
                    "metrics": metrics or {},
                    "poincare_coords": poincare_coords,
                    "recorded_at": recorded_at,
                }
                if client is not None:
                    self._ensure_collection_and_sha256_index(client)
                    client.upsert(
                        collection_name=self.collection_name,
                        points=[PointStruct(id=point_id, vector=vec, payload=payload)],
                    )
                self._index.store_in_index("MODELS", model_id, payload)
                self._index.store_in_index("SHA256_MAP", combined_sha, [payload])
                return {
                    "status": "indexed",
                    "changed": True,
                    "model_id": model_id,
                    "sha256": combined_sha,
                    "code_sha256": code_sha,
                }
            finally:
                if owns and client is not None and hasattr(client, "close"):
                    try:
                        client.close()
                    except Exception:
                        pass

    def list_indexed_models(self) -> list[dict[str, Any]]:
        models = [dict(m) for _, m in self._index.range_index("MODELS")]
        models.sort(key=lambda m: str(m.get("model_id", "")))
        return models

    # ------------------------------------------------------------------
    # Persistence & Datasource Interface
    # ------------------------------------------------------------------
    def persist_ledger(self, target_path: str | Path | None = None) -> Path:
        out_path = Path(target_path) if target_path is not None else self.ledger_path
        if out_path is None:
            raise ValueError("target_path or self.ledger_path must be specified")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "repo_path": str(self.repo_path),
            "collection_name": self.collection_name,
            "last_synced_at": self._last_synced_at,
            "baseline_corpus_digest": self._baseline_corpus_digest,
            "corpus_digest": self.compute_corpus_digest(),
            "files": self.list_files(),
            "graph_edges": [dict(e) for _, e in self._index.range_index("GRAPH_EDGES")],
            "indexed_models": self.list_indexed_models(),
            "temporal_events": list(self._history_list.all()),
        }
        buf = io.BytesIO(json.dumps(payload, indent=2).encode("utf-8"))
        self._fs.write(str(out_path), buf)
        return out_path

    def _load_persisted_ledger(self) -> None:
        if self.ledger_path is None or not self.ledger_path.exists():
            return
        try:
            text = self._read_text_via_fsbase(self.ledger_path)
            data = json.loads(text)
            self._baseline_corpus_digest = str(data.get("baseline_corpus_digest", ""))
            self._last_synced_at = str(data.get("last_synced_at", ""))
            for m in data.get("indexed_models", []):
                if isinstance(m, dict) and m.get("model_id"):
                    self._index.store_in_index("MODELS", str(m["model_id"]), m)
                    if m.get("sha256"):
                        self._index.store_in_index("SHA256_MAP", str(m["sha256"]), [m])
        except Exception:
            return

    def query_datasource(self, query: str = "", **kwargs: Any) -> dict[str, Any]:
        q_lower = query.strip().lower()
        files = self.list_files()
        if q_lower:
            files = [
                f
                for f in files
                if q_lower in str(f.get("rel_path", "")).lower()
                or q_lower in str(f.get("module_name", "")).lower()
                or any(q_lower in c.lower() for c in f.get("classes", []))
                or any(q_lower in fn.lower() for fn in f.get("functions", []))
            ]
        return {
            "query": query,
            "total_results": len(files),
            "results": files,
        }


class CodeCorpusPeriodicService(ServiceController, Datasource):
    """
    Periodic background service (`ServiceController` + `PeriodicProducer` + `QueueController`)
    that watches a repository via `FSBase`, checks Qdrant's `sha256` index before saving,
    and updates the code corpus + hyperbolic geodesic code graph whenever code changes.
    """

    def __init__(
        self,
        code_index: CodeCorpusIndex,
        interval_seconds: float = 2.0,
        subdirs: list[str] | None = None,
        on_code_change: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        super().__init__()
        self.code_index = code_index
        self.interval_seconds = interval_seconds
        self.subdirs = subdirs
        self.on_code_change = on_code_change
        self.stats = new_stats()
        self.times = SuperlativeTimes()
        self._last_fs_sig: str = ""

        def _consumer_action(q_data: Any) -> Any:
            payload = q_data.attribute("result")
            if payload and self.on_code_change is not None:
                self.on_code_change(payload)
            return payload

        def _producer_poll() -> list[dict[str, Any]] | None:
            cur_sig = self._compute_fs_signature()
            if cur_sig != self._last_fs_sig:
                self._last_fs_sig = cur_sig
                sync_res = self.code_index.sync_corpus(subdirs=self.subdirs)
                self.times.update()
                if sync_res.get("changed_files_indexed", 0) > 0:
                    return [sync_res]
            return None

        self.queue_controller = QueueController(action=_consumer_action, worker_count=1)
        self._producer_poll_fn = _producer_poll
        self.periodic_producer: PeriodicProducer | None = None
        self.controllers = []
        self.queues = [self.queue_controller]

    def init(self, stop_event: Any, tg: Any, logger: Any = None) -> None:
        self.periodic_producer = PeriodicProducer(
            action=self._producer_poll_fn,
            queue=self.queue_controller,
            interval=max(1, int(round(self.interval_seconds))),
            start_now=False,
        )
        self.controllers = [self.periodic_producer]
        super().init(stop_event, tg, logger=logger)

    def _compute_fs_signature(self) -> str:
        files = self.code_index._discover_repo_code_files(subdirs=self.subdirs)
        parts: list[str] = []
        for p in files:
            try:
                st = p.stat()
                parts.append(f"{p.name}:{st.st_mtime_ns}:{st.st_size}")
            except Exception:
                continue
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    def trigger_once(self) -> dict[str, Any]:
        """Synchronously executes one poll + Qdrant SHA-256 check + sync cycle."""
        self._last_fs_sig = self._compute_fs_signature()
        res = self.code_index.sync_corpus(subdirs=self.subdirs)
        if res.get("changed_files_indexed", 0) > 0 and self.on_code_change is not None:
            self.on_code_change(res)
        return res

    def query_datasource(self, query: str = "", **kwargs: Any) -> dict[str, Any]:
        return self.code_index.query_datasource(query=query, **kwargs)
