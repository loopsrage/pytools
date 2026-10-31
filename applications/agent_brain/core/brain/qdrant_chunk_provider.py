from __future__ import annotations

import hashlib
import os
import sys
import threading
from pathlib import Path
from typing import Any

from qdrant_client import QdrantClient

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from indexes.informational_index.informational_index import _deterministic_384d_vector
from thread_safe.index import Index
from thread_safe.onceler import Onceler

QDRANT_NAMESPACE_UUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

SUPPORTED_CPU_EMBEDDING_MODELS: list[dict[str, Any]] = [
    {
        "id": "sentence-transformers/all-MiniLM-L6-v2",
        "label": "all-MiniLM-L6-v2 (384d CPU Fast)",
        "dimensions": 384,
        "device": "cpu",
    },
    {
        "id": "BAAI/bge-small-en-v1.5",
        "label": "BAAI/bge-small-en-v1.5 (384d CPU High-Precision)",
        "dimensions": 384,
        "device": "cpu",
    },
]

MODEL_DIMENSIONS: dict[str, int] = {
    m["id"]: int(m["dimensions"]) for m in SUPPORTED_CPU_EMBEDDING_MODELS
}

CPU_MODEL_ONCELER = Onceler()
EMBEDDING_VECTOR_CACHE = Index()
EMBEDDING_VECTOR_CACHE.new("VECTORS")


def _try_get_sentence_transformer(model_name: str) -> Any | None:
    """Loads SentenceTransformer once per process when installed; returns None if unavailable."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None

    resolved_name = (
        model_name
        if model_name in MODEL_DIMENSIONS
        else "sentence-transformers/all-MiniLM-L6-v2"
    )

    def _load() -> Any:
        try:
            return SentenceTransformer(resolved_name, device="cpu", local_files_only=True)
        except Exception:
            return SentenceTransformer(resolved_name, device="cpu")

    try:
        model, _ = CPU_MODEL_ONCELER.store_once("CPU_MODELS", resolved_name, _load)
        return model
    except Exception:
        return None


def encode_texts_cpu(texts: list[str], model_name: str) -> list[list[float]]:
    """
    Encodes a batch of strings on CPU into 384-D unit-normalized vectors with thread-safe
    Index caching. Uses `SentenceTransformer` when installed in the active Python environment,
    falling back deterministically to `_deterministic_384d_vector` (trigram + token hashing).
    """
    if not texts:
        return []
    resolved_name = (
        model_name
        if model_name in MODEL_DIMENSIONS
        else "sentence-transformers/all-MiniLM-L6-v2"
    )
    dim = MODEL_DIMENSIONS.get(resolved_name, 384)
    results: list[list[float] | None] = [None] * len(texts)
    missing_indices: list[int] = []
    missing_texts: list[str] = []
    missing_keys: list[str] = []

    for idx, t in enumerate(texts):
        cache_key = f"{resolved_name}:{hashlib.sha256(t.encode('utf-8')).hexdigest()}"
        cached_vec = EMBEDDING_VECTOR_CACHE.load_from_index("VECTORS", cache_key)
        if cached_vec is not None:
            results[idx] = cached_vec
        else:
            missing_indices.append(idx)
            missing_texts.append(t)
            missing_keys.append(cache_key)

    if missing_texts:
        st_model = _try_get_sentence_transformer(resolved_name)
        if st_model is not None:
            encoded = st_model.encode(
                missing_texts,
                normalize_embeddings=True,
                show_progress_bar=False,
                convert_to_numpy=True,
            )
            for m_idx, cache_key, vec_np in zip(missing_indices, missing_keys, encoded):
                vec_list = [round(float(x), 6) for x in vec_np.tolist()]
                EMBEDDING_VECTOR_CACHE.store_in_index("VECTORS", cache_key, vec_list)
                results[m_idx] = vec_list
        else:
            for m_idx, cache_key, raw_text in zip(missing_indices, missing_keys, missing_texts):
                vec_list = _deterministic_384d_vector(raw_text, dim=dim)
                EMBEDDING_VECTOR_CACHE.store_in_index("VECTORS", cache_key, vec_list)
                results[m_idx] = vec_list

    return [r or [] for r in results]


def chunk_document_text(
    text: str,
    chunk_size: int = 380,
    chunk_overlap: int = 64,
) -> list[dict[str, Any]]:
    """
    Splits `text` into overlapping semantic chunks for Qdrant vector indexing while
    recording exact `core_start`, `core_end`, and `overlap_prefix_len` offsets so that
    `rejoin_chunks` reconstructs `text` with lossless byte-for-byte SHA-256 fidelity.
    """
    if not text:
        return []

    effective_size = max(120, int(chunk_size))
    effective_overlap = max(0, min(int(chunk_overlap), effective_size // 2))
    n = len(text)

    if n <= effective_size:
        return [
            {
                "chunk_index": 0,
                "total_chunks": 1,
                "char_start": 0,
                "char_end": n,
                "core_start": 0,
                "core_end": n,
                "overlap_prefix_len": 0,
                "chunk_text": text,
                "non_overlapping_slice": text,
            }
        ]

    core_ranges: list[tuple[int, int]] = []
    cursor = 0
    while cursor < n:
        target_end = min(n, cursor + effective_size)
        if target_end < n:
            min_split = cursor + max(80, effective_size // 2)
            window = text[min_split:target_end]
            nl_pos = window.rfind("\n")
            sp_pos = window.rfind(" ")
            if nl_pos != -1:
                target_end = min_split + nl_pos + 1
            elif sp_pos != -1:
                target_end = min_split + sp_pos + 1
        if target_end <= cursor:
            target_end = min(n, cursor + effective_size)
        core_ranges.append((cursor, target_end))
        cursor = target_end

    total = len(core_ranges)
    chunks: list[dict[str, Any]] = []
    for idx, (c_start, c_end) in enumerate(core_ranges):
        if idx == 0 or effective_overlap == 0:
            o_start = c_start
        else:
            raw_o_start = max(0, c_start - effective_overlap)
            overlap_window = text[raw_o_start:c_start]
            first_space = overlap_window.find(" ")
            if 0 <= first_space < len(overlap_window) - 1 and raw_o_start > 0:
                o_start = raw_o_start + first_space + 1
            else:
                o_start = raw_o_start

        overlap_prefix_len = c_start - o_start
        chunk_text = text[o_start:c_end]
        non_overlapping = chunk_text[overlap_prefix_len:]
        chunks.append(
            {
                "chunk_index": idx,
                "total_chunks": total,
                "char_start": o_start,
                "char_end": c_end,
                "core_start": c_start,
                "core_end": c_end,
                "overlap_prefix_len": overlap_prefix_len,
                "chunk_text": chunk_text,
                "non_overlapping_slice": non_overlapping,
            }
        )
    return chunks


def rejoin_chunks(chunk_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Re-joins a set of Qdrant chunk payloads for a single document in `chunk_index` order,
    stripping the overlap prefix from each chunk and verifying chain continuity and SHA-256 integrity.
    """
    if not chunk_payloads:
        return {
            "doc_id": "",
            "rejoined_text": "",
            "chunk_count": 0,
            "expected_chunks": 0,
            "chain_continuous": False,
            "integrity_verified": False,
            "rejoined_sha256": "",
            "expected_sha256": "",
            "chunks": [],
        }

    ordered = sorted(chunk_payloads, key=lambda c: int(c.get("chunk_index", 0)))
    first = ordered[0]
    doc_id = str(first.get("doc_id", ""))
    expected_total = int(first.get("total_chunks", len(ordered)))
    expected_sha256 = str(first.get("content_sha256", ""))

    slices: list[str] = []
    chain_ok = len(ordered) == expected_total
    for i, ch in enumerate(ordered):
        if int(ch.get("chunk_index", -1)) != i:
            chain_ok = False
        if i > 0:
            prev_id = ordered[i - 1].get("point_id")
            if ch.get("prev_chunk_id") != prev_id:
                chain_ok = False
        if i < len(ordered) - 1:
            next_id = ordered[i + 1].get("point_id")
            if ch.get("next_chunk_id") != next_id:
                chain_ok = False

        c_text = str(ch.get("chunk_text", ""))
        ov_len = int(ch.get("overlap_prefix_len", 0))
        slices.append(c_text[ov_len:])

    rejoined_text = "".join(slices)
    rejoined_sha256 = hashlib.sha256(rejoined_text.encode("utf-8")).hexdigest()
    integrity_ok = bool(chain_ok and expected_sha256 and rejoined_sha256 == expected_sha256)

    return {
        "doc_id": doc_id,
        "title": first.get("title", doc_id),
        "doc_type": first.get("doc_type", "ki"),
        "domain_group": first.get("domain_group", ""),
        "rejoined_text": rejoined_text,
        "char_length": len(rejoined_text),
        "chunk_count": len(ordered),
        "expected_chunks": expected_total,
        "chain_continuous": chain_ok,
        "integrity_verified": integrity_ok,
        "rejoined_sha256": rejoined_sha256,
        "expected_sha256": expected_sha256,
        "relational_graph": first.get("relational_graph", {}),
        "chunks": [
            {
                "point_id": c.get("point_id"),
                "chunk_index": c.get("chunk_index"),
                "total_chunks": c.get("total_chunks"),
                "char_start": c.get("char_start"),
                "char_end": c.get("char_end"),
                "core_start": c.get("core_start"),
                "core_end": c.get("core_end"),
                "overlap_prefix_len": c.get("overlap_prefix_len"),
                "prev_chunk_id": c.get("prev_chunk_id"),
                "next_chunk_id": c.get("next_chunk_id"),
                "chunk_text": c.get("chunk_text"),
                "score": c.get("score"),
            }
            for c in ordered
        ],
    }


class QdrantChunkClientProvider:
    """
    Thread-safe Qdrant client provider shared across `AntigravityBrainService` and
    `NeuralBenchmarkService` inside `applications.agent_brain`.
    """

    def __init__(
        self,
        storage_path: str | Path | None = None,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        chunk_size: int = 380,
        chunk_overlap: int = 64,
        prefer_memory: bool = False,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
    ) -> None:
        self.storage_path = Path(storage_path).resolve() if storage_path else None
        self.embedding_model = (
            embedding_model
            if embedding_model in MODEL_DIMENSIONS
            else "sentence-transformers/all-MiniLM-L6-v2"
        )
        self.chunk_size = max(120, int(chunk_size or 380))
        self.chunk_overlap = max(0, min(int(chunk_overlap or 64), self.chunk_size // 2))
        self.prefer_memory = bool(prefer_memory)
        self.qdrant_url = (qdrant_url or os.environ.get("QDRANT_URL") or "").strip() or None
        self.qdrant_api_key = (qdrant_api_key or os.environ.get("QDRANT_API_KEY") or "").strip() or None
        self._lock = threading.RLock()
        self._qdrant_client: QdrantClient | None = None
        self.storage_mode: str = "uninitialized"
        self.active_storage_location: str = ""

    def _ensure_qdrant_client(self) -> QdrantClient:
        with self._lock:
            if self._qdrant_client is not None:
                return self._qdrant_client

            if self.qdrant_url:
                self._qdrant_client = QdrantClient(
                    url=self.qdrant_url,
                    api_key=self.qdrant_api_key,
                )
                self.storage_mode = "remote_qdrant"
                self.active_storage_location = self.qdrant_url
            elif not self.prefer_memory and self.storage_path is not None:
                try:
                    self.storage_path.mkdir(parents=True, exist_ok=True)
                    self._qdrant_client = QdrantClient(path=str(self.storage_path))
                    self.storage_mode = "local_disk"
                    self.active_storage_location = str(self.storage_path)
                except Exception:
                    pid_path = self.storage_path.parent / f".qdrant_options_pid_{os.getpid()}"
                    try:
                        pid_path.mkdir(parents=True, exist_ok=True)
                        self._qdrant_client = QdrantClient(path=str(pid_path))
                        self.storage_mode = "local_disk_isolated"
                        self.active_storage_location = str(pid_path)
                    except Exception:
                        self._qdrant_client = QdrantClient(":memory:")
                        self.storage_mode = "memory"
                        self.active_storage_location = ":memory:"
            else:
                self._qdrant_client = QdrantClient(":memory:")
                self.storage_mode = "memory"
                self.active_storage_location = ":memory:"

            return self._qdrant_client

    def close(self) -> None:
        with self._lock:
            if self._qdrant_client is not None:
                try:
                    self._qdrant_client.close()
                except Exception:
                    pass
                self._qdrant_client = None
