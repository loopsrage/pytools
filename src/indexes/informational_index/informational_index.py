"""
Unified Qdrant Staging & Juxtaposed Code/Documentation Informational Graph Index.

Unifies all Qdrant namespaces across sibling repositories into a permanent staging
directory above `portfolio` (`/Users/jarek/IdeaProjects/qdrant_staging`), supports
bidirectional/directional synchronization with external environments, and maintains
the `agent_informational_graph_index` collection that maps and juxtaposes code,
UI elements, `toolset` indexes, and documentation so agents never get lost.
"""

from __future__ import annotations

import ast
import base64
import collections
import hashlib
import json
import math
import pickle
import re
import shutil
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from benchmark_ledger.vocab_mask import mask_restricted_string, mask_restricted_vocabulary
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.datasource_index.datasource_index import Datasource
from simple_functions.simple_functions import uuid5_from_args
from stats_collector.stats_collector import Stats
from thread_safe.index.index import Index
from thread_safe.onceler import Onceler
from thread_safe.tslist import TsList

INFORMATIONAL_NAMESPACE_UUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
INFORMATIONAL_COLLECTION_NAME = "agent_informational_graph_index"
DEFAULT_VECTOR_SIZE = 384

DEFAULT_IDEA_PROJECTS_ROOT = Path("/Users/jarek/IdeaProjects")
DEFAULT_STAGING_ROOT = DEFAULT_IDEA_PROJECTS_ROOT / "qdrant_staging"
DEFAULT_SKILL_MD_PATH = Path("/Users/jarek/.gemini/config/skills/pytools-toolset/SKILL.md")
DEFAULT_KNOWLEDGE_ROOT = Path("/Users/jarek/.gemini/antigravity-cli/knowledge")


def _encode_sqlite_point_id(point_id: str) -> str:
    """Encodes a point ID string into Qdrant local SQLite primary key format (base64 pickle)."""
    return base64.b64encode(pickle.dumps(str(point_id))).decode("ascii")


def _decode_sqlite_point_id(encoded_id: str | bytes) -> str:
    """Decodes a Qdrant local SQLite primary key back into a string point ID."""
    try:
        raw = base64.b64decode(encoded_id) if isinstance(encoded_id, str) else encoded_id
        return str(pickle.loads(raw))
    except Exception:
        return str(encoded_id)


def _deterministic_384d_vector(text: str, dim: int = DEFAULT_VECTOR_SIZE) -> List[float]:
    """
    Computes a deterministic unit-normalized 384-D semantic feature vector from token
    trigrams and identifier stems when CPU SentenceTransformer weights are not preloaded,
    guaranteeing sub-millisecond indexing and cosine similarity wayfinding.
    """
    vec = [0.0] * dim
    tokens = re.findall(r"[a-zA-Z0-9_]{2,}", text.lower())
    if not tokens:
        vec[0] = 1.0
        return vec

    for tok in tokens:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        idx1 = h % dim
        idx2 = (h >> 16) % dim
        sign1 = 1.0 if ((h >> 32) & 1) == 0 else -1.0
        sign2 = 1.0 if ((h >> 33) & 1) == 0 else -1.0
        vec[idx1] += sign1 * 1.25
        vec[idx2] += sign2 * 0.75
        # Character 3-grams for partial symbol matching
        if len(tok) >= 3:
            for i in range(len(tok) - 2):
                tri = tok[i : i + 3]
                th = int(hashlib.md5(tri.encode("utf-8")).hexdigest(), 16)
                vec[th % dim] += 0.35 if ((th >> 8) & 1) == 0 else -0.35

    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 6) for v in vec]


def _compute_poincare_ball_coords(
    node_id: str,
    hierarchy_level: int,
    category_seed: str,
    dim: int = 16,
) -> List[float]:
    """
    Maps a node onto the 16-dimensional Poincaré ball (||x|| < 1.0) where radial norm
    reflects hierarchy depth (root repos near origin r~0.22, indexes/modules r~0.52,
    leaf UI components and files r~0.78).
    """
    radius_by_level = {
        0: 0.18,  # Root staging / workspace anchor
        1: 0.34,  # Sibling repository or Qdrant namespace
        2: 0.56,  # Toolset index / module / promoted model
        3: 0.76,  # UI component / concrete file / KI
    }
    target_r = radius_by_level.get(hierarchy_level, 0.65)
    seed_bytes = hashlib.sha256(f"{category_seed}:{node_id}".encode("utf-8")).digest()
    raw = [((seed_bytes[i % len(seed_bytes)] / 255.0) * 2.0 - 1.0) for i in range(dim)]
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [round((x / norm) * target_r, 5) for x in raw]


@dataclass
class JuxtaposedGraphEdge:
    """Directed typed relationship between two JuxtaposedNodes in the informational graph."""

    source: str
    target: str
    relation: str
    weight: float = 1.0
    rationale: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return mask_restricted_vocabulary(asdict(self))


@dataclass
class JuxtaposedNode:
    """
    Single wayfinding vertex in `agent_informational_graph_index` that juxtaposes physical
    code structure directly alongside its authoritative documentation, Qdrant namespace,
    placement invariant (`keep_in_place`), and graph edges.
    """

    node_id: str
    node_type: str
    title: str
    idiomatic_name: str
    repo_name: str
    keep_in_place: bool
    placement_directive: str
    juxtaposed_code: Dict[str, Any]
    juxtaposed_docs: Dict[str, Any]
    qdrant_namespace: str
    staging_path: str
    hierarchy_level: int = 2
    poincare_coords: List[float] = field(default_factory=list)
    graph_edges: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    updated_at: float = field(default_factory=lambda: round(time.time(), 3))

    def to_payload(self) -> Dict[str, Any]:
        payload = mask_restricted_vocabulary(asdict(self))
        payload["point_id"] = str(
            uuid5_from_args(INFORMATIONAL_NAMESPACE_UUID, "info_node", payload["node_id"])
        )
        payload["search_text"] = mask_restricted_string(self.build_search_text())
        return payload

    def build_search_text(self) -> str:
        code_part = (
            f"{self.juxtaposed_code.get('code_path', '')} "
            f"{' '.join(self.juxtaposed_code.get('exported_symbols', []))} "
            f"{self.juxtaposed_code.get('code_excerpt', '')}"
        )
        doc_part = (
            f"{self.juxtaposed_docs.get('doc_path', '')} "
            f"{self.juxtaposed_docs.get('doc_section', '')} "
            f"{self.juxtaposed_docs.get('doc_excerpt', '')}"
        )
        return (
            f"{self.node_id} {self.node_type} {self.title} {self.idiomatic_name} "
            f"{self.repo_name} {self.qdrant_namespace} {self.placement_directive} "
            f"{' '.join(self.tags)} {code_part} {doc_part}"
        )


class UnifiedQdrantStagingManager:
    """
    Manages the permanent unified Qdrant directory at `/Users/jarek/IdeaProjects/qdrant_staging`
    adjacent to sibling repositories (`components`, `portfolio`, `toolset`, `neural_networks`,
    `large_tools`, `StageCompiler-development`) and performs lock-free collection unification
    and cross-environment synchronization.
    """

    def __init__(
        self,
        staging_root: str | Path = DEFAULT_STAGING_ROOT,
        idea_projects_root: str | Path = DEFAULT_IDEA_PROJECTS_ROOT,
    ) -> None:
        self.staging_root = Path(staging_root).resolve()
        self.idea_projects_root = Path(idea_projects_root).resolve()
        self.storage_dir = self.staging_root / "storage"
        self.snapshots_dir = self.staging_root / "snapshots"
        self.manifest_path = self.staging_root / "staging_manifest.json"
        self.ledger_path = self.staging_root / "informational_graph_ledger.json"

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        (self.storage_dir / "collection").mkdir(parents=True, exist_ok=True)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

        self.staging_fs = FSBase("file")
        self.projects_fs = FSBase("file")

    def discover_source_qdrant_stores(self) -> List[Dict[str, Any]]:
        """
        Discovers all repo-local Qdrant storage directories across sibling repositories.
        """
        candidates = [
            {
                "source_id": "portfolio_qdrant_data",
                "repo_name": "portfolio",
                "path": self.idea_projects_root / "portfolio" / "portfolio" / ".qdrant_data",
                "description": "Portfolio Wiki Chunks, Neural Benchmarks Leaderboard & Antigravity Brain namespaces",
            },
            {
                "source_id": "neural_networks_code_index",
                "repo_name": "neural_networks",
                "path": self.idea_projects_root / "neural_networks" / ".qdrant_code_index",
                "description": "Neural Networks AST Code Corpus & Temporal Git Index namespaces",
            },
        ]
        discovered: List[Dict[str, Any]] = []
        for item in candidates:
            p: Path = item["path"]
            meta_file = p / "meta.json"
            if p.is_dir() and meta_file.is_file():
                try:
                    meta = json.loads(meta_file.read_text(encoding="utf-8"))
                    collections_dict = meta.get("collections", {})
                except Exception:
                    collections_dict = {}
                discovered.append(
                    {
                        "source_id": item["source_id"],
                        "repo_name": item["repo_name"],
                        "path": str(p),
                        "description": item["description"],
                        "collections": sorted(collections_dict.keys()),
                    }
                )
        return discovered

    def _load_meta_json(self, storage_path: Path) -> Dict[str, Any]:
        meta_path = storage_path / "meta.json"
        if meta_path.is_file():
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    data.setdefault("collections", {})
                    for ccfg in data["collections"].values():
                        if isinstance(ccfg, dict):
                            ccfg.pop("vacuum_optimizer_threshold", None)
                            ccfg.pop("init_from", None)
                    data.setdefault("aliases", {})
                    data.setdefault("text_indexes", {})
                    return data
            except Exception:
                pass
        return {"collections": {}, "aliases": {}, "text_indexes": {}}

    def _save_meta_json(self, storage_path: Path, meta: Dict[str, Any]) -> None:
        storage_path.mkdir(parents=True, exist_ok=True)
        meta_path = storage_path / "meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    @staticmethod
    def _default_collection_config(size: int = DEFAULT_VECTOR_SIZE) -> Dict[str, Any]:
        return {
            "vectors": {
                "size": size,
                "distance": "Cosine",
                "hnsw_config": None,
                "quantization_config": None,
                "on_disk": None,
                "datatype": None,
                "multivector_config": None,
            },
            "shard_number": None,
            "sharding_method": None,
            "replication_factor": None,
            "write_consistency_factor": None,
            "on_disk_payload": None,
            "hnsw_config": None,
            "wal_config": None,
            "optimizers_config": None,
            "quantization_config": None,
            "sparse_vectors": None,
            "strict_mode_config": None,
        }

    @staticmethod
    def _ensure_sqlite_table(sqlite_path: Path) -> None:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(sqlite_path), timeout=5.0) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS points (id TEXT PRIMARY KEY, point BLOB)")
            conn.commit()

    @staticmethod
    def _count_sqlite_points(sqlite_path: Path) -> int:
        if not sqlite_path.is_file():
            return 0
        try:
            with sqlite3.connect(str(sqlite_path), timeout=5.0) as conn:
                row = conn.execute("SELECT COUNT(*) FROM points").fetchone()
                return int(row[0]) if row else 0
        except Exception:
            return 0

    @staticmethod
    def _merge_sqlite_points(source_sqlite: Path, target_sqlite: Path) -> Dict[str, int]:
        """
        Lock-free read from `source_sqlite` and transactional upsert into
        `target_sqlite`, returning `{source_count, upserted_count, target_total}`.
        """
        if not source_sqlite.is_file():
            return {"source_count": 0, "upserted_count": 0, "target_total": 0}

        UnifiedQdrantStagingManager._ensure_sqlite_table(target_sqlite)
        try:
            with sqlite3.connect(f"file:{source_sqlite}?mode=ro", uri=True, timeout=5.0) as src_conn:
                rows = src_conn.execute("SELECT id, point FROM points").fetchall()
        except sqlite3.OperationalError:
            with sqlite3.connect(str(source_sqlite), timeout=5.0) as src_conn:
                rows = src_conn.execute("SELECT id, point FROM points").fetchall()

        with sqlite3.connect(str(target_sqlite), timeout=5.0) as tgt_conn:
            if rows:
                tgt_conn.executemany(
                    "INSERT OR REPLACE INTO points (id, point) VALUES (?, ?)",
                    rows,
                )
                tgt_conn.commit()
            total_row = tgt_conn.execute("SELECT COUNT(*) FROM points").fetchone()
            target_total = int(total_row[0]) if total_row else 0

        return {
            "source_count": len(rows),
            "upserted_count": len(rows),
            "target_total": target_total,
        }

    def upsert_points_lockfree(
        self,
        storage_path: Path,
        collection_name: str,
        points: List[PointStruct],
        vector_size: int = DEFAULT_VECTOR_SIZE,
        recreate: bool = False,
    ) -> int:
        """
        Upserts `PointStruct` records directly into `storage_path/collection/<collection_name>/storage.sqlite`
        and registers the collection in `storage_path/meta.json` without acquiring a portalocker lock.
        """
        meta = self._load_meta_json(storage_path)
        meta["collections"][collection_name] = self._default_collection_config(size=vector_size)
        self._save_meta_json(storage_path, meta)

        sqlite_path = storage_path / "collection" / collection_name / "storage.sqlite"
        self._ensure_sqlite_table(sqlite_path)

        serialized_rows = [
            (_encode_sqlite_point_id(str(pt.id)), pickle.dumps(pt))
            for pt in points
        ]
        with sqlite3.connect(str(sqlite_path), timeout=5.0) as conn:
            if recreate:
                conn.execute("DELETE FROM points")
            if serialized_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO points (id, point) VALUES (?, ?)",
                    serialized_rows,
                )
            conn.commit()
            total_row = conn.execute("SELECT COUNT(*) FROM points").fetchone()
            return int(total_row[0]) if total_row else 0

    def read_points_lockfree(
        self,
        storage_path: Path,
        collection_name: str,
        limit: Optional[int] = None,
    ) -> List[PointStruct]:
        """Reads `PointStruct` rows from a local Qdrant SQLite collection in read-only mode."""
        sqlite_path = storage_path / "collection" / collection_name / "storage.sqlite"
        if not sqlite_path.is_file():
            return []
        query = "SELECT point FROM points"
        if limit is not None and int(limit) > 0:
            query += f" LIMIT {int(limit)}"
        points: List[PointStruct] = []
        with sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True, timeout=5.0) as conn:
            for (blob,) in conn.execute(query):
                try:
                    pt = pickle.loads(blob)
                    if isinstance(pt, PointStruct):
                        if pt.payload:
                            pt.payload = mask_restricted_vocabulary(pt.payload)
                        points.append(pt)
                except Exception:
                    continue
        return points

    def unify_all_collections(self, export_snapshots: bool = True) -> Dict[str, Any]:
        """
        Unifies all Qdrant collections from `/Users/jarek/IdeaProjects/portfolio/portfolio/.qdrant_data`
        and `/Users/jarek/IdeaProjects/neural_networks/.qdrant_code_index` into the permanent
        staging directory `/Users/jarek/IdeaProjects/qdrant_staging/storage`.
        """
        t0 = time.perf_counter()
        sources = self.discover_source_qdrant_stores()
        staging_meta = self._load_meta_json(self.storage_dir)
        unified_summary: Dict[str, Dict[str, Any]] = {}

        for src in sources:
            src_path = Path(src["path"])
            src_meta = self._load_meta_json(src_path)
            for coll_name, coll_cfg in src_meta.get("collections", {}).items():
                staging_meta["collections"][coll_name] = coll_cfg
                src_sqlite = src_path / "collection" / coll_name / "storage.sqlite"
                dst_sqlite = self.storage_dir / "collection" / coll_name / "storage.sqlite"
                stats = self._merge_sqlite_points(src_sqlite, dst_sqlite)
                vec_cfg = coll_cfg.get("vectors", {}) if isinstance(coll_cfg, dict) else {}
                unified_summary[coll_name] = {
                    "collection_name": coll_name,
                    "source_repo": src["repo_name"],
                    "source_path": str(src_path),
                    "staging_sqlite_path": str(dst_sqlite),
                    "vector_size": vec_cfg.get("size", DEFAULT_VECTOR_SIZE),
                    "distance": vec_cfg.get("distance", "Cosine"),
                    "source_points": stats["source_count"],
                    "staged_points": stats["target_total"],
                }

        # Also include any existing collections already in staging (such as agent_informational_graph_index)
        for coll_name, coll_cfg in staging_meta.get("collections", {}).items():
            if coll_name not in unified_summary:
                dst_sqlite = self.storage_dir / "collection" / coll_name / "storage.sqlite"
                vec_cfg = coll_cfg.get("vectors", {}) if isinstance(coll_cfg, dict) else {}
                count = self._count_sqlite_points(dst_sqlite)
                unified_summary[coll_name] = {
                    "collection_name": coll_name,
                    "source_repo": "qdrant_staging",
                    "source_path": str(self.storage_dir),
                    "staging_sqlite_path": str(dst_sqlite),
                    "vector_size": vec_cfg.get("size", DEFAULT_VECTOR_SIZE),
                    "distance": vec_cfg.get("distance", "Cosine"),
                    "source_points": count,
                    "staged_points": count,
                }

        self._save_meta_json(self.storage_dir, staging_meta)

        if export_snapshots:
            self._write_snapshot_summary(unified_summary)

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        total_points = sum(int(v["staged_points"]) for v in unified_summary.values())
        manifest = {
            "staging_root": str(self.staging_root),
            "storage_dir": str(self.storage_dir),
            "snapshots_dir": str(self.snapshots_dir),
            "total_collections": len(unified_summary),
            "total_staged_points": total_points,
            "collections": unified_summary,
            "sources": sources,
            "placement_invariants": {
                "components_repo": {
                    "path": str(self.idea_projects_root / "components"),
                    "keep_in_place": True,
                    "rule": "Shared React/MUI components library remains in /Users/jarek/IdeaProjects/components",
                },
                "portfolio_ui_elements": {
                    "path": str(self.idea_projects_root / "portfolio" / "portfolio" / "react" / "src"),
                    "keep_in_place": True,
                    "rule": "All Portfolio UI components and sections remain in /Users/jarek/IdeaProjects/portfolio/portfolio/react/src",
                },
                "toolset_indexes": {
                    "path": str(self.idea_projects_root / "toolset" / "src" / "indexes"),
                    "keep_in_place": True,
                    "rule": "All toolset index classes remain in /Users/jarek/IdeaProjects/toolset/src/indexes",
                },
            },
            "last_unified_at": round(time.time(), 3),
            "unification_wall_time_ms": elapsed_ms,
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        return manifest

    def _write_snapshot_summary(self, unified_summary: Dict[str, Dict[str, Any]]) -> None:
        """Writes portable metadata and exports compact JSONL snapshot for `agent_informational_graph_index`."""
        bundle_meta = {
            "staging_root": str(self.staging_root),
            "exported_at": round(time.time(), 3),
            "collections": unified_summary,
        }
        (self.snapshots_dir / "staging_bundle_manifest.json").write_text(
            json.dumps(bundle_meta, indent=2) + "\n",
            encoding="utf-8",
        )
        if INFORMATIONAL_COLLECTION_NAME in unified_summary:
            pts = self.read_points_lockfree(self.storage_dir, INFORMATIONAL_COLLECTION_NAME)
            snap_file = self.snapshots_dir / f"{INFORMATIONAL_COLLECTION_NAME}.jsonl"
            lines = [
                json.dumps(
                    {
                        "id": str(pt.id),
                        "vector": list(pt.vector) if isinstance(pt.vector, list) else [],
                        "payload": pt.payload or {},
                    },
                    sort_keys=True,
                )
                for pt in pts
            ]
            snap_file.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def sync_with_environment(
        self,
        target_environment: Optional[str | Path] = None,
        direction: str = "bidirectional",
        collections: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Synchronizes the unified staging Qdrant store (`/Users/jarek/IdeaProjects/qdrant_staging/storage`)
        with another environment:
        - If `target_environment` is None: syncs between all repo-local Qdrant stores and `qdrant_staging`.
        - If `target_environment` is a HTTP/gRPC URL (`http://...` or `https://...`): syncs via `QdrantClient(url=...)`.
        - If `target_environment` is a filesystem path: creates/merges a full Qdrant storage directory
          at `target_environment` (`stage_to_target`, `target_to_stage`, or `bidirectional`).
        """
        t0 = time.perf_counter()
        normalized_dir = direction.lower().strip()
        if normalized_dir not in ("stage_to_target", "target_to_stage", "bidirectional"):
            raise ValueError(
                f"Invalid sync direction '{direction}'. Must be 'stage_to_target', 'target_to_stage', or 'bidirectional'."
            )

        # First ensure staging has unified latest collections from repo stores
        if normalized_dir in ("stage_to_target", "bidirectional") or target_environment is None:
            self.unify_all_collections(export_snapshots=True)

        staging_meta = self._load_meta_json(self.storage_dir)
        active_collections = (
            [c for c in collections if c in staging_meta["collections"]]
            if collections
            else sorted(staging_meta["collections"].keys())
        )

        # Case 1: Default repo-to-staging and staging-to-repo sync when no external target is given
        if target_environment is None:
            portfolio_qdrant = self.idea_projects_root / "portfolio" / "portfolio" / ".qdrant_data"
            synced_details: Dict[str, Any] = {}
            if portfolio_qdrant.is_dir() and INFORMATIONAL_COLLECTION_NAME in staging_meta["collections"]:
                p_meta = self._load_meta_json(portfolio_qdrant)
                p_meta["collections"][INFORMATIONAL_COLLECTION_NAME] = staging_meta["collections"][
                    INFORMATIONAL_COLLECTION_NAME
                ]
                self._save_meta_json(portfolio_qdrant, p_meta)
                src_sq = self.storage_dir / "collection" / INFORMATIONAL_COLLECTION_NAME / "storage.sqlite"
                dst_sq = portfolio_qdrant / "collection" / INFORMATIONAL_COLLECTION_NAME / "storage.sqlite"
                synced_details[INFORMATIONAL_COLLECTION_NAME] = self._merge_sqlite_points(src_sq, dst_sq)

            for coll_name in active_collections:
                sq = self.storage_dir / "collection" / coll_name / "storage.sqlite"
                synced_details[coll_name] = {
                    "staged_points": self._count_sqlite_points(sq),
                    "status": "synced",
                }
            return {
                "status": "synced_with_repo_environments",
                "direction": normalized_dir,
                "staging_root": str(self.staging_root),
                "target_environment": "repo_local_qdrant_stores",
                "collections_synced": synced_details,
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        target_str = str(target_environment).strip()

        # Case 2: Remote Qdrant HTTP/gRPC server URL
        if target_str.startswith("http://") or target_str.startswith("https://"):
            client = QdrantClient(url=target_str, timeout=10)
            remote_synced: Dict[str, Any] = {}
            try:
                existing_remote = {c.name for c in client.get_collections().collections}
                for coll_name in active_collections:
                    coll_cfg = staging_meta["collections"].get(coll_name, {})
                    vec_size = int(coll_cfg.get("vectors", {}).get("size", DEFAULT_VECTOR_SIZE))
                    if coll_name not in existing_remote:
                        client.create_collection(
                            collection_name=coll_name,
                            vectors_config=VectorParams(size=vec_size, distance=Distance.COSINE),
                        )
                    pushed = 0
                    pulled = 0
                    if normalized_dir in ("stage_to_target", "bidirectional"):
                        local_pts = self.read_points_lockfree(self.storage_dir, coll_name)
                        for i in range(0, len(local_pts), 256):
                            batch = local_pts[i : i + 256]
                            client.upsert(collection_name=coll_name, points=batch)
                        pushed = len(local_pts)
                    if normalized_dir in ("target_to_stage", "bidirectional"):
                        records, _ = client.scroll(
                            collection_name=coll_name,
                            limit=10000,
                            with_payload=True,
                            with_vectors=True,
                        )
                        remote_pts = [
                            PointStruct(
                                id=str(r.id),
                                vector=list(r.vector) if isinstance(r.vector, list) else [],
                                payload=r.payload or {},
                            )
                            for r in records
                        ]
                        if remote_pts:
                            self.upsert_points_lockfree(
                                self.storage_dir,
                                coll_name,
                                remote_pts,
                                vector_size=vec_size,
                            )
                        pulled = len(remote_pts)
                    remote_synced[coll_name] = {"pushed_points": pushed, "pulled_points": pulled}
            finally:
                client.close()

            return {
                "status": "synced_with_remote_qdrant",
                "direction": normalized_dir,
                "staging_root": str(self.staging_root),
                "target_environment": target_str,
                "collections_synced": remote_synced,
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        # Case 3: Local filesystem target environment directory
        target_path = Path(target_str).resolve()
        # If target_path has a `storage` child or is itself a Qdrant directory, handle gracefully
        target_storage = (
            target_path / "storage"
            if (target_path / "storage" / "meta.json").is_file()
            else target_path
        )
        target_storage.mkdir(parents=True, exist_ok=True)
        (target_storage / "collection").mkdir(parents=True, exist_ok=True)

        target_meta = self._load_meta_json(target_storage)
        all_candidate_colls = sorted(
            set(active_collections)
            | (set(target_meta.get("collections", {}).keys()) if not collections else set(collections))
        )

        synced_colls: Dict[str, Any] = {}
        for coll_name in all_candidate_colls:
            stage_sqlite = self.storage_dir / "collection" / coll_name / "storage.sqlite"
            tgt_sqlite = target_storage / "collection" / coll_name / "storage.sqlite"
            pushed_stats = {"source_count": 0, "upserted_count": 0, "target_total": 0}
            pulled_stats = {"source_count": 0, "upserted_count": 0, "target_total": 0}

            if normalized_dir in ("stage_to_target", "bidirectional") and coll_name in staging_meta["collections"]:
                target_meta["collections"][coll_name] = staging_meta["collections"][coll_name]
                pushed_stats = self._merge_sqlite_points(stage_sqlite, tgt_sqlite)

            if normalized_dir in ("target_to_stage", "bidirectional") and coll_name in target_meta["collections"]:
                staging_meta["collections"][coll_name] = target_meta["collections"][coll_name]
                pulled_stats = self._merge_sqlite_points(tgt_sqlite, stage_sqlite)
                if normalized_dir == "bidirectional" and pulled_stats["source_count"] > 0:
                    pushed_stats = self._merge_sqlite_points(stage_sqlite, tgt_sqlite)

            synced_colls[coll_name] = {
                "staged_points": self._count_sqlite_points(stage_sqlite),
                "target_points": self._count_sqlite_points(tgt_sqlite),
                "pushed_to_target": pushed_stats["upserted_count"],
                "pulled_to_stage": pulled_stats["upserted_count"],
            }

        self._save_meta_json(target_storage, target_meta)
        self._save_meta_json(self.storage_dir, staging_meta)

        return {
            "status": "synced_with_filesystem_environment",
            "direction": normalized_dir,
            "staging_root": str(self.staging_root),
            "target_environment": str(target_storage),
            "collections_synced": synced_colls,
            "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }


class AgentInformationalGraphIndex(Datasource):
    """
    Informational Graph Index (`indexes.informational_index.AgentInformationalGraphIndex`)
    backed by Qdrant collection `agent_informational_graph_index` in `/Users/jarek/IdeaProjects/qdrant_staging`.

    Juxtaposes code files, AST symbols, UI components (kept in place in `components` and `portfolio`),
    `toolset` index classes, promoted neural models, and Qdrant namespaces directly with their
    documentation (`SKILL.md`, Knowledge Items, `README.md`, docstrings) and typed graph edges
    so agents can navigate and map the entire workspace without getting lost.
    """

    def __init__(
        self,
        staging_root: str | Path = DEFAULT_STAGING_ROOT,
        idea_projects_root: str | Path = DEFAULT_IDEA_PROJECTS_ROOT,
        skill_md_path: str | Path = DEFAULT_SKILL_MD_PATH,
        knowledge_root: str | Path = DEFAULT_KNOWLEDGE_ROOT,
        collection_name: str = INFORMATIONAL_COLLECTION_NAME,
        auto_build: bool = True,
    ) -> None:
        self.staging_manager = UnifiedQdrantStagingManager(
            staging_root=staging_root,
            idea_projects_root=idea_projects_root,
        )
        self.staging_root = self.staging_manager.staging_root
        self.idea_projects_root = self.staging_manager.idea_projects_root
        self.skill_md_path = Path(skill_md_path).resolve()
        self.knowledge_root = Path(knowledge_root).resolve()
        self.collection_name = collection_name

        self.index = Index()
        self.index.new("nodes")
        self.index.new("edges_by_node")
        self.index.new("telemetry")

        self.audit_log = TsList()
        self.onceler = Onceler()
        self.stats = Stats()

        if auto_build:
            self.build_and_unify(force_rebuild=False)

    def _parse_skill_md_sections(self) -> Dict[str, Dict[str, str]]:
        """
        Parses `/Users/jarek/.gemini/config/skills/pytools-toolset/SKILL.md` into a lookup
        mapping module/package names to their manifest description and detailed markdown section.
        """
        if not self.skill_md_path.is_file():
            return {}
        raw = self.skill_md_path.read_text(encoding="utf-8", errors="replace")
        sections: Dict[str, Dict[str, str]] = {}

        # Parse markdown table rows in Decision Manifest
        for line in raw.splitlines():
            if line.strip().startswith("|") and "`" in line:
                parts = [p.strip() for p in line.strip().strip("|").split("|")]
                if len(parts) >= 4:
                    pkg_match = re.findall(r"`([a-zA-Z0-9_]+)`", parts[1])
                    for pkg in pkg_match:
                        sections.setdefault(pkg, {})
                        sections[pkg]["manifest_row"] = (
                            f"Domain: {parts[0]} | Package: {parts[1]} | "
                            f"Classes: {parts[2]} | Use When: {parts[3]}"
                        )
                        sections[pkg]["section_title"] = f"SKILL.md Decision Manifest ({pkg})"

        # Parse ### N. `pkg` headings
        heading_pattern = re.compile(r"^###\s+([^\n]+)$", re.MULTILINE)
        matches = list(heading_pattern.finditer(raw))
        for i, m in enumerate(matches):
            heading = m.group(1).strip()
            start = m.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
            body = raw[start:end].strip()
            for pkg in re.findall(r"`([a-zA-Z0-9_.]+)`", heading):
                root_pkg = pkg.split(".")[0]
                sections.setdefault(root_pkg, {})
                existing = sections[root_pkg].get("body", "")
                sections[root_pkg]["body"] = (existing + "\n\n" + body).strip()[:1200]
                sections[root_pkg]["section_title"] = heading

        return sections

    def _scan_knowledge_items(self) -> Dict[str, List[Dict[str, str]]]:
        """Scans `~/.gemini/antigravity-cli/knowledge` to map KI docs to packages and repos."""
        ki_by_keyword: Dict[str, List[Dict[str, str]]] = collections.defaultdict(list)
        if not self.knowledge_root.is_dir():
            return ki_by_keyword

        for ki_dir in sorted(self.knowledge_root.iterdir()):
            if not ki_dir.is_dir():
                continue
            masked_ki_id = mask_restricted_string(ki_dir.name)
            if any(existing["ki_id"] == masked_ki_id for existing in ki_by_keyword["all"]):
                continue
            meta_file = ki_dir / "metadata.json"
            title = masked_ki_id
            summary = ""
            if meta_file.is_file():
                try:
                    m = json.loads(meta_file.read_text(encoding="utf-8"))
                    title = mask_restricted_string(str(m.get("title") or masked_ki_id))
                    summary = mask_restricted_string(str(m.get("summary") or ""))
                except Exception:
                    pass
            artifacts_dir = ki_dir / "artifacts"
            first_md = None
            if artifacts_dir.is_dir():
                mds = sorted(artifacts_dir.rglob("*.md"))
                if mds:
                    first_md = mds[0]
            info = {
                "ki_id": masked_ki_id,
                "title": title,
                "summary": summary[:450],
                "doc_path": str(first_md or meta_file),
            }
            text_lower = f"{ki_dir.name} {title} {summary}".lower()
            ki_by_keyword["all"].append(info)
            for kw in (
                "qdrant",
                "portfolio",
                "components",
                "neural",
                "benchmark",
                "antigravity",
                "toolset",
                "indexes",
                "fsspecc",
                "thread_safe",
                "application_controller",
                "stage_compiler",
                "large",
                "sync",
                "mcp",
                "geodesic",
            ):
                if kw in text_lower:
                    ki_by_keyword[kw].append(info)
        return ki_by_keyword

    @staticmethod
    def _extract_python_symbols_and_doc(py_path: Path) -> Tuple[List[str], str, str, str]:
        """
        Extracts AST class/function names, module/class docstring, code excerpt, and SHA-256
        from a Python source file.
        """
        if not py_path.is_file():
            return [], "", "", ""
        raw_disk = py_path.read_text(encoding="utf-8", errors="replace")
        sha = hashlib.sha256(raw_disk.encode("utf-8")).hexdigest()
        raw = mask_restricted_string(raw_disk)
        symbols: List[str] = []
        docstrings: List[str] = []
        try:
            tree = ast.parse(raw)
            mod_doc = ast.get_docstring(tree)
            if mod_doc:
                docstrings.append(mod_doc.strip())
            for node in tree.body:
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    symbols.append(mask_restricted_string(node.name))
                    ndoc = ast.get_docstring(node)
                    if ndoc and len(docstrings) < 3:
                        docstrings.append(f"{node.name}: {ndoc.strip()}")
        except SyntaxError:
            pass
        excerpt_lines = [ln for ln in raw.splitlines() if ln.strip()][:28]
        return (
            symbols[:25],
            mask_restricted_string("\n".join(docstrings)[:700]),
            mask_restricted_string("\n".join(excerpt_lines)[:900]),
            sha,
        )

    @staticmethod
    def _extract_tsx_symbols_and_excerpt(tsx_path: Path) -> Tuple[List[str], str, str, str]:
        """
        Extracts exported TypeScript/React component & interface names, leading JSDoc comments,
        code excerpt, and SHA-256 from a `.tsx`/`.ts` file.
        """
        if not tsx_path.is_file():
            return [], "", "", ""
        raw_disk = tsx_path.read_text(encoding="utf-8", errors="replace")
        sha = hashlib.sha256(raw_disk.encode("utf-8")).hexdigest()
        raw = mask_restricted_string(raw_disk)
        symbols = re.findall(
            r"export\s+(?:default\s+)?(?:function|const|class|interface|type)\s+([A-Za-z0-9_]+)",
            raw,
        )
        jsdoc_matches = re.findall(r"/\*\*([\s\S]*?)\*/", raw)
        doc_comment = ""
        if jsdoc_matches:
            cleaned = [
                re.sub(r"^\s*\*\s?", "", ln).strip()
                for ln in jsdoc_matches[0].splitlines()
            ]
            doc_comment = " ".join(x for x in cleaned if x)[:600]
        excerpt_lines = [ln for ln in raw.splitlines() if ln.strip()][:28]
        return (
            sorted(set(symbols))[:25],
            mask_restricted_string(doc_comment),
            mask_restricted_string("\n".join(excerpt_lines)[:900]),
            sha,
        )

    @staticmethod
    def _extract_sig_or_md_symbols_and_doc(file_path: Path) -> Tuple[List[str], str, str, str]:
        """
        Extracts exported functions/structs/constants or Markdown headings, header doc comments,
        excerpt, and SHA-256 from `.sig` (StageCompiler) or `.md` (Design Spec) files.
        """
        if not file_path.is_file():
            return [], "", "", ""
        raw_disk = file_path.read_text(encoding="utf-8", errors="replace")
        sha = hashlib.sha256(raw_disk.encode("utf-8")).hexdigest()
        raw = mask_restricted_string(raw_disk)
        suffix = file_path.suffix.lower()
        symbols: List[str] = []
        doc_lines: List[str] = []
        if suffix == ".sig":
            for m in re.finditer(r"^\s*(?:func|struct|const)\s+([A-Za-z0-9_]+)", raw, re.MULTILINE):
                sym = m.group(1)
                if sym not in symbols:
                    symbols.append(sym)
            for ln in raw.splitlines()[:30]:
                stripped = ln.strip()
                if stripped.startswith("#"):
                    cleaned = stripped.lstrip("#").strip()
                    if cleaned and not cleaned.startswith("==="):
                        doc_lines.append(cleaned)
        else:
            for m in re.finditer(r"^#{1,3}\s+([^\n]+)", raw, re.MULTILINE):
                heading = re.sub(r"[`*_]", "", m.group(1)).strip()
                if heading and heading not in symbols:
                    symbols.append(heading[:60])
            for ln in raw.splitlines()[:25]:
                if ln.strip():
                    doc_lines.append(ln.strip())
        excerpt_lines = [ln for ln in raw.splitlines() if ln.strip()][:28]
        return (
            symbols[:25],
            mask_restricted_string("\n".join(doc_lines)[:750]),
            mask_restricted_string("\n".join(excerpt_lines)[:900]),
            sha,
        )

    def _construct_juxtaposed_graph(
        self,
        unified_manifest: Dict[str, Any],
    ) -> Tuple[List[JuxtaposedNode], List[JuxtaposedGraphEdge]]:
        """
        Constructs the complete set of `JuxtaposedNode`s and `JuxtaposedGraphEdge`s across:
        0. Unified Qdrant Staging Root (`qdrant_staging`)
        1. Sibling Repositories (`components`, `portfolio`, `toolset`, `neural_networks`, `large_tools`, `StageCompiler-development`)
        2. Unified Qdrant Namespaces (`portfolio_wiki_chunks`, `portfolio_neural_benchmarks`,
           `portfolio_antigravity_brain`, `neural_networks_code_corpus`, `code_corpus_temporal_index`,
           `agent_informational_graph_index`)
        3. Shared UI Elements in `/Users/jarek/IdeaProjects/components` (`keep_in_place=True`)
        4. Portfolio UI Elements in `/Users/jarek/IdeaProjects/portfolio/portfolio/react/src` (`keep_in_place=True`)
        5. All `toolset` Index Classes in `/Users/jarek/IdeaProjects/toolset/src/indexes` (`keep_in_place=True`)
        6. All 37 `toolset` Library Packages juxtaposed with `SKILL.md`
        7. Portfolio Backend Services & API Router + Neural Networks Promoted Gauntlet Model
        """
        skill_sections = self._parse_skill_md_sections()
        ki_lookup = self._scan_knowledge_items()
        nodes_by_id: Dict[str, JuxtaposedNode] = {}
        edges: List[JuxtaposedGraphEdge] = []

        def add_node(node: JuxtaposedNode) -> None:
            if not node.poincare_coords:
                node.poincare_coords = _compute_poincare_ball_coords(
                    node.node_id, node.hierarchy_level, node.node_type
                )
            nodes_by_id[node.node_id] = node

        def add_edge(src: str, dst: str, rel: str, weight: float = 1.0, rationale: str = "") -> None:
            edges.append(
                JuxtaposedGraphEdge(
                    source=src,
                    target=dst,
                    relation=rel,
                    weight=weight,
                    rationale=rationale,
                )
            )

        # --- 0. Root Unified Qdrant Staging Anchor ---
        staging_node_id = "staging:qdrant_staging"
        add_node(
            JuxtaposedNode(
                node_id=staging_node_id,
                node_type="qdrant_staging_root",
                title="Unified Permanent Qdrant Staging Directory",
                idiomatic_name="qdrant_staging",
                repo_name="qdrant_staging",
                keep_in_place=True,
                placement_directive=(
                    "PERMANENT_STAGING_ROOT: Lives at /Users/jarek/IdeaProjects/qdrant_staging "
                    "adjacent to sibling repositories; unifies all Qdrant namespaces and syncs across environments."
                ),
                juxtaposed_code={
                    "code_path": str(
                        self.idea_projects_root
                        / "toolset"
                        / "src"
                        / "indexes"
                        / "informational_index"
                        / "informational_index.py"
                    ),
                    "relative_path": "toolset/src/indexes/informational_index/informational_index.py",
                    "language": "python",
                    "exported_symbols": [
                        "UnifiedQdrantStagingManager",
                        "AgentInformationalGraphIndex",
                        "JuxtaposedNode",
                        "JuxtaposedGraphEdge",
                    ],
                    "code_excerpt": (
                        "DEFAULT_STAGING_ROOT = Path('/Users/jarek/IdeaProjects/qdrant_staging')\n"
                        "class UnifiedQdrantStagingManager:\n"
                        "    def unify_all_collections(self) -> Dict[str, Any]: ...\n"
                        "    def sync_with_environment(self, target_environment, direction='bidirectional'): ..."
                    ),
                    "sha256": hashlib.sha256(str(self.staging_root).encode("utf-8")).hexdigest(),
                },
                juxtaposed_docs={
                    "doc_path": str(self.staging_manager.manifest_path),
                    "doc_type": "staging_manifest",
                    "doc_section": "Unified Qdrant Staging & Cross-Environment Sync Architecture",
                    "doc_excerpt": (
                        f"Unified Qdrant staging directory containing {unified_manifest.get('total_collections', 6)} "
                        f"collections and {unified_manifest.get('total_staged_points', 0)} staged points across "
                        "portfolio, neural_networks, and agent_informational_graph_index."
                    ),
                    "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("qdrant", [])[:4]],
                },
                qdrant_namespace=INFORMATIONAL_COLLECTION_NAME,
                staging_path=str(self.staging_manager.storage_dir),
                hierarchy_level=0,
                tags=["qdrant", "staging", "sync", "unified", "wayfinding", "root"],
            )
        )

        # --- 1. Sibling Repositories in /Users/jarek/IdeaProjects ---
        repo_specs = [
            (
                "components",
                "Shared React 19 & MUI v6 Meta-Components Library",
                "INVARIANT: Keep /Users/jarek/IdeaProjects/components in place; consumed by portfolio frontend.",
                "components/src/index.ts",
                "components/package.json",
                "portfolio_wiki_chunks",
            ),
            (
                "portfolio",
                "Sovereign Systems Architect Portfolio, Dynamic Wiki & Telemetry Workbench",
                "INVARIANT: Keep /Users/jarek/IdeaProjects/portfolio/portfolio in place; hosts FastAPI + React UI.",
                "portfolio/portfolio/python/main.py",
                "portfolio/portfolio/settings.yaml",
                "portfolio_wiki_chunks",
            ),
            (
                "toolset",
                "37-Library Python Infrastructure & Thread-Safe Indexes Collection",
                "INVARIANT: Keep /Users/jarek/IdeaProjects/toolset in place; authoritative source for all 37 packages.",
                "toolset/pyproject.toml",
                str(self.skill_md_path),
                "portfolio_wiki_chunks",
            ),
            (
                "neural_networks",
                "Cohesive-Manifold & Quantum-Inspired Neural Architectures + Promoted Gauntlet Model",
                "INVARIANT: Keep /Users/jarek/IdeaProjects/neural_networks in place; promoted model in promoted/.",
                "neural_networks/promoted/promoted_model.py",
                "neural_networks/README.md",
                "neural_networks_code_corpus",
            ),
            (
                "large_tools",
                "Large-Scale Auxiliary Engineering & Pipeline Utilities",
                "INVARIANT: Sibling repository at /Users/jarek/IdeaProjects/large_tools.",
                "large_tools",
                str(self.skill_md_path),
                INFORMATIONAL_COLLECTION_NAME,
            ),
            (
                "StageCompiler-development",
                "StageCompiler Compiler & Symbolic Phase Language Development Repository",
                "INVARIANT: Sibling repository at /Users/jarek/IdeaProjects/StageCompiler-development.",
                "StageCompiler-development/README.md",
                "StageCompiler-development/README.md",
                INFORMATIONAL_COLLECTION_NAME,
            ),
        ]

        for repo_name, repo_title, directive, rel_code, doc_candidate, default_ns in repo_specs:
            repo_dir = self.idea_projects_root / repo_name
            if not repo_dir.exists():
                continue
            r_node_id = f"repo:{repo_name}"
            code_p = (
                self.idea_projects_root / rel_code
                if not str(rel_code).startswith("/")
                else Path(rel_code)
            )
            doc_p = (
                self.idea_projects_root / doc_candidate
                if not str(doc_candidate).startswith("/")
                else Path(doc_candidate)
            )
            doc_excerpt = ""
            if doc_p.is_file():
                doc_excerpt = doc_p.read_text(encoding="utf-8", errors="replace")[:600]
            else:
                doc_excerpt = f"Repository root at {repo_dir}. {directive}"

            add_node(
                JuxtaposedNode(
                    node_id=r_node_id,
                    node_type="repository",
                    title=repo_title,
                    idiomatic_name=repo_name,
                    repo_name=repo_name,
                    keep_in_place=True,
                    placement_directive=directive,
                    juxtaposed_code={
                        "code_path": str(code_p if code_p.exists() else repo_dir),
                        "relative_path": str(rel_code),
                        "language": "directory",
                        "exported_symbols": [repo_name],
                        "code_excerpt": f"Repository Path: {repo_dir}",
                        "sha256": hashlib.sha256(str(repo_dir).encode("utf-8")).hexdigest(),
                    },
                    juxtaposed_docs={
                        "doc_path": str(doc_p if doc_p.exists() else self.skill_md_path),
                        "doc_type": "readme_md",
                        "doc_section": f"Repository Overview: {repo_name}",
                        "doc_excerpt": doc_excerpt,
                        "related_ki_ids": [
                            x["ki_id"] for x in ki_lookup.get(repo_name.split("_")[0].lower(), [])[:4]
                        ],
                    },
                    qdrant_namespace=default_ns,
                    staging_path=str(self.staging_manager.storage_dir / "collection" / default_ns),
                    hierarchy_level=1,
                    tags=["repository", repo_name, "keep_in_place"],
                )
            )
            add_edge(
                r_node_id,
                staging_node_id,
                "UNIFIED_INTO_STAGING",
                weight=1.0,
                rationale=f"Repository {repo_name} indexes are unified in {self.staging_root}",
            )

        # --- 2. Unified Qdrant Namespaces (Collections) ---
        namespace_docs = {
            "portfolio_wiki_chunks": (
                "portfolio",
                "Portfolio Dynamic Skill Wiki & Knowledge Item Chunks Namespace",
                "portfolio/portfolio/python/portfoliolib/service/qdrant_graph_service.py",
                "Stores lossless overlapping 384-D vector chunks of SKILL.md, 37 toolset modules, and Knowledge Items.",
            ),
            "portfolio_neural_benchmarks": (
                "portfolio",
                "Neural Benchmarks Gauntlet Leaderboard & Promoted Model Namespace",
                "toolset/src/benchmark_ledger/ledger.py",
                "Stores all 25 gauntlet neural architecture models (with PolyphonicStageCompilerCascadeModel promoted at Rank #1 alongside CohesiveUnifiedMonadicCavityModel) and 12 benchmark tasks.",
            ),
            "portfolio_antigravity_brain": (
                "portfolio",
                "Antigravity Agent Brain Conversations & Artifacts Namespace",
                "portfolio/portfolio/python/portfoliolib/service/antigravity_brain_service.py",
                "Stores periodically indexed agent transcripts, user/agent turns, and markdown/mermaid artifacts via FSBase.",
            ),
            "neural_networks_code_corpus": (
                "neural_networks",
                "Neural Networks AST & Markdown Code Corpus Namespace",
                "toolset/src/indexes/code_index/code_index.py",
                "Stores 5,330+ AST-chunked Python modules, classes, functions, and tests from /Users/jarek/IdeaProjects/neural_networks.",
            ),
            "code_corpus_temporal_index": (
                "neural_networks",
                "Neural Networks Temporal Git History Code Index Namespace",
                "toolset/src/indexes/code_index/code_index.py",
                "Temporal git commit & evolution namespace associated with CodeCorpusIndex.",
            ),
            INFORMATIONAL_COLLECTION_NAME: (
                "qdrant_staging",
                "Agent Juxtaposed Code + Documentation Informational Graph Namespace",
                "toolset/src/indexes/informational_index/informational_index.py",
                "Maps and juxtaposes every repo, UI element, toolset index, module, and Qdrant namespace with its documentation and graph edges.",
            ),
        }

        collections_manifest = unified_manifest.get("collections", {})
        for ns_name, (ns_repo, ns_title, ns_code_rel, ns_desc) in namespace_docs.items():
            ns_node_id = f"qdrant_ns:{ns_name}"
            coll_info = collections_manifest.get(ns_name, {})
            staged_sqlite = self.staging_manager.storage_dir / "collection" / ns_name / "storage.sqlite"
            code_file = self.idea_projects_root / ns_code_rel
            syms, doc_str, excerpt, sha = self._extract_python_symbols_and_doc(code_file)
            add_node(
                JuxtaposedNode(
                    node_id=ns_node_id,
                    node_type="qdrant_namespace",
                    title=ns_title,
                    idiomatic_name=ns_name,
                    repo_name=ns_repo,
                    keep_in_place=True,
                    placement_directive=(
                        f"UNIFIED_STAGING_NAMESPACE: Source repo '{ns_repo}' collection '{ns_name}' "
                        f"is permanently unified at {staged_sqlite} and syncable across environments."
                    ),
                    juxtaposed_code={
                        "code_path": str(code_file),
                        "relative_path": ns_code_rel,
                        "language": "python",
                        "exported_symbols": syms[:10],
                        "code_excerpt": excerpt[:650],
                        "sha256": sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(self.staging_manager.manifest_path),
                        "doc_type": "staging_manifest",
                        "doc_section": f"Qdrant Namespace: {ns_name}",
                        "doc_excerpt": (
                            f"{ns_desc} Staged points: {coll_info.get('staged_points', 'active')}. "
                            f"Vector dimension: {coll_info.get('vector_size', DEFAULT_VECTOR_SIZE)}."
                        ),
                        "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("qdrant", [])[:3]],
                    },
                    qdrant_namespace=ns_name,
                    staging_path=str(staged_sqlite),
                    hierarchy_level=1,
                    tags=["qdrant_namespace", ns_name, ns_repo, "staging"],
                )
            )
            add_edge(
                ns_node_id,
                staging_node_id,
                "UNIFIED_INTO_STAGING",
                weight=1.0,
                rationale=f"Collection {ns_name} is stored in unified staging directory {self.staging_manager.storage_dir}",
            )
            if f"repo:{ns_repo}" in nodes_by_id:
                add_edge(
                    ns_node_id,
                    f"repo:{ns_repo}",
                    "RESIDES_IN_REPO",
                    weight=0.95,
                    rationale=f"Namespace {ns_name} originates from repository {ns_repo}",
                )

        # --- 3. Shared UI Elements in /Users/jarek/IdeaProjects/components (MUST STAY IN PLACE) ---
        components_meta_dir = self.idea_projects_root / "components" / "src" / "meta_components"
        if components_meta_dir.is_dir():
            for tsx_file in sorted(components_meta_dir.rglob("*.tsx")):
                if tsx_file.name.endswith(".test.tsx"):
                    continue
                syms, jsdoc, excerpt, sha = self._extract_tsx_symbols_and_excerpt(tsx_file)
                pascal_stem = "".join(part.capitalize() for part in tsx_file.stem.split("_"))
                comp_name = (
                    pascal_stem
                    if pascal_stem in syms
                    else (
                        next((s for s in syms if not s.endswith("Props") and not s.endswith("Item")), None)
                        or pascal_stem
                    )
                )
                node_id = f"ui:components:{comp_name}"
                rel_p = str(tsx_file.relative_to(self.idea_projects_root))
                add_node(
                    JuxtaposedNode(
                        node_id=node_id,
                        node_type="ui_component",
                        title=f"Shared UI Component: {comp_name}",
                        idiomatic_name=comp_name,
                        repo_name="components",
                        keep_in_place=True,
                        placement_directive=(
                            f"INVARIANT: Keep shared UI component in place at {tsx_file} "
                            "(/Users/jarek/IdeaProjects/components remains where it is)."
                        ),
                        juxtaposed_code={
                            "code_path": str(tsx_file),
                            "relative_path": rel_p,
                            "language": "tsx",
                            "exported_symbols": syms or [comp_name],
                            "code_excerpt": excerpt,
                            "sha256": sha,
                        },
                        juxtaposed_docs={
                            "doc_path": str(self.idea_projects_root / "components" / "src" / "index.ts"),
                            "doc_type": "docstring",
                            "doc_section": f"@jarek/components -> {comp_name}",
                            "doc_excerpt": (
                                jsdoc
                                or f"Reusable React 19 + MUI v6 meta-component `{comp_name}` exported from "
                                "`/Users/jarek/IdeaProjects/components/src/index.ts` and consumed by `portfolio`."
                            ),
                            "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("components", [])[:3]],
                        },
                        qdrant_namespace=INFORMATIONAL_COLLECTION_NAME,
                        staging_path=str(self.staging_manager.storage_dir),
                        hierarchy_level=3,
                        tags=["ui_component", "components", "keep_in_place", comp_name.lower()],
                    )
                )
                add_edge(
                    node_id,
                    "repo:components",
                    "RESIDES_IN_REPO",
                    weight=1.0,
                    rationale=f"{comp_name} is permanently resident in /Users/jarek/IdeaProjects/components",
                )

        # --- 4. Portfolio UI Elements in /Users/jarek/IdeaProjects/portfolio/portfolio/react/src (MUST STAY IN PLACE) ---
        portfolio_react_src = self.idea_projects_root / "portfolio" / "portfolio" / "react" / "src"
        portfolio_ui_files = list(sorted((portfolio_react_src / "components").glob("*.tsx")))
        if (portfolio_react_src / "App.tsx").is_file():
            portfolio_ui_files.insert(0, portfolio_react_src / "App.tsx")

        ui_to_namespace_map = {
            "BenchmarksSection": "portfolio_neural_benchmarks",
            "AntigravityBrainSection": "portfolio_antigravity_brain",
            "WikiArchitectureSection": "portfolio_wiki_chunks",
            "QdrantGraphSection": "portfolio_wiki_chunks",
            "ToolsetGridSection": "portfolio_wiki_chunks",
            "SourceInspectorDrawer": "portfolio_wiki_chunks",
            "HeroHeader": "portfolio_wiki_chunks",
            "App": "portfolio_wiki_chunks",
        }

        for tsx_file in portfolio_ui_files:
            comp_name = tsx_file.stem
            node_id = f"ui:portfolio:{comp_name}"
            syms, jsdoc, excerpt, sha = self._extract_tsx_symbols_and_excerpt(tsx_file)
            rel_p = str(tsx_file.relative_to(self.idea_projects_root))
            raw_tsx = tsx_file.read_text(encoding="utf-8", errors="replace")
            backing_ns = ui_to_namespace_map.get(comp_name, "portfolio_wiki_chunks")

            ui_doc_notes = {
                "BenchmarksSection": (
                    "Renders the Neural Benchmarks Leaderboard with the promoted gauntlet model "
                    "(`PolyphonicStageCompilerCascadeModel` / `Cohesive-Manifold Unified Monadic Cavity`) "
                    "pinned at Rank #1 in a compact quarter-inch (0.26in) pseudo-column row layout "
                    "with metrics (Accuracy, Best Val Loss, Latency, Speedup, Params) and zero badges."
                ),
                "AntigravityBrainSection": (
                    "Renders the Antigravity Brain live conversation tabs using the shared `AsyncChatWindow` "
                    "single-card component with clean timestamps, collapsible agent thoughts, bottom-sticky "
                    "auto-refresh pause on scroll-up, and artifact modals."
                ),
            }.get(comp_name, jsdoc or f"Portfolio React UI view component `{comp_name}` in `{rel_p}`.")

            add_node(
                JuxtaposedNode(
                    node_id=node_id,
                    node_type="ui_component",
                    title=f"Portfolio UI Element: {comp_name}",
                    idiomatic_name=comp_name,
                    repo_name="portfolio",
                    keep_in_place=True,
                    placement_directive=(
                        f"INVARIANT: Keep Portfolio UI element in place at {tsx_file} — do not relocate."
                    ),
                    juxtaposed_code={
                        "code_path": str(tsx_file),
                        "relative_path": rel_p,
                        "language": "tsx",
                        "exported_symbols": syms or [comp_name],
                        "code_excerpt": excerpt,
                        "sha256": sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(
                            self.idea_projects_root / "portfolio" / "portfolio" / "settings.yaml"
                        ),
                        "doc_type": "settings_yaml",
                        "doc_section": f"Portfolio UI Component: {comp_name}",
                        "doc_excerpt": ui_doc_notes,
                        "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("portfolio", [])[:3]],
                    },
                    qdrant_namespace=backing_ns,
                    staging_path=str(
                        self.staging_manager.storage_dir / "collection" / backing_ns / "storage.sqlite"
                    ),
                    hierarchy_level=3,
                    tags=["ui_component", "portfolio", "keep_in_place", comp_name.lower()],
                )
            )
            add_edge(
                node_id,
                "repo:portfolio",
                "RESIDES_IN_REPO",
                weight=1.0,
                rationale=f"UI element {comp_name} remains in place inside portfolio/react/src",
            )
            add_edge(
                node_id,
                f"qdrant_ns:{backing_ns}",
                "INDEXED_IN_NAMESPACE",
                weight=0.95,
                rationale=f"UI element {comp_name} visualizes data from Qdrant namespace {backing_ns}",
            )
            # Link any imported shared components from `/Users/jarek/IdeaProjects/components`
            for shared_node_id, shared_node in list(nodes_by_id.items()):
                if shared_node.repo_name == "components" and shared_node.idiomatic_name in raw_tsx:
                    add_edge(
                        node_id,
                        shared_node_id,
                        "CONSUMES_UI_PACKAGE",
                        weight=0.95,
                        rationale=f"Portfolio {comp_name} imports shared component {shared_node.idiomatic_name} from /Users/jarek/IdeaProjects/components",
                    )

        # --- 5. All Toolset Index Classes in /Users/jarek/IdeaProjects/toolset/src/indexes (KEEP IN PLACE) ---
        indexes_dir = self.idea_projects_root / "toolset" / "src" / "indexes"
        indexes_skill_doc = skill_sections.get("indexes", {})
        if indexes_dir.is_dir():
            for sub_dir in sorted(indexes_dir.iterdir()):
                if not sub_dir.is_dir() or sub_dir.name.startswith("__"):
                    continue
                py_files = [
                    p
                    for p in sorted(sub_dir.glob("*.py"))
                    if p.name != "__init__.py" and not p.name.startswith("test_")
                ]
                if not py_files:
                    continue
                py_file = py_files[0]
                syms, doc_str, excerpt, sha = self._extract_python_symbols_and_doc(py_file)
                primary_cls = syms[0] if syms else sub_dir.name
                idx_node_id = f"index:toolset:{sub_dir.name}"
                rel_p = str(py_file.relative_to(self.idea_projects_root))

                backing_ns = (
                    "neural_networks_code_corpus"
                    if sub_dir.name == "code_index"
                    else INFORMATIONAL_COLLECTION_NAME
                    if sub_dir.name == "informational_index"
                    else "portfolio_wiki_chunks"
                )
                add_node(
                    JuxtaposedNode(
                        node_id=idx_node_id,
                        node_type="toolset_index",
                        title=f"Toolset Index: {primary_cls} (indexes.{sub_dir.name})",
                        idiomatic_name=primary_cls,
                        repo_name="toolset",
                        keep_in_place=True,
                        placement_directive=(
                            f"INVARIANT: Keep index implementation in place at {py_file} "
                            "and unify its Qdrant backing store via /Users/jarek/IdeaProjects/qdrant_staging."
                        ),
                        juxtaposed_code={
                            "code_path": str(py_file),
                            "relative_path": rel_p,
                            "language": "python",
                            "exported_symbols": syms,
                            "code_excerpt": excerpt,
                            "sha256": sha,
                        },
                        juxtaposed_docs={
                            "doc_path": str(self.skill_md_path),
                            "doc_type": "skill_md",
                            "doc_section": indexes_skill_doc.get(
                                "section_title", f"SKILL.md -> indexes.{sub_dir.name}"
                            ),
                            "doc_excerpt": (
                                f"{doc_str}\n\n{indexes_skill_doc.get('manifest_row', '')}\n"
                                f"{indexes_skill_doc.get('body', '')}"
                            ).strip()[:900],
                            "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("indexes", [])[:4]],
                        },
                        qdrant_namespace=backing_ns,
                        staging_path=str(
                            self.staging_manager.storage_dir / "collection" / backing_ns / "storage.sqlite"
                        ),
                        hierarchy_level=2,
                        tags=["toolset_index", "indexes", sub_dir.name, primary_cls.lower(), "keep_in_place"],
                    )
                )
                add_edge(
                    idx_node_id,
                    "repo:toolset",
                    "RESIDES_IN_REPO",
                    weight=1.0,
                    rationale=f"Index {primary_cls} resides in toolset/src/indexes/{sub_dir.name}",
                )
                add_edge(
                    idx_node_id,
                    f"qdrant_ns:{backing_ns}",
                    "INDEXED_IN_NAMESPACE",
                    weight=0.95,
                    rationale=f"Index {primary_cls} is indexed/backed by Qdrant namespace {backing_ns}",
                )
                add_edge(
                    idx_node_id,
                    staging_node_id,
                    "JUXTAPOSED_WITH_DOC",
                    weight=0.9,
                    rationale=f"Juxtaposes {rel_p} with {self.skill_md_path.name}",
                )

        # --- 6. All Top-Level Toolset Packages Juxtaposed with SKILL.md ---
        toolset_src = self.idea_projects_root / "toolset" / "src"
        if toolset_src.is_dir():
            for pkg_dir in sorted(toolset_src.iterdir()):
                if (
                    not pkg_dir.is_dir()
                    or pkg_dir.name.startswith(".")
                    or pkg_dir.name.startswith("__")
                    or pkg_dir.name.endswith(".egg-info")
                ):
                    continue
                pkg_name = pkg_dir.name
                py_candidates = [
                    p
                    for p in sorted(pkg_dir.rglob("*.py"))
                    if p.name != "__init__.py" and not p.name.startswith("test_")
                ]
                if not py_candidates:
                    py_candidates = list(sorted(pkg_dir.rglob("*.py")))
                rep_file = py_candidates[0] if py_candidates else pkg_dir
                syms, doc_str, excerpt, sha = (
                    self._extract_python_symbols_and_doc(rep_file)
                    if rep_file.is_file()
                    else ([], "", "", "")
                )
                skill_info = skill_sections.get(pkg_name, {})
                mod_node_id = f"module:toolset:{pkg_name}"
                rel_p = str(rep_file.relative_to(self.idea_projects_root))
                backing_ns = (
                    "portfolio_neural_benchmarks"
                    if pkg_name == "benchmark_ledger"
                    else "portfolio_wiki_chunks"
                )
                add_node(
                    JuxtaposedNode(
                        node_id=mod_node_id,
                        node_type="toolset_module",
                        title=f"Toolset Package: {pkg_name}",
                        idiomatic_name=pkg_name,
                        repo_name="toolset",
                        keep_in_place=True,
                        placement_directive=(
                            f"INVARIANT: Canonical toolset module at {pkg_dir} — keep in place."
                        ),
                        juxtaposed_code={
                            "code_path": str(rep_file),
                            "relative_path": rel_p,
                            "language": "python",
                            "exported_symbols": syms or [pkg_name],
                            "code_excerpt": excerpt,
                            "sha256": sha,
                        },
                        juxtaposed_docs={
                            "doc_path": str(self.skill_md_path),
                            "doc_type": "skill_md",
                            "doc_section": skill_info.get(
                                "section_title", f"SKILL.md -> {pkg_name}"
                            ),
                            "doc_excerpt": (
                                f"{skill_info.get('manifest_row', '')}\n\n"
                                f"{skill_info.get('body', doc_str or f'Toolset package {pkg_name}')}"
                            ).strip()[:900],
                            "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("toolset", [])[:3]],
                        },
                        qdrant_namespace=backing_ns,
                        staging_path=str(
                            self.staging_manager.storage_dir / "collection" / backing_ns / "storage.sqlite"
                        ),
                        hierarchy_level=2,
                        tags=["toolset_module", pkg_name, "skill_md", "keep_in_place"],
                    )
                )
                add_edge(
                    mod_node_id,
                    "repo:toolset",
                    "RESIDES_IN_REPO",
                    weight=1.0,
                    rationale=f"Package {pkg_name} is a canonical package in toolset/src",
                )
                add_edge(
                    mod_node_id,
                    f"qdrant_ns:{backing_ns}",
                    "INDEXED_IN_NAMESPACE",
                    weight=0.9,
                    rationale=f"Package {pkg_name} documentation and AST chunks are indexed in {backing_ns}",
                )

        # --- 7. Promoted Neural Gauntlet Model & Separated Architecture Packages (Cross-Repo Wayfinding) ---
        promoted_model_path = (
            self.idea_projects_root / "neural_networks" / "promoted" / "promoted_model.py"
        )
        promoted_manifest_path = (
            self.idea_projects_root / "neural_networks" / "promoted" / "promoted_manifest.json"
        )
        promoted_harness_path = (
            self.idea_projects_root / "neural_networks" / "promoted" / "harness.py"
        )
        if promoted_model_path.is_file():
            prom_manifest: Dict[str, Any] = {}
            if promoted_manifest_path.is_file():
                try:
                    prom_manifest = json.loads(promoted_manifest_path.read_text(encoding="utf-8"))
                except Exception:
                    prom_manifest = {}
            prom_cls = str(
                prom_manifest.get("class_name") or "PolyphonicStageCompilerCascadeModel"
            )
            prom_title = str(
                prom_manifest.get("model_name")
                or "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)"
            )
            syms, doc_str, excerpt, sha = self._extract_python_symbols_and_doc(promoted_model_path)
            h_syms, h_doc, _h_excerpt, _ = self._extract_python_symbols_and_doc(promoted_harness_path)
            prom_node_id = f"model:neural_networks:{prom_cls}"
            add_node(
                JuxtaposedNode(
                    node_id=prom_node_id,
                    node_type="promoted_model",
                    title=f"{prom_title} (Promoted #1 Gauntlet Model)",
                    idiomatic_name=prom_cls,
                    repo_name="neural_networks",
                    keep_in_place=True,
                    placement_directive=(
                        f"INVARIANT: Keep promoted gauntlet model files in {promoted_model_path.parent} "
                        "and pin at Rank #1 in portfolio_neural_benchmarks."
                    ),
                    juxtaposed_code={
                        "code_path": str(promoted_model_path),
                        "relative_path": "neural_networks/promoted/promoted_model.py",
                        "language": "python",
                        "exported_symbols": list(dict.fromkeys([prom_cls] + syms + h_syms))
                        or [prom_cls, "PromotedModelHarness"],
                        "code_excerpt": excerpt,
                        "sha256": sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(promoted_harness_path),
                        "doc_type": "docstring",
                        "doc_section": "Promoted Gauntlet Winner Architecture & Harness",
                        "doc_excerpt": f"{doc_str}\n\n{h_doc}".strip()[:850],
                        "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("neural", [])[:3]],
                    },
                    qdrant_namespace="portfolio_neural_benchmarks",
                    staging_path=str(
                        self.staging_manager.storage_dir
                        / "collection"
                        / "portfolio_neural_benchmarks"
                        / "storage.sqlite"
                    ),
                    hierarchy_level=2,
                    tags=[
                        "promoted_model",
                        "gauntlet",
                        "stage_compiler",
                        "cohesive_manifold",
                        "PinnedRank1",
                        "neural_networks",
                    ],
                )
            )
            add_edge(
                prom_node_id,
                "repo:neural_networks",
                "RESIDES_IN_REPO",
                weight=1.0,
                rationale="Promoted gauntlet model lives in /Users/jarek/IdeaProjects/neural_networks/promoted",
            )
            add_edge(
                prom_node_id,
                "module:toolset:benchmark_ledger",
                "WAYFINDING_NEXT_HOP",
                weight=1.0,
                rationale="BenchmarkLedger loads promoted_manifest.json via FSBase and pins it at Rank #1",
            )
            add_edge(
                "module:toolset:benchmark_ledger",
                "ui:portfolio:BenchmarksSection",
                "WAYFINDING_NEXT_HOP",
                weight=1.0,
                rationale="BenchmarksSection.tsx renders the pinned promoted model in quarter-inch pseudo-column rows",
            )
            add_edge(
                prom_node_id,
                "qdrant_ns:portfolio_neural_benchmarks",
                "INDEXED_IN_NAMESPACE",
                weight=1.0,
                rationale="Indexed alongside all 25 gauntlet models in portfolio_neural_benchmarks",
            )

        # Dedicated nodes for separated `stage_compiler` and `monadic_cavity` packages
        neural_arch_specs = [
            (
                "model_pkg:neural_networks:stage_compiler",
                "neural_architecture_package",
                "StageCompiler Cohesive Compiler Cascade Package (src/models/stage_compiler)",
                "stage_compiler",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "stage_compiler"
                / "stage_compiler_model.py",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "stage_compiler"
                / "benchmark_stage_compiler.py",
                [
                    "stage_compiler",
                    "PolyphonicStageCompilerCascadeModel",
                    "isa_ast_gate",
                    "cohesive_manifold",
                    "neural_networks",
                ],
            ),
            (
                "model_pkg:neural_networks:monadic_cavity",
                "neural_architecture_package",
                "Monadic Cavity Architecture Package (src/models/monadic_cavity)",
                "monadic_cavity",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "monadic_cavity"
                / "monadic_cavity_models.py",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "monadic_cavity"
                / "benchmark_monadic_cavity.py",
                [
                    "monadic_cavity",
                    "CohesiveUnifiedMonadicCavityModel",
                    "cohesive_manifold",
                    "vcpu",
                    "neural_networks",
                ],
            ),
            (
                "model:neural_networks:CohesiveUnifiedMonadicCavityModel",
                "neural_architecture_package",
                "Cohesive-Manifold Unified Monadic Cavity (src/models/monadic_cavity)",
                "CohesiveUnifiedMonadicCavityModel",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "monadic_cavity"
                / "monadic_cavity_models.py",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "monadic_cavity"
                / "benchmark_monadic_cavity.py",
                ["monadic_cavity", "cohesive_manifold", "vcpu", "neural_networks"],
            ),
        ]
        for (
            arch_node_id,
            arch_node_type,
            arch_title,
            arch_idiomatic,
            arch_code_path,
            arch_doc_path,
            arch_tags,
        ) in neural_arch_specs:
            if arch_node_id in nodes_by_id or not arch_code_path.is_file():
                continue
            a_syms, a_doc, a_excerpt, a_sha = self._extract_python_symbols_and_doc(arch_code_path)
            _, b_doc, _, _ = self._extract_python_symbols_and_doc(arch_doc_path)
            rel_arch = str(arch_code_path.relative_to(self.idea_projects_root))
            add_node(
                JuxtaposedNode(
                    node_id=arch_node_id,
                    node_type=arch_node_type,
                    title=arch_title,
                    idiomatic_name=arch_idiomatic,
                    repo_name="neural_networks",
                    keep_in_place=True,
                    placement_directive=(
                        f"INVARIANT: Keep separated neural architecture package at {arch_code_path} "
                        "and index in neural_networks_code_corpus and portfolio_neural_benchmarks."
                    ),
                    juxtaposed_code={
                        "code_path": str(arch_code_path),
                        "relative_path": rel_arch,
                        "language": "python",
                        "exported_symbols": a_syms or [arch_idiomatic],
                        "code_excerpt": a_excerpt,
                        "sha256": a_sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(arch_doc_path if arch_doc_path.is_file() else arch_code_path),
                        "doc_type": "docstring",
                        "doc_section": arch_title,
                        "doc_excerpt": f"{a_doc}\n\n{b_doc}".strip()[:850],
                        "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("neural", [])[:3]],
                    },
                    qdrant_namespace="portfolio_neural_benchmarks",
                    staging_path=str(
                        self.staging_manager.storage_dir
                        / "collection"
                        / "portfolio_neural_benchmarks"
                        / "storage.sqlite"
                    ),
                    hierarchy_level=2,
                    tags=arch_tags,
                )
            )
            add_edge(
                arch_node_id,
                "repo:neural_networks",
                "RESIDES_IN_REPO",
                weight=1.0,
                rationale=f"{arch_idiomatic} resides in {rel_arch}",
            )
            add_edge(
                arch_node_id,
                "module:toolset:benchmark_ledger",
                "WAYFINDING_NEXT_HOP",
                weight=1.0,
                rationale=f"BenchmarkLedger indexes {arch_idiomatic} into portfolio_neural_benchmarks",
            )
            add_edge(
                arch_node_id,
                "qdrant_ns:portfolio_neural_benchmarks",
                "INDEXED_IN_NAMESPACE",
                weight=1.0,
                rationale=f"{arch_idiomatic} is indexed in portfolio_neural_benchmarks",
            )
            add_edge(
                arch_node_id,
                "qdrant_ns:neural_networks_code_corpus",
                "INDEXED_IN_NAMESPACE",
                weight=0.95,
                rationale=f"{rel_arch} AST chunks are indexed in neural_networks_code_corpus",
            )

        # Link key cross-repo wayfinding hops for Antigravity Brain and Wiki
        if "ui:portfolio:AntigravityBrainSection" in nodes_by_id and "ui:components:AsyncChatWindow" in nodes_by_id:
            add_edge(
                "qdrant_ns:portfolio_antigravity_brain",
                "ui:portfolio:AntigravityBrainSection",
                "WAYFINDING_NEXT_HOP",
                weight=1.0,
                rationale="AntigravityBrainSection queries portfolio_antigravity_brain and renders conversations in AsyncChatWindow",
            )
        if "index:toolset:informational_index" in nodes_by_id:
            add_edge(
                "index:toolset:informational_index",
                "module:toolset:indexes",
                "WAYFINDING_NEXT_HOP",
                weight=1.0,
                rationale="AgentInformationalGraphIndex is part of the toolset `indexes` package",
            )

        # --- 8. Design, Compiler, and Runtime Code Tri-Layer Pipeline Nodes (Start-to-Finish Geodesic Path) ---
        pipeline_node_specs = [
            # Design Specifications (Stage 1: Design)
            (
                "design:large_tools:project_master_spec",
                "design_spec",
                "StageCompiler & Hypoborea Master Architecture Design Spec (PROJECT.md)",
                "project_master_spec",
                "large_tools",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                self.idea_projects_root / "large_tools" / "ORIGINAL_REQUEST.md",
                INFORMATIONAL_COLLECTION_NAME,
                1,
                ["design", "architecture", "stage_compiler", "hypoborea", "monadic_cavity", "spec"],
            ),
            (
                "design:hypoborea_vm:vcpu_microloop_spec",
                "design_spec",
                "Hypoborea vCPU Microloop & Monadic Cavity Execution Engine Spec",
                "vcpu_microloop_spec",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "hypoborea-vm"
                / "design"
                / "01_vmm_core_and_vcpu_pipeline"
                / "02_vcpu_microloop_and_execution_engine.md",
                self.idea_projects_root
                / "large_tools"
                / "hypoborea-vm"
                / "design"
                / "00_architecture_and_master_spec"
                / "01_hypervisor_master_specification.md",
                INFORMATIONAL_COLLECTION_NAME,
                1,
                ["design", "vcpu", "microloop", "monadic_cavity", "hypoborea_vm", "spec"],
            ),
            (
                "design:hypoborea_vm:polyphonic_cohesive_rns_register_spec",
                "design_spec",
                "Polyphonic Cohesive Multi-Tenant Hypervisor & Nested RNS RNSRegister Design Spec",
                "polyphonic_cohesive_rns_register_spec",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "hypoborea-vm"
                / "design"
                / "02_memory_management_and_isolation"
                / "03_polyphonic_cohesive_multi_tenant_hypervisor_and_nested_rns.md",
                self.idea_projects_root / "large_tools" / "hypoborea-vm" / "design" / "README.md",
                INFORMATIONAL_COLLECTION_NAME,
                1,
                ["design", "cohesive", "rns_register", "hypervisor", "memory", "spec"],
            ),
            # Compiler Pipeline (Stages 2..4: Stage-0 Bootstrap -> Stage-1 Ingress/Parser/Symbols/MCC/vCPU/Emitter/Linker -> Mach-O)
            (
                "compiler:stage_compiler:stage0_bootstrap",
                "compiler_stage",
                "StageCompiler Stage-0 Bootstrap Compiler (stage0/compiler_v2.py)",
                "stage0_compiler_v2",
                "large_tools",
                self.idea_projects_root / "large_tools" / "stage-compiler" / "stage0" / "compiler_v2.py",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage0", "bootstrap", "stage_compiler", "tier01_tier12"],
            ),
            (
                "compiler:stage_compiler:stage1_main",
                "compiler_stage",
                "StageCompiler Stage-1 Self-Hosting Compiler Driver (stage1/main.sig)",
                "stage1_main_sig",
                "large_tools",
                self.idea_projects_root / "large_tools" / "stage-compiler" / "stage1" / "main.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "main_sig", "stage_compiler", "repl", "vfs"],
            ),
            (
                "compiler:stage_compiler:stage1_ingress",
                "compiler_stage",
                "StageCompiler Stage-1 Tokenless Shift-Or Wave Masker Ingress (stage1/ingress/wave_masker.sig)",
                "wave_masker_ingress",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "ingress"
                / "wave_masker.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "ingress", "shift_or", "wave_masker", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:stage1_parser",
                "compiler_stage",
                "StageCompiler Stage-1 Argmin PEMDAS Wave Parser (stage1/parser/parser.sig)",
                "stage1_wave_parser",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "parser"
                / "parser.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "parser", "ast", "pemdas", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:stage1_symbols",
                "compiler_stage",
                "StageCompiler Stage-1 Julia Fractal Symbol Arena (stage1/symbols/symbol_arena.sig)",
                "symbol_arena_sig",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "symbols"
                / "symbol_arena.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "symbols", "symbol_arena", "julia_scope", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:stage1_mcc_ir",
                "compiler_stage",
                "StageCompiler Stage-1 Monadic Continuum Compiler IR & Scope Lowering (stage1/mcc/mcc.sig)",
                "stage1_mcc_ir",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "mcc"
                / "mcc.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "mcc", "ir", "cohesive_scope", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:stage1_emitter",
                "compiler_stage",
                "StageCompiler Stage-1 Wave Tape Emitter (stage1/emitter/tape_emitter.sig)",
                "tape_emitter_sig",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "emitter"
                / "tape_emitter.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "emitter", "tape_emitter", "codegen", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:stage1_linker",
                "compiler_stage",
                "StageCompiler Stage-1 Cohesive Linker (stage1/linker/cohesive_linker.sig)",
                "cohesive_linker_sig",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "stage1"
                / "linker"
                / "cohesive_linker.sig",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "stage1", "linker", "cohesive_linker", "stage_compiler"],
            ),
            (
                "compiler:stage_compiler:macho_synthesizer",
                "compiler_stage",
                "StageCompiler Native ARM64 Mach-O Binary Synthesizer (macho_synthesizer.py)",
                "macho_synthesizer",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage-compiler"
                / "macho_synthesizer.py",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["compiler", "macho", "arm64", "binary_emission", "stage_compiler"],
            ),
            # Runtime Code & Service Execution (Stages 5..7)
            (
                "code:stage_compiler:core_vcpu_monadic_cavity",
                "runtime_code",
                "StageCompiler Core Runtime vCPU Monadic Single-Cavity C_0 Engine (core/vcpu/monadic_cavity.sig)",
                "core_vcpu_monadic_cavity",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage_compiler"
                / "core"
                / "vcpu"
                / "monadic_cavity.sig",
                self.idea_projects_root
                / "large_tools"
                / "hypoborea-vm"
                / "design"
                / "01_vmm_core_and_vcpu_pipeline"
                / "02_vcpu_microloop_and_execution_engine.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["code", "runtime", "vcpu", "monadic_cavity", "5dof", "stage_compiler"],
            ),
            (
                "code:stage_compiler:core_vcpu_cohesive_continuum",
                "runtime_code",
                "StageCompiler Core Runtime Cohesive Register Continuum (core/vcpu/cohesive_register_continuum.sig)",
                "cohesive_register_continuum",
                "large_tools",
                self.idea_projects_root
                / "large_tools"
                / "stage_compiler"
                / "core"
                / "vcpu"
                / "cohesive_register_continuum.sig",
                self.idea_projects_root
                / "large_tools"
                / "hypoborea-vm"
                / "design"
                / "02_memory_management_and_isolation"
                / "03_polyphonic_cohesive_multi_tenant_hypervisor_and_nested_rns.md",
                INFORMATIONAL_COLLECTION_NAME,
                2,
                ["code", "runtime", "vcpu", "cohesive", "register_continuum", "stage_compiler"],
            ),
            (
                "code:neural_networks:implicit_vcpu_layer",
                "runtime_code",
                "Neural StageCompiler Implicit vCPU Layer (src/polyphonic_model/layers/stage_implicit_vcpu.py)",
                "StageCompilerImplicitVCPU",
                "neural_networks",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "polyphonic_model"
                / "layers"
                / "stage_implicit_vcpu.py",
                self.idea_projects_root / "large_tools" / "PROJECT.md",
                "neural_networks_code_corpus",
                2,
                ["code", "runtime", "vcpu", "neural_networks", "stage_implicit_vcpu"],
            ),
            (
                "code:neural_networks:promoted_harness",
                "runtime_code",
                "Promoted Gauntlet Model 6-Scale Any-Gram Qdrant Harness (promoted/harness.py)",
                "PromotedModelHarness",
                "neural_networks",
                self.idea_projects_root / "neural_networks" / "promoted" / "harness.py",
                self.idea_projects_root / "neural_networks" / "promoted" / "promoted_manifest.json",
                "portfolio_neural_benchmarks",
                2,
                ["code", "runtime", "harness", "promoted_model", "anygram", "qdrant", "neural_networks"],
            ),
            (
                "service:portfolio:benchmark_service",
                "portfolio_service",
                "Portfolio Neural Benchmark & Promoted Model Qdrant Service (benchmark_service.py)",
                "NeuralBenchmarkService",
                "portfolio",
                self.idea_projects_root
                / "portfolio"
                / "portfolio"
                / "python"
                / "portfoliolib"
                / "service"
                / "benchmark_service.py",
                self.idea_projects_root / "portfolio" / "portfolio" / "settings.yaml",
                "portfolio_neural_benchmarks",
                2,
                ["code", "service", "portfolio", "benchmark_service", "promoted_model", "qdrant"],
            ),
            (
                "service:portfolio:portfolio_api",
                "portfolio_service",
                "Portfolio FastAPI Router, WebSocket Chat & MCP Endpoint (portfolio_api.py)",
                "portfolio_router",
                "portfolio",
                self.idea_projects_root
                / "portfolio"
                / "portfolio"
                / "python"
                / "portfoliolib"
                / "service"
                / "portfolio_api.py",
                self.idea_projects_root / "portfolio" / "portfolio" / "settings.yaml",
                "portfolio_wiki_chunks",
                2,
                ["code", "service", "api", "websocket", "mcp", "portfolio"],
            ),
            (
                "service:portfolio:qdrant_graph_service",
                "portfolio_service",
                "Portfolio Qdrant Lifecycle, CPU Embeddings & Relational Graph Service (qdrant_graph_service.py)",
                "QdrantGraphService",
                "portfolio",
                self.idea_projects_root
                / "portfolio"
                / "portfolio"
                / "python"
                / "portfoliolib"
                / "service"
                / "qdrant_graph_service.py",
                self.idea_projects_root / "portfolio" / "portfolio" / "settings.yaml",
                "portfolio_wiki_chunks",
                2,
                ["code", "service", "qdrant", "graph", "portfolio"],
            ),
        ]

        for (
            p_node_id,
            p_node_type,
            p_title,
            p_idiomatic,
            p_repo,
            p_code_path,
            p_doc_path,
            p_ns,
            p_level,
            p_tags,
        ) in pipeline_node_specs:
            if p_node_id in nodes_by_id or not p_code_path.is_file():
                continue
            if p_code_path.suffix.lower() == ".py":
                p_syms, p_doc, p_excerpt, p_sha = self._extract_python_symbols_and_doc(p_code_path)
                lang = "python"
            else:
                p_syms, p_doc, p_excerpt, p_sha = self._extract_sig_or_md_symbols_and_doc(p_code_path)
                lang = "stage_compiler_sig" if p_code_path.suffix.lower() == ".sig" else "markdown"

            if p_doc_path.is_file():
                if p_doc_path.suffix.lower() == ".py":
                    _, d_doc, d_excerpt, _ = self._extract_python_symbols_and_doc(p_doc_path)
                else:
                    _, d_doc, d_excerpt, _ = self._extract_sig_or_md_symbols_and_doc(p_doc_path)
            else:
                d_doc, d_excerpt = p_doc, p_excerpt

            rel_p = str(p_code_path.relative_to(self.idea_projects_root))
            add_node(
                JuxtaposedNode(
                    node_id=p_node_id,
                    node_type=p_node_type,
                    title=p_title,
                    idiomatic_name=p_idiomatic,
                    repo_name=p_repo,
                    keep_in_place=True,
                    placement_directive=(
                        f"INVARIANT: Keep {p_node_type} artifact in place at {p_code_path} "
                        f"as part of the Design -> Compiler -> Code geodesic pipeline."
                    ),
                    juxtaposed_code={
                        "code_path": str(p_code_path),
                        "relative_path": rel_p,
                        "language": lang,
                        "exported_symbols": p_syms or [p_idiomatic],
                        "code_excerpt": p_excerpt,
                        "sha256": p_sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(p_doc_path if p_doc_path.is_file() else p_code_path),
                        "doc_type": "design_spec" if p_node_type == "design_spec" else "architecture_doc",
                        "doc_section": p_title,
                        "doc_excerpt": (f"{p_doc}\n\n{d_doc or d_excerpt}").strip()[:850],
                        "related_ki_ids": [x["ki_id"] for x in ki_lookup.get("stage_compiler", [])[:3]],
                    },
                    qdrant_namespace=p_ns,
                    staging_path=str(
                        self.staging_manager.storage_dir / "collection" / p_ns / "storage.sqlite"
                    ),
                    hierarchy_level=p_level,
                    tags=p_tags,
                )
            )
            if f"repo:{p_repo}" in nodes_by_id:
                add_edge(
                    p_node_id,
                    f"repo:{p_repo}",
                    "RESIDES_IN_REPO",
                    weight=1.0,
                    rationale=f"{p_idiomatic} resides in {rel_p}",
                )

        # Ordered End-to-End Geodesic Pipeline Edges (Design -> Stage-0/1 Compiler -> Core Runtime -> Neural Model -> Harness -> Ledger -> Service -> API -> UI)
        geodesic_pipeline_chain = [
            (
                "design:large_tools:project_master_spec",
                "design:hypoborea_vm:vcpu_microloop_spec",
                "DESIGN_REFINES_TO_SUBSPEC",
                "Master StageCompiler/Hypoborea spec defines the 5-DoF Monadic Cavity C_0 vCPU microloop architecture",
            ),
            (
                "design:hypoborea_vm:vcpu_microloop_spec",
                "design:hypoborea_vm:polyphonic_cohesive_rns_register_spec",
                "DESIGN_REFINES_TO_SUBSPEC",
                "vCPU microloop couples with the Polyphonic Cohesive & Nested RNS RNSRegister isolation architecture",
            ),
            (
                "design:hypoborea_vm:polyphonic_cohesive_rns_register_spec",
                "compiler:stage_compiler:stage0_bootstrap",
                "DESIGN_SPECIFIES_COMPILER",
                "Design specs are materialized by the Stage-0 Python bootstrap compiler (stage0/compiler_v2.py, Tiers 01..12)",
            ),
            (
                "compiler:stage_compiler:stage0_bootstrap",
                "compiler:stage_compiler:stage1_main",
                "COMPILER_STAGE_NEXT",
                "Stage-0 bootstrap compiler compiles and boots the Stage-1 self-hosting compiler driver (stage1/main.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_main",
                "compiler:stage_compiler:stage1_ingress",
                "COMPILER_STAGE_NEXT",
                "Stage-1 driver dispatches source waves into Tokenless Shift-Or Wave Masker Ingress (stage1/ingress/wave_masker.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_ingress",
                "compiler:stage_compiler:stage1_parser",
                "COMPILER_STAGE_NEXT",
                "Shift-Or bitmasks feed Argmin PEMDAS Wave Parser (stage1/parser/parser.sig) to split expression trees",
            ),
            (
                "compiler:stage_compiler:stage1_parser",
                "compiler:stage_compiler:stage1_symbols",
                "COMPILER_STAGE_NEXT",
                "Parsed AST waves bind lexical scopes in the Julia Fractal Symbol Arena (stage1/symbols/symbol_arena.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_symbols",
                "compiler:stage_compiler:stage1_mcc_ir",
                "COMPILER_STAGE_NEXT",
                "Symbol arena lowers into Monadic Continuum Compiler IR & Cohesive Scopes (stage1/mcc/mcc.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_mcc_ir",
                "compiler:stage_compiler:stage1_emitter",
                "COMPILER_STAGE_NEXT",
                "MCC IR lowers into 64-float wave packets via Stage-1 Wave Tape Emitter (stage1/emitter/tape_emitter.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_emitter",
                "compiler:stage_compiler:stage1_linker",
                "COMPILER_STAGE_NEXT",
                "Emitted wave tapes are linked across cohesive stalks by Stage-1 Cohesive Linker (stage1/linker/cohesive_linker.sig)",
            ),
            (
                "compiler:stage_compiler:stage1_linker",
                "compiler:stage_compiler:macho_synthesizer",
                "COMPILER_STAGE_NEXT",
                "Linked wave packets synthesize native Apple Silicon ARM64 Mach-O binaries via macho_synthesizer.py",
            ),
            (
                "compiler:stage_compiler:macho_synthesizer",
                "code:stage_compiler:core_vcpu_monadic_cavity",
                "COMPILER_EMITS_RUNTIME",
                "Synthesized instructions execute on the StageCompiler Core Runtime Monadic Single-Cavity C_0 vCPU (core/vcpu/monadic_cavity.sig)",
            ),
            (
                "code:stage_compiler:core_vcpu_monadic_cavity",
                "code:stage_compiler:core_vcpu_cohesive_continuum",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "Monadic Cavity C_0 modulates 24-stalk 120-DoF states in the Cohesive Register Continuum (core/vcpu/cohesive_register_continuum.sig)",
            ),
            (
                "code:stage_compiler:core_vcpu_cohesive_continuum",
                "code:neural_networks:implicit_vcpu_layer",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "Cohesive Register Continuum is embedded into differentiable PyTorch tensors via StageCompilerImplicitVCPU",
            ),
            (
                "code:neural_networks:implicit_vcpu_layer",
                "model_pkg:neural_networks:stage_compiler",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "StageCompilerImplicitVCPU + ISA Spectral-AST Gate power PolyphonicStageCompilerCascadeModel",
            ),
            (
                "model_pkg:neural_networks:stage_compiler",
                "model_pkg:neural_networks:monadic_cavity",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "Couples with CohesiveUnifiedMonadicCavityModel in the 12-benchmark neural gauntlet",
            ),
            (
                "model_pkg:neural_networks:monadic_cavity",
                "code:neural_networks:promoted_harness",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "Gauntlet winner is promoted to promoted/ and wrapped in PromotedModelHarness with 6-Scale Any-Gram Qdrant memory",
            ),
            (
                "code:neural_networks:promoted_harness",
                "module:toolset:benchmark_ledger",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "BenchmarkLedger indexes the promoted model & harness into Qdrant portfolio_neural_benchmarks",
            ),
            (
                "module:toolset:benchmark_ledger",
                "service:portfolio:benchmark_service",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "NeuralBenchmarkService queries Qdrant for the promoted model and drives PromotedModelHarness chat/streaming",
            ),
            (
                "service:portfolio:benchmark_service",
                "service:portfolio:portfolio_api",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "portfolio_api.py exposes /api/benchmarks/promoted, /api/benchmarks/promoted/ws/chat, and /api/mcp",
            ),
            (
                "service:portfolio:portfolio_api",
                "ui:portfolio:PromotedModelSection",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "PromotedModelSection.tsx renders Qdrant provenance, PromotedModelHarness source, and live chat UI",
            ),
            (
                "ui:portfolio:PromotedModelSection",
                "ui:components:AsyncChatWindow",
                "EXECUTES_CODE_PATH_NEXT_HOP",
                "PromotedModelSection streams bidirectional WebSocket chat through shared AsyncChatWindow component",
            ),
        ]

        for src_id, dst_id, rel_name, rationale_str in geodesic_pipeline_chain:
            if src_id in nodes_by_id and dst_id in nodes_by_id:
                add_edge(src_id, dst_id, rel_name, weight=0.95, rationale=rationale_str)

        # --- 9. Antigravity Knowledge Item Nodes (Agent Sync & Geodesic Protocol KIs) ---
        all_ki_infos = ki_lookup.get("all", [])
        global_ki_ids = [x["ki_id"] for x in all_ki_infos]
        for ki_info in all_ki_infos:
            ki_id = ki_info["ki_id"]
            ki_node_id = f"ki:antigravity:{ki_id}"
            ki_doc_path = Path(ki_info["doc_path"])
            ki_excerpt = ""
            ki_sha = hashlib.sha256(ki_id.encode("utf-8")).hexdigest()
            if ki_doc_path.is_file():
                raw_ki = ki_doc_path.read_text(encoding="utf-8", errors="replace")
                ki_excerpt = raw_ki[:850]
                ki_sha = hashlib.sha256(raw_ki.encode("utf-8")).hexdigest()
                masked_path_str = mask_restricted_string(str(ki_doc_path))
                if masked_path_str != str(ki_doc_path):
                    masked_path = Path(masked_path_str)
                    if not masked_path.is_file():
                        masked_path.parent.mkdir(parents=True, exist_ok=True)
                        masked_path.write_text(mask_restricted_string(raw_ki), encoding="utf-8")
                    ki_doc_path = masked_path
            add_node(
                JuxtaposedNode(
                    node_id=ki_node_id,
                    node_type="knowledge_item",
                    title=f"Knowledge Item: {ki_info['title']}",
                    idiomatic_name=ki_id,
                    repo_name="qdrant_staging",
                    keep_in_place=True,
                    placement_directive=(
                        f"INVARIANT: Canonical Antigravity Knowledge Item at {ki_doc_path} "
                        "teaching all agents how to sync Qdrant staging and trace geodesic code paths."
                    ),
                    juxtaposed_code={
                        "code_path": str(
                            self.idea_projects_root
                            / "toolset"
                            / "src"
                            / "indexes"
                            / "informational_index"
                            / "mcp_server.py"
                        ),
                        "relative_path": "toolset/src/indexes/informational_index/mcp_server.py",
                        "language": "python",
                        "exported_symbols": [
                            "QdrantGeodesicLifecycleMCPServer",
                            "AgentInformationalGraphIndex",
                            "UnifiedQdrantStagingManager",
                        ],
                        "code_excerpt": (
                            "idx.sync_staging(direction='bidirectional', rebuild_graph=True)\n"
                            "mcp.handle_jsonrpc_message({'method': 'tools/call', 'params': {'name': 'manage_system_lifecycle', ...}})"
                        ),
                        "sha256": ki_sha,
                    },
                    juxtaposed_docs={
                        "doc_path": str(ki_doc_path),
                        "doc_type": "knowledge_item_md",
                        "doc_section": ki_info["title"],
                        "doc_excerpt": f"{ki_info['summary']}\n\n{ki_excerpt}".strip()[:900],
                        "related_ki_ids": global_ki_ids,
                    },
                    qdrant_namespace=INFORMATIONAL_COLLECTION_NAME,
                    staging_path=str(self.staging_manager.storage_dir),
                    hierarchy_level=1,
                    tags=["knowledge_item", "ki", "sync", "mcp", "geodesic", "qdrant_staging", ki_id],
                )
            )
            add_edge(
                ki_node_id,
                staging_node_id,
                "UNIFIED_INTO_STAGING",
                weight=1.0,
                rationale=f"Knowledge Item {ki_id} documents cross-repo Qdrant staging sync and MCP lifecycle tools",
            )

        # Ensure every node carries related_ki_ids linking to the canonical sync & geodesic KIs
        for node in nodes_by_id.values():
            existing_kis = list(node.juxtaposed_docs.get("related_ki_ids") or [])
            for gid in global_ki_ids:
                if gid not in existing_kis:
                    existing_kis.append(gid)
            node.juxtaposed_docs["related_ki_ids"] = existing_kis[:6]

        # Attach incident edges onto each node's `graph_edges` list
        edges_by_src: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for e in edges:
            ed = e.to_dict()
            edges_by_src[e.source].append(ed)
            # Also store reverse visibility for wayfinding
            rev = {
                "source": e.target,
                "target": e.source,
                "relation": f"INVERSE_{e.relation}",
                "weight": e.weight,
                "rationale": e.rationale,
            }
            edges_by_src[e.target].append(rev)

        for nid, node in nodes_by_id.items():
            node.graph_edges = edges_by_src.get(nid, [])

        return list(nodes_by_id.values()), edges

    def build_and_unify(
        self,
        force_rebuild: bool = False,
        export_snapshots: bool = True,
    ) -> Dict[str, Any]:
        """
        1. Unifies all Qdrant namespaces across sibling repos into `/Users/jarek/IdeaProjects/qdrant_staging/storage`.
        2. Builds the juxtaposed Code + Documentation `JuxtaposedNode`s and `JuxtaposedGraphEdge`s.
        3. Upserts all juxtaposed nodes into `agent_informational_graph_index` in `/Users/jarek/IdeaProjects/qdrant_staging/storage`
           and mirrors the collection into `/Users/jarek/IdeaProjects/portfolio/portfolio/.qdrant_data`.
        4. Writes `/Users/jarek/IdeaProjects/qdrant_staging/informational_graph_ledger.json` and updates `staging_manifest.json`.
        """
        t0 = time.perf_counter()
        unified_manifest = self.staging_manager.unify_all_collections(export_snapshots=False)
        nodes, edges = self._construct_juxtaposed_graph(unified_manifest)

        points: List[PointStruct] = []
        for node in nodes:
            payload = node.to_payload()
            vec = _deterministic_384d_vector(payload["search_text"], dim=DEFAULT_VECTOR_SIZE)
            pt = PointStruct(
                id=payload["point_id"],
                vector=vec,
                payload=payload,
            )
            points.append(pt)
            self.index.store_in_index("nodes", node.node_id, payload)
            self.index.store_in_index("edges_by_node", node.node_id, node.graph_edges)

        staged_count = self.staging_manager.upsert_points_lockfree(
            storage_path=self.staging_manager.storage_dir,
            collection_name=self.collection_name,
            points=points,
            vector_size=DEFAULT_VECTOR_SIZE,
            recreate=True,
        )

        # Mirror `agent_informational_graph_index` to portfolio/.qdrant_data so local portfolio queries also see it
        portfolio_qdrant_dir = (
            self.idea_projects_root / "portfolio" / "portfolio" / ".qdrant_data"
        )
        if portfolio_qdrant_dir.is_dir():
            self.staging_manager.upsert_points_lockfree(
                storage_path=portfolio_qdrant_dir,
                collection_name=self.collection_name,
                points=points,
                vector_size=DEFAULT_VECTOR_SIZE,
                recreate=True,
            )

        # Re-run manifest unification so `agent_informational_graph_index` point count is recorded in `staging_manifest.json`
        final_manifest = self.staging_manager.unify_all_collections(export_snapshots=export_snapshots)

        by_type = collections.Counter(n.node_type for n in nodes)
        by_repo = collections.Counter(n.repo_name for n in nodes)
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        ledger_data = {
            "collection_name": self.collection_name,
            "staging_root": str(self.staging_root),
            "storage_dir": str(self.staging_manager.storage_dir),
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "staged_qdrant_points": staged_count,
            "node_counts_by_type": dict(sorted(by_type.items())),
            "node_counts_by_repo": dict(sorted(by_repo.items())),
            "nodes": [n.to_payload() for n in nodes],
            "edges": [e.to_dict() for e in edges],
            "built_at": round(time.time(), 3),
            "build_wall_time_ms": elapsed_ms,
        }
        self.staging_manager.ledger_path.write_text(
            json.dumps(ledger_data, indent=2) + "\n",
            encoding="utf-8",
        )

        telemetry = {
            "status": "READY",
            "collection_name": self.collection_name,
            "staging_root": str(self.staging_root),
            "storage_dir": str(self.staging_manager.storage_dir),
            "snapshots_dir": str(self.staging_manager.snapshots_dir),
            "manifest_path": str(self.staging_manager.manifest_path),
            "ledger_path": str(self.staging_manager.ledger_path),
            "total_juxtaposed_nodes": len(nodes),
            "total_graph_edges": len(edges),
            "node_counts_by_type": dict(sorted(by_type.items())),
            "node_counts_by_repo": dict(sorted(by_repo.items())),
            "unified_collections": final_manifest.get("collections", {}),
            "total_collections": final_manifest.get("total_collections", 0),
            "total_staged_points": final_manifest.get("total_staged_points", 0),
            "placement_invariants": final_manifest.get("placement_invariants", {}),
            "build_wall_time_ms": elapsed_ms,
            "updated_at": round(time.time(), 3),
        }
        self.index.store_in_index("telemetry", "latest", telemetry)
        self.stats.add_counter(1)
        self.audit_log.append(
            {
                "event": "informational_graph_built_and_unified",
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "total_staged_points": final_manifest.get("total_staged_points", 0),
                "wall_time_ms": elapsed_ms,
                "timestamp": round(time.time(), 3),
            }
        )
        return telemetry

    def get_staging_overview(self) -> Dict[str, Any]:
        """Returns the complete overview of `/Users/jarek/IdeaProjects/qdrant_staging` and the informational index."""
        latest = self.index.load_from_index("telemetry", "latest")
        if not isinstance(latest, dict):
            return self.build_and_unify(force_rebuild=False)
        return latest

    def query_datasource(
        self,
        query: str = "",
        node_type: Optional[str] = None,
        repo_name: Optional[str] = None,
        keep_in_place: Optional[bool] = None,
        top_k: int = 25,
    ) -> Dict[str, Any]:
        """Implements `Datasource.query_datasource` by querying the juxtaposed informational graph."""
        return self.query_juxtaposed_map(
            query=query,
            node_type=node_type,
            repo_name=repo_name,
            keep_in_place=keep_in_place,
            top_k=top_k,
        )

    def query_juxtaposed_map(
        self,
        query: str = "",
        node_type: Optional[str] = None,
        repo_name: Optional[str] = None,
        keep_in_place: Optional[bool] = None,
        top_k: int = 25,
    ) -> Dict[str, Any]:
        """
        Searches `agent_informational_graph_index` using hybrid cosine similarity + lexical token
        matching and returns juxtaposed `{code, documentation, placement_directive, graph_edges}`
        records so agents immediately see where code and documentation live side-by-side.
        """
        all_nodes: List[Dict[str, Any]] = [
            v for _, v in self.index.range_index("nodes") if isinstance(v, dict)
        ]
        if not all_nodes:
            self.build_and_unify(force_rebuild=False)
            all_nodes = [
                v for _, v in self.index.range_index("nodes") if isinstance(v, dict)
            ]

        repo_filter = mask_restricted_string(repo_name) if repo_name else None
        filtered: List[Dict[str, Any]] = []
        for n in all_nodes:
            if node_type and n.get("node_type") != node_type:
                continue
            if repo_filter and n.get("repo_name") != repo_filter:
                continue
            if keep_in_place is not None and bool(n.get("keep_in_place")) != bool(keep_in_place):
                continue
            filtered.append(n)

        q_clean = mask_restricted_string((query or "").strip())
        if not q_clean:
            scored = [(1.0, n) for n in filtered]
            scored.sort(key=lambda x: (x[1].get("hierarchy_level", 9), x[1].get("node_id", "")))
        else:
            q_vec = _deterministic_384d_vector(q_clean, dim=DEFAULT_VECTOR_SIZE)
            q_tokens = [t for t in re.findall(r"[a-zA-Z0-9_]{2,}", q_clean.lower())]
            scored = []
            for n in filtered:
                s_text = str(n.get("search_text", "")).lower()
                n_vec = _deterministic_384d_vector(s_text, dim=DEFAULT_VECTOR_SIZE)
                cosine = sum(a * b for a, b in zip(q_vec, n_vec))
                lexical_hits = sum(1 for tok in q_tokens if tok in s_text)
                idiomatic_lower = str(n.get("idiomatic_name", "")).lower()
                node_id_lower = str(n.get("node_id", "")).lower()
                if q_clean.lower() in (idiomatic_lower, node_id_lower):
                    exact_id_boost = 1.5
                elif q_clean.lower() in node_id_lower or q_clean.lower() in idiomatic_lower:
                    exact_id_boost = 0.65
                else:
                    exact_id_boost = 0.0
                lex_score = (lexical_hits / max(1, len(q_tokens))) * 0.55
                total_score = round(float(cosine * 0.45 + lex_score + exact_id_boost), 4)
                if total_score > 0.05 or lexical_hits > 0:
                    scored.append((total_score, n))
            scored.sort(key=lambda x: x[0], reverse=True)

        results: List[Dict[str, Any]] = []
        for score, n in scored[: max(1, int(top_k))]:
            item = dict(n)
            item.pop("search_text", None)
            item["score"] = score
            results.append(item)

        return {
            "query": q_clean,
            "filters": {
                "node_type": node_type,
                "repo_name": repo_name,
                "keep_in_place": keep_in_place,
            },
            "total_matches": len(scored),
            "results": results,
            "staging_root": str(self.staging_root),
            "collection_name": self.collection_name,
        }

    def trace_wayfinding_path(
        self,
        start_query: str,
        target_query: str,
    ) -> Dict[str, Any]:
        """
        Finds the shortest path in the juxtaposed informational graph between `start_query`
        and `target_query`, returning every hop with its juxtaposed code path, doc path,
        Qdrant namespace, and placement directive so no agent can get lost.
        """
        start_matches = self.query_juxtaposed_map(query=start_query, top_k=1)["results"]
        target_matches = self.query_juxtaposed_map(query=target_query, top_k=1)["results"]
        if not start_matches or not target_matches:
            return {
                "found": False,
                "start_query": start_query,
                "target_query": target_query,
                "hops": [],
                "edges": [],
            }

        start_id = start_matches[0]["node_id"]
        target_id = target_matches[0]["node_id"]

        if start_id == target_id:
            return {
                "found": True,
                "start_node_id": start_id,
                "target_node_id": target_id,
                "hop_count": 1,
                "hops": [start_matches[0]],
                "edges": [],
            }

        queue: collections.deque[Tuple[str, List[str], List[Dict[str, Any]]]] = collections.deque(
            [(start_id, [start_id], [])]
        )
        visited = {start_id}

        while queue:
            curr_id, path_ids, path_edges = queue.popleft()
            adj_edges = self.index.load_from_index("edges_by_node", curr_id) or []
            for edge in adj_edges:
                nxt_id = edge.get("target")
                if not nxt_id or nxt_id in visited:
                    continue
                next_path = path_ids + [nxt_id]
                next_edges = path_edges + [edge]
                if nxt_id == target_id:
                    hops = []
                    for pid in next_path:
                        node_obj = self.index.load_from_index("nodes", pid)
                        if isinstance(node_obj, dict):
                            clean_node = dict(node_obj)
                            clean_node.pop("search_text", None)
                            hops.append(clean_node)
                    return {
                        "found": True,
                        "start_node_id": start_id,
                        "target_node_id": target_id,
                        "hop_count": len(hops),
                        "hops": hops,
                        "edges": next_edges,
                    }
                visited.add(nxt_id)
                queue.append((nxt_id, next_path, next_edges))

        return {
            "found": False,
            "start_node_id": start_id,
            "target_node_id": target_id,
            "hop_count": 0,
            "hops": [],
            "edges": [],
        }

    def sync_staging(
        self,
        target_environment: Optional[str | Path] = None,
        direction: str = "bidirectional",
        collections: Optional[List[str]] = None,
        rebuild_graph: bool = True,
    ) -> Dict[str, Any]:
        """
        Rebuilds the juxtaposed informational graph (if `rebuild_graph=True`), unifies all
        repo collections into `/Users/jarek/IdeaProjects/qdrant_staging/storage`, and syncs
        with `target_environment` (`stage_to_target`, `target_to_stage`, or `bidirectional`).
        """
        if rebuild_graph:
            self.build_and_unify(force_rebuild=True)
        sync_res = self.staging_manager.sync_with_environment(
            target_environment=target_environment,
            direction=direction,
            collections=collections,
        )
        self.audit_log.append(
            {
                "event": "qdrant_staging_synced",
                "target_environment": str(target_environment or "repo_local_qdrant_stores"),
                "direction": direction,
                "wall_time_ms": sync_res.get("wall_time_ms"),
                "timestamp": round(time.time(), 3),
            }
        )
        return {
            "sync_result": sync_res,
            "staging_overview": self.get_staging_overview(),
        }

    # ------------------------------------------------------------------
    # Multi-System Catalog, Unified Qdrant Search, Tri-Layer Juxtaposition,
    # Start-to-Finish Geodesic Code Path & Full Lifecycle Management
    # ------------------------------------------------------------------
    @staticmethod
    def _poincare_distance(u: List[float], v: List[float]) -> float:
        if not u or not v:
            return 0.0
        dim = min(len(u), len(v))
        diff_sq = sum((u[i] - v[i]) ** 2 for i in range(dim))
        nu = sum(u[i] ** 2 for i in range(dim))
        nv = sum(v[i] ** 2 for i in range(dim))
        denom = max(1.0 - nu, 1e-6) * max(1.0 - nv, 1e-6)
        arg = max(1.0, 1.0 + (2.0 * diff_sq) / denom)
        return round(math.acosh(arg), 6)

    @staticmethod
    def _poincare_midpoint(u: List[float], v: List[float]) -> List[float]:
        if not u or not v:
            return []
        dim = min(len(u), len(v))
        nu = sum(u[i] ** 2 for i in range(dim))
        nv = sum(v[i] ** 2 for i in range(dim))
        gu = 2.0 / max(1.0 - nu, 1e-6)
        gv = 2.0 / max(1.0 - nv, 1e-6)
        denom = max(gu + gv - 1.0, 1e-6)
        mid = [round((gu * u[i] + gv * v[i]) / denom, 5) for i in range(dim)]
        return mid

    @staticmethod
    def _classify_node_layer(node: Dict[str, Any]) -> str:
        ntype = str(node.get("node_type", ""))
        nid = str(node.get("node_id", ""))
        if ntype == "design_spec" or nid.startswith("design:"):
            return "design"
        if ntype == "compiler_stage" or nid.startswith("compiler:") or "stage_compiler" in nid:
            return "compiler"
        if ntype in ("ui_component",):
            return "ui"
        if ntype in ("toolset_index", "qdrant_namespace", "qdrant_staging_root"):
            return "index"
        if ntype in ("promoted_model", "neural_architecture_package"):
            return "model"
        return "code"

    def _build_tri_layer_juxtaposition_for_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds a cohesive side-by-side `{design, compiler, code}` juxtaposition bundle
        for any node in `agent_informational_graph_index`.
        """
        nid = str(node.get("node_id", ""))
        repo = str(node.get("repo_name", ""))
        code_info = dict(node.get("juxtaposed_code") or {})
        doc_info = dict(node.get("juxtaposed_docs") or {})

        # Resolve companion Design and Compiler nodes
        design_node = (
            self.index.load_from_index("nodes", "design:large_tools:project_master_spec")
            or self.index.load_from_index("nodes", "design:hypoborea_vm:vcpu_microloop_spec")
            or {}
        )
        if repo in ("toolset", "portfolio", "components"):
            design_info = {
                "design_node_id": f"design:{repo}:spec",
                "doc_path": doc_info.get("doc_path", str(self.skill_md_path)),
                "doc_type": doc_info.get("doc_type", "skill_md"),
                "section": doc_info.get("doc_section", node.get("title", "")),
                "placement_directive": node.get("placement_directive", ""),
                "excerpt": doc_info.get("doc_excerpt", "")[:600],
            }
        else:
            d_code = (design_node.get("juxtaposed_code") or {}) if isinstance(design_node, dict) else {}
            design_info = {
                "design_node_id": design_node.get("node_id", "design:large_tools:project_master_spec"),
                "doc_path": doc_info.get("doc_path") or d_code.get("code_path", ""),
                "doc_type": doc_info.get("doc_type", "design_spec"),
                "section": doc_info.get("doc_section", node.get("title", "")),
                "placement_directive": node.get("placement_directive", ""),
                "excerpt": (doc_info.get("doc_excerpt") or d_code.get("code_excerpt", ""))[:600],
            }

        # Resolve Compiler companion
        compiler_node_id = "compiler:stage_compiler:stage1_main"
        if "stage0" in nid:
            compiler_node_id = "compiler:stage_compiler:stage0_bootstrap"
        elif "ingress" in nid or "shift_or" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_ingress"
        elif "parser" in nid or "ast" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_parser"
        elif "symbol" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_symbols"
        elif "mcc" in nid or "cohesive" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_mcc_ir"
        elif "emit" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_emitter"
        elif "link" in nid:
            compiler_node_id = "compiler:stage_compiler:stage1_linker"
        elif "macho" in nid:
            compiler_node_id = "compiler:stage_compiler:macho_synthesizer"
        elif repo == "neural_networks" or "promoted" in nid or "benchmark" in nid.lower():
            compiler_node_id = "model_pkg:neural_networks:stage_compiler"

        comp_node = self.index.load_from_index("nodes", compiler_node_id) or {}
        comp_code = (comp_node.get("juxtaposed_code") or {}) if isinstance(comp_node, dict) else {}
        compiler_info = {
            "compiler_node_id": comp_node.get("node_id", compiler_node_id),
            "title": comp_node.get("title", "StageCompiler Stage-1 & Cohesive Compiler Cascade"),
            "compiler_path": comp_code.get("code_path", ""),
            "relative_path": comp_code.get("relative_path", ""),
            "exported_symbols": comp_code.get("exported_symbols", []),
            "compiler_stages": [
                "compiler:stage_compiler:stage0_bootstrap (stage0/compiler_v2.py)",
                "compiler:stage_compiler:stage1_ingress (stage1/ingress/wave_masker.sig)",
                "compiler:stage_compiler:stage1_parser (stage1/parser/parser.sig)",
                "compiler:stage_compiler:stage1_symbols (stage1/symbols/symbol_arena.sig)",
                "compiler:stage_compiler:stage1_mcc_ir (stage1/mcc/mcc.sig)",
                "compiler:stage_compiler:stage1_emitter (stage1/emitter/tape_emitter.sig)",
                "compiler:stage_compiler:stage1_linker (stage1/linker/cohesive_linker.sig)",
                "compiler:stage_compiler:macho_synthesizer (macho_synthesizer.py)",
                "model_pkg:neural_networks:stage_compiler (PolyphonicStageCompilerCascadeModel)",
            ],
            "excerpt": comp_code.get("code_excerpt", "")[:600],
        }

        runtime_info = {
            "code_node_id": nid,
            "code_path": code_info.get("code_path", ""),
            "relative_path": code_info.get("relative_path", ""),
            "language": code_info.get("language", "python"),
            "exported_symbols": code_info.get("exported_symbols", []),
            "sha256": code_info.get("sha256", ""),
            "qdrant_namespace": node.get("qdrant_namespace", self.collection_name),
            "keep_in_place": bool(node.get("keep_in_place", True)),
            "excerpt": code_info.get("code_excerpt", "")[:600],
        }

        return {
            "design": design_info,
            "compiler": compiler_info,
            "code": runtime_info,
        }

    def list_systems(
        self,
        include_collections: bool = True,
        include_lifecycle: bool = True,
    ) -> Dict[str, Any]:
        """
        Lists every system across `/Users/jarek/IdeaProjects` (`portfolio`, `components`,
        `toolset`, `neural_networks`, `large_tools`, `StageCompiler-development`, `qdrant_staging`)
        with its Qdrant collections, point counts, design/compiler/code entrypoints,
        placement invariants (`keep_in_place`), and lifecycle health.
        """
        overview = self.get_staging_overview()
        unified_colls = overview.get("unified_collections", {})
        node_counts_by_repo = overview.get("node_counts_by_repo", {})

        system_catalog_specs = [
            {
                "system_id": "portfolio",
                "name": "Portfolio Architect Workbench, Dynamic Wiki & Promoted Model Chat",
                "root_path": str(self.idea_projects_root / "portfolio" / "portfolio"),
                "repo_name": "portfolio",
                "keep_in_place": True,
                "qdrant_collections": [
                    "portfolio_wiki_chunks",
                    "portfolio_neural_benchmarks",
                    "portfolio_antigravity_brain",
                    INFORMATIONAL_COLLECTION_NAME,
                ],
                "layers": {
                    "design": str(self.idea_projects_root / "portfolio" / "portfolio" / "settings.yaml"),
                    "compiler": str(self.idea_projects_root / "portfolio" / "portfolio" / "react" / "vite.config.ts"),
                    "code_backend": str(
                        self.idea_projects_root
                        / "portfolio"
                        / "portfolio"
                        / "python"
                        / "portfoliolib"
                        / "service"
                        / "portfolio_service.py"
                    ),
                    "code_frontend": str(
                        self.idea_projects_root
                        / "portfolio"
                        / "portfolio"
                        / "react"
                        / "src"
                        / "components"
                        / "PromotedModelSection.tsx"
                    ),
                },
                "entrypoints": [
                    "portfolio/portfolio/main.py",
                    "portfolio/portfolio/python/portfoliolib/service/portfolio_api.py",
                    "portfolio/portfolio/react/src/App.tsx",
                ],
            },
            {
                "system_id": "components",
                "name": "Shared React 19 & MUI v6 Meta-Components Library",
                "root_path": str(self.idea_projects_root / "components"),
                "repo_name": "components",
                "keep_in_place": True,
                "qdrant_collections": [INFORMATIONAL_COLLECTION_NAME],
                "layers": {
                    "design": str(self.idea_projects_root / "components" / "package.json"),
                    "compiler": str(self.idea_projects_root / "components" / "tsconfig.json"),
                    "code": str(
                        self.idea_projects_root
                        / "components"
                        / "src"
                        / "meta_components"
                        / "async_chat_window"
                        / "async_chat_window.tsx"
                    ),
                },
                "entrypoints": [
                    "components/src/index.ts",
                    "components/src/meta_components/async_chat_window/async_chat_window.tsx",
                ],
            },
            {
                "system_id": "toolset",
                "name": "37-Library Python Infrastructure, Thread-Safe Indexes & Geodesic Code Graph",
                "root_path": str(self.idea_projects_root / "toolset"),
                "repo_name": "toolset",
                "keep_in_place": True,
                "qdrant_collections": [
                    INFORMATIONAL_COLLECTION_NAME,
                    "portfolio_wiki_chunks",
                    "portfolio_neural_benchmarks",
                    "neural_networks_code_corpus",
                ],
                "layers": {
                    "design": str(self.skill_md_path),
                    "compiler": str(
                        self.idea_projects_root
                        / "toolset"
                        / "src"
                        / "indexes"
                        / "code_index"
                        / "code_index.py"
                    ),
                    "code": str(
                        self.idea_projects_root
                        / "toolset"
                        / "src"
                        / "indexes"
                        / "informational_index"
                        / "informational_index.py"
                    ),
                },
                "entrypoints": [
                    "toolset/src/indexes/informational_index/informational_index.py",
                    "toolset/src/indexes/informational_index/mcp_server.py",
                    "toolset/src/indexes/code_index/code_index.py",
                    "toolset/src/benchmark_ledger/ledger.py",
                ],
            },
            {
                "system_id": "neural_networks",
                "name": "Cohesive-Manifold & Monadic Cavity Neural Architectures + Promoted Gauntlet Model",
                "root_path": str(self.idea_projects_root / "neural_networks"),
                "repo_name": "neural_networks",
                "keep_in_place": True,
                "qdrant_collections": [
                    "neural_networks_code_corpus",
                    "code_corpus_temporal_index",
                    "portfolio_neural_benchmarks",
                ],
                "layers": {
                    "design": str(
                        self.idea_projects_root
                        / "neural_networks"
                        / "promoted"
                        / "promoted_manifest.json"
                    ),
                    "compiler": str(
                        self.idea_projects_root
                        / "neural_networks"
                        / "src"
                        / "models"
                        / "stage_compiler"
                        / "stage_compiler_model.py"
                    ),
                    "code": str(
                        self.idea_projects_root / "neural_networks" / "promoted" / "harness.py"
                    ),
                },
                "entrypoints": [
                    "neural_networks/promoted/harness.py",
                    "neural_networks/promoted/promoted_model.py",
                    "neural_networks/src/models/stage_compiler/stage_compiler_model.py",
                    "neural_networks/src/models/monadic_cavity/monadic_cavity_models.py",
                ],
            },
            {
                "system_id": "large_tools",
                "name": "StageCompiler Stage-0/Stage-1 Compiler, Monadic Cavity vCPU & Hypoborea VM",
                "root_path": str(self.idea_projects_root / "large_tools"),
                "repo_name": "large_tools",
                "keep_in_place": True,
                "qdrant_collections": [INFORMATIONAL_COLLECTION_NAME],
                "layers": {
                    "design": str(self.idea_projects_root / "large_tools" / "PROJECT.md"),
                    "compiler": str(
                        self.idea_projects_root
                        / "large_tools"
                        / "stage-compiler"
                        / "stage1"
                        / "main.sig"
                    ),
                    "code": str(
                        self.idea_projects_root
                        / "large_tools"
                        / "stage_compiler"
                        / "core"
                        / "vcpu"
                        / "monadic_cavity.sig"
                    ),
                },
                "entrypoints": [
                    "large_tools/PROJECT.md",
                    "large_tools/stage-compiler/stage0/compiler_v2.py",
                    "large_tools/stage-compiler/stage1/main.sig",
                    "large_tools/stage-compiler/macho_synthesizer.py",
                    "large_tools/stage-runtime/core/vcpu/monadic_cavity.sig",
                ],
            },
            {
                "system_id": "StageCompiler-development",
                "name": "StageCompiler Development Workspace (StageCompiler, StageCompiler-compiler, Hyperborea, Hyperborea-vm)",
                "root_path": str(self.idea_projects_root / "StageCompiler-development"),
                "repo_name": "StageCompiler-development",
                "keep_in_place": True,
                "qdrant_collections": [INFORMATIONAL_COLLECTION_NAME],
                "layers": {
                    "design": str(self.idea_projects_root / "StageCompiler-development" / "AGENTS.md"),
                    "compiler": str(
                        self.idea_projects_root / "StageCompiler-development" / "StageCompiler-compiler"
                    ),
                    "code": str(self.idea_projects_root / "StageCompiler-development" / "StageCompiler"),
                },
                "entrypoints": [
                    "StageCompiler-development/AGENTS.md",
                    "StageCompiler-development/settings.yaml",
                ],
            },
            {
                "system_id": "qdrant_staging",
                "name": "Unified Permanent Qdrant Staging & Cross-Environment Sync Hub",
                "root_path": str(self.staging_root),
                "repo_name": "qdrant_staging",
                "keep_in_place": True,
                "qdrant_collections": sorted(unified_colls.keys()),
                "layers": {
                    "design": str(self.staging_manager.manifest_path),
                    "compiler": str(self.staging_manager.ledger_path),
                    "code": str(self.staging_manager.storage_dir),
                },
                "entrypoints": [
                    "qdrant_staging/staging_manifest.json",
                    "qdrant_staging/informational_graph_ledger.json",
                    "qdrant_staging/storage/meta.json",
                ],
            },
        ]

        systems: List[Dict[str, Any]] = []
        for spec in system_catalog_specs:
            root_p = Path(spec["root_path"])
            exists = root_p.exists()
            colls_detail: List[Dict[str, Any]] = []
            total_sys_points = 0
            for cname in spec["qdrant_collections"]:
                cinfo = unified_colls.get(cname, {})
                pts = int(cinfo.get("staged_points", 0))
                total_sys_points += pts
                if include_collections:
                    colls_detail.append(
                        {
                            "collection_name": cname,
                            "staged_points": pts,
                            "vector_size": cinfo.get("vector_size", DEFAULT_VECTOR_SIZE),
                            "staging_sqlite_path": cinfo.get("staging_sqlite_path", ""),
                        }
                    )

            entry: Dict[str, Any] = {
                "system_id": spec["system_id"],
                "name": spec["name"],
                "root_path": spec["root_path"],
                "exists": exists,
                "keep_in_place": spec["keep_in_place"],
                "juxtaposed_nodes_count": int(node_counts_by_repo.get(spec["repo_name"], 0)),
                "total_qdrant_points": total_sys_points,
                "layers": spec["layers"],
                "entrypoints": spec["entrypoints"],
            }
            if include_collections:
                entry["collections"] = colls_detail
            if include_lifecycle:
                entry["lifecycle_status"] = "READY" if exists else "MISSING"
                entry["supported_lifecycle_actions"] = [
                    "status",
                    "build",
                    "rebuild",
                    "sync",
                    "verify",
                    "promote_harness",
                    "snapshot",
                ]
            systems.append(entry)

        return {
            "total_systems": len(systems),
            "staging_root": str(self.staging_root),
            "total_unified_collections": overview.get("total_collections", len(unified_colls)),
            "total_staged_points": overview.get("total_staged_points", 0),
            "systems": systems,
        }

    def search_qdrant_unified(
        self,
        query: str = "",
        collection: Optional[str] = None,
        system_id: Optional[str] = None,
        layer: Optional[str] = None,
        node_type: Optional[str] = None,
        top_k: int = 15,
        rejoin_chunks: bool = True,
    ) -> Dict[str, Any]:
        """
        Searches across any or all unified Qdrant collections in `/Users/jarek/IdeaProjects/qdrant_staging/storage`
        (`agent_informational_graph_index`, `portfolio_wiki_chunks`, `portfolio_neural_benchmarks`,
        `portfolio_antigravity_brain`, `neural_networks_code_corpus`, `code_corpus_temporal_index`),
        optionally filtering by `collection`, `system_id`, `layer` (`design` | `compiler` | `code` | `ui` | `index` | `model`),
        and `node_type`, rejoining multi-chunk documents and attaching Design + Compiler + Code juxtaposition context.
        """
        q_clean = mask_restricted_string((query or "").strip())
        system_id = mask_restricted_string(system_id) if system_id else system_id
        q_vec = _deterministic_384d_vector(q_clean or "architecture", dim=DEFAULT_VECTOR_SIZE)
        q_tokens = [t for t in re.findall(r"[a-zA-Z0-9_]{2,}", q_clean.lower())]

        staging_meta = self.staging_manager._load_meta_json(self.staging_manager.storage_dir)
        all_colls = sorted(staging_meta.get("collections", {}).keys())
        if collection and collection != "all":
            target_colls = [c for c in all_colls if c == collection]
        else:
            # Prioritize informational graph, benchmarks, wiki, brain, and code corpus
            target_colls = all_colls

        hits: List[Tuple[float, Dict[str, Any]]] = []

        # 1. Search `agent_informational_graph_index` via in-memory + Qdrant index
        if not collection or collection in ("all", INFORMATIONAL_COLLECTION_NAME):
            jux_res = self.query_juxtaposed_map(
                query=q_clean,
                node_type=node_type,
                repo_name=system_id if system_id and system_id != "all" else None,
                top_k=max(top_k * 2, 30),
            )
            for item in jux_res.get("results", []):
                item_layer = self._classify_node_layer(item)
                if layer and layer != "all" and item_layer != layer:
                    continue
                tri = self._build_tri_layer_juxtaposition_for_node(item)
                sc = float(item.get("score", 0.5)) + 0.15
                hits.append(
                    (
                        round(sc, 4),
                        {
                            "collection": INFORMATIONAL_COLLECTION_NAME,
                            "point_id": item.get("point_id", ""),
                            "id": item.get("node_id", ""),
                            "title": item.get("title", ""),
                            "idiomatic_name": item.get("idiomatic_name", ""),
                            "system_id": item.get("repo_name", ""),
                            "layer": item_layer,
                            "node_type": item.get("node_type", ""),
                            "score": round(sc, 4),
                            "keep_in_place": item.get("keep_in_place", True),
                            "poincare_coords": item.get("poincare_coords", []),
                            "juxtaposition": tri,
                            "payload": item,
                        },
                    )
                )

        # 2. Search other requested unified Qdrant collections in `/Users/jarek/IdeaProjects/qdrant_staging/storage`
        other_colls = [c for c in target_colls if c != INFORMATIONAL_COLLECTION_NAME]
        for coll_name in other_colls:
            # Cap scan on 5,300+ point collections for sub-second response
            scan_limit = 600 if "code_corpus" in coll_name else 1000
            pts = self.staging_manager.read_points_lockfree(
                self.staging_manager.storage_dir, coll_name, limit=scan_limit
            )
            grouped_by_doc: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
            for pt in pts:
                payload = dict(pt.payload or {})
                doc_key = str(
                    payload.get("doc_id")
                    or payload.get("model_id")
                    or payload.get("entry_id")
                    or payload.get("rel_path")
                    or pt.id
                )
                grouped_by_doc[doc_key].append(
                    {
                        "point_id": str(pt.id),
                        "vector": list(pt.vector) if isinstance(pt.vector, list) else [],
                        "payload": payload,
                    }
                )

            for doc_key, chunk_list in grouped_by_doc.items():
                chunk_list.sort(key=lambda c: int(c["payload"].get("chunk_index", 0)))
                rep = chunk_list[0]["payload"]
                if system_id and system_id not in ("all", ""):
                    sys_match = (
                        system_id.lower() in coll_name.lower()
                        or system_id.lower() in str(rep.get("repo_name", "")).lower()
                        or system_id.lower() in str(rep.get("source_file", "")).lower()
                        or system_id.lower() in str(rep.get("rel_path", "")).lower()
                    )
                    if not sys_match:
                        continue

                rejoined_text = (
                    "\n".join(
                        str(
                            c["payload"].get("chunk_text")
                            or c["payload"].get("text")
                            or c["payload"].get("content")
                            or c["payload"].get("snippet")
                            or ""
                        )
                        for c in chunk_list
                    ).strip()
                    if rejoin_chunks
                    else str(
                        rep.get("chunk_text")
                        or rep.get("text")
                        or rep.get("content")
                        or rep.get("snippet")
                        or ""
                    )
                )
                searchable = (
                    f"{doc_key} {rep.get('title', '')} {rep.get('model_name', '')} "
                    f"{rep.get('class_name', '')} {rep.get('rel_path', '')} "
                    f"{rep.get('source_file', '')} {rejoined_text[:1600]}"
                ).lower()

                if not q_clean:
                    score = 0.5
                else:
                    lex_hits = sum(1 for tok in q_tokens if tok in searchable)
                    if lex_hits == 0 and q_clean.lower() not in searchable:
                        continue
                    vec = chunk_list[0]["vector"]
                    cos = (
                        sum(a * b for a, b in zip(q_vec, vec))
                        if len(vec) == len(q_vec)
                        else 0.25
                    )
                    exact_boost = 0.8 if q_clean.lower() in searchable else 0.0
                    score = round(
                        float(cos * 0.4 + (lex_hits / max(1, len(q_tokens))) * 0.6 + exact_boost),
                        4,
                    )

                inferred_layer = (
                    "model"
                    if "neural_benchmarks" in coll_name
                    else "compiler"
                    if "compiler" in searchable
                    else "design"
                    if "wiki" in coll_name
                    else "code"
                )
                if layer and layer != "all" and inferred_layer != layer:
                    continue

                sys_inferred = (
                    "neural_networks"
                    if "neural" in coll_name or "code_corpus" in coll_name
                    else "portfolio"
                )
                hits.append(
                    (
                        score,
                        {
                            "collection": coll_name,
                            "point_id": chunk_list[0]["point_id"],
                            "id": doc_key,
                            "title": str(
                                rep.get("title")
                                or rep.get("model_name")
                                or rep.get("rel_path")
                                or doc_key
                            ),
                            "idiomatic_name": str(
                                rep.get("class_name")
                                or rep.get("module_name")
                                or rep.get("rel_path")
                                or doc_key
                            ),
                            "system_id": sys_inferred,
                            "layer": inferred_layer,
                            "node_type": str(rep.get("doc_type") or rep.get("entry_type") or coll_name),
                            "score": score,
                            "chunks_rejoined": len(chunk_list) if rejoin_chunks else 1,
                            "rejoined_text_excerpt": rejoined_text[:900],
                            "juxtaposition": self._build_tri_layer_juxtaposition_for_node(
                                {
                                    "node_id": doc_key,
                                    "repo_name": sys_inferred,
                                    "title": str(rep.get("title") or rep.get("model_name") or doc_key),
                                }
                            ),
                            "payload": rep,
                        },
                    )
                )

        hits.sort(key=lambda x: x[0], reverse=True)
        top_results = [h[1] for h in hits[: max(1, int(top_k))]]
        return {
            "query": q_clean,
            "collection_filter": collection or "all",
            "system_filter": system_id or "all",
            "layer_filter": layer or "all",
            "total_matches": len(hits),
            "results": top_results,
            "staging_root": str(self.staging_root),
        }

    def get_design_compiler_code_juxtaposition(
        self,
        query: str,
        system_id: Optional[str] = None,
        include_geodesic_trace: bool = True,
    ) -> Dict[str, Any]:
        """
        Returns the tri-layer juxtaposition context for **Design**, **Compiler**, and **Code**
        matching `query` (and optional `system_id`), alongside the full start-to-finish
        geodesic code path connecting them.
        """
        q_clean = mask_restricted_string((query or "").strip()) or "PolyphonicStageCompilerCascadeModel"
        system_id = mask_restricted_string(system_id) if system_id else system_id
        search_res = self.query_juxtaposed_map(
            query=q_clean,
            repo_name=system_id if system_id and system_id != "all" else None,
            top_k=8,
        )
        matches = search_res.get("results", [])
        if not matches and system_id:
            matches = self.query_juxtaposed_map(query=q_clean, top_k=8).get("results", [])

        primary_node = (
            matches[0]
            if matches
            else (
                self.index.load_from_index("nodes", "compiler:stage_compiler:stage1_main")
                or self.index.load_from_index("nodes", "staging:qdrant_staging")
                or {}
            )
        )
        tri_juxtaposition = self._build_tri_layer_juxtaposition_for_node(primary_node)

        # Also collect top Design, Compiler, and Code nodes specifically matching or linked to the query
        design_matches = self.query_juxtaposed_map(
            query=q_clean, node_type="design_spec", top_k=3
        ).get("results", [])
        if not design_matches:
            design_matches = self.query_juxtaposed_map(
                query="project_master_spec", node_type="design_spec", top_k=3
            ).get("results", [])

        compiler_matches = self.query_juxtaposed_map(
            query=q_clean, node_type="compiler_stage", top_k=4
        ).get("results", [])
        if not compiler_matches:
            compiler_matches = self.query_juxtaposed_map(
                query="stage1", node_type="compiler_stage", top_k=4
            ).get("results", [])

        code_matches = [
            m
            for m in matches
            if m.get("node_type")
            in (
                "runtime_code",
                "promoted_model",
                "neural_architecture_package",
                "portfolio_service",
                "toolset_index",
                "toolset_module",
                "ui_component",
            )
        ][:4]

        geodesic_path = (
            self.trace_full_geodesic_code_path(
                start_query=q_clean,
                target_query="",
                system_id=system_id,
                include_source_excerpts=True,
            )
            if include_geodesic_trace
            else {}
        )

        return {
            "query": q_clean,
            "system_id": system_id or primary_node.get("repo_name", "all"),
            "anchor_node": {
                "node_id": primary_node.get("node_id", ""),
                "title": primary_node.get("title", ""),
                "node_type": primary_node.get("node_type", ""),
                "repo_name": primary_node.get("repo_name", ""),
                "layer": self._classify_node_layer(primary_node),
                "poincare_coords": primary_node.get("poincare_coords", []),
            },
            "juxtaposition": tri_juxtaposition,
            "design_layer_nodes": [
                {
                    "node_id": d.get("node_id"),
                    "title": d.get("title"),
                    "doc_path": (d.get("juxtaposed_code") or {}).get("code_path"),
                    "excerpt": (d.get("juxtaposed_docs") or {}).get("doc_excerpt", "")[:450],
                }
                for d in design_matches
            ],
            "compiler_layer_nodes": [
                {
                    "node_id": c.get("node_id"),
                    "title": c.get("title"),
                    "code_path": (c.get("juxtaposed_code") or {}).get("code_path"),
                    "exported_symbols": (c.get("juxtaposed_code") or {}).get("exported_symbols", []),
                    "excerpt": (c.get("juxtaposed_code") or {}).get("code_excerpt", "")[:450],
                }
                for c in compiler_matches
            ],
            "code_layer_nodes": [
                {
                    "node_id": r.get("node_id"),
                    "title": r.get("title"),
                    "code_path": (r.get("juxtaposed_code") or {}).get("code_path"),
                    "exported_symbols": (r.get("juxtaposed_code") or {}).get("exported_symbols", []),
                    "excerpt": (r.get("juxtaposed_code") or {}).get("code_excerpt", "")[:450],
                }
                for r in code_matches
            ],
            "geodesic_code_path": geodesic_path,
        }

    def trace_full_geodesic_code_path(
        self,
        start_query: str = "",
        target_query: str = "",
        system_id: Optional[str] = None,
        include_source_excerpts: bool = True,
    ) -> Dict[str, Any]:
        """
        Computes the full start-to-finish Poincaré hyperbolic geodesic code path:
        - When both `start_query` and `target_query` are provided, traces the shortest
          geodesic path from `start_query` to `target_query` and enriches every hop with
          its Design + Compiler + Code juxtaposition context and hyperbolic geometry.
        - When only `start_query` (or `system_id` / empty) is provided, traces the complete
          Start-to-Finish End-to-End Execution Geodesic Path across all 7 architectural phases
          (1_DESIGN -> 2_COMPILER_BOOTSTRAP_AND_INGRESS -> 3_COMPILER_PARSER_AND_IR ->
           4_COMPILER_EMIT_AND_SYNTHESIS -> 5_RUNTIME_VCPU_AND_NEURAL_MODEL ->
           6_HARNESS_LEDGER_AND_SERVICE -> 7_UI_STREAMING_PRESENTATION), anchored through
          the queried node if not already in the canonical spine.
        """
        canonical_spine_ids = [
            "design:large_tools:project_master_spec",
            "design:hypoborea_vm:vcpu_microloop_spec",
            "design:hypoborea_vm:polyphonic_cohesive_rns_register_spec",
            "compiler:stage_compiler:stage0_bootstrap",
            "compiler:stage_compiler:stage1_main",
            "compiler:stage_compiler:stage1_ingress",
            "compiler:stage_compiler:stage1_parser",
            "compiler:stage_compiler:stage1_symbols",
            "compiler:stage_compiler:stage1_mcc_ir",
            "compiler:stage_compiler:stage1_emitter",
            "compiler:stage_compiler:stage1_linker",
            "compiler:stage_compiler:macho_synthesizer",
            "code:stage_compiler:core_vcpu_monadic_cavity",
            "code:stage_compiler:core_vcpu_cohesive_continuum",
            "code:neural_networks:implicit_vcpu_layer",
            "model_pkg:neural_networks:stage_compiler",
            "model_pkg:neural_networks:monadic_cavity",
            "code:neural_networks:promoted_harness",
            "module:toolset:benchmark_ledger",
            "service:portfolio:benchmark_service",
            "service:portfolio:portfolio_api",
            "ui:portfolio:PromotedModelSection",
            "ui:components:AsyncChatWindow",
        ]

        start_clean = mask_restricted_string((start_query or "").strip())
        target_clean = mask_restricted_string((target_query or "").strip())
        system_id = mask_restricted_string(system_id) if system_id else system_id

        ordered_node_ids: List[str] = []
        edge_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for s_id in canonical_spine_ids:
            for e in self.index.load_from_index("edges_by_node", s_id) or []:
                if isinstance(e, dict) and e.get("source") and e.get("target"):
                    edge_lookup[(str(e["source"]), str(e["target"]))] = e

        if start_clean and target_clean:
            wf = self.trace_wayfinding_path(start_clean, target_clean)
            wf_hops = [str(h["node_id"]) for h in wf.get("hops", [])] if wf.get("found") else []
            s_resolved = start_clean if start_clean in canonical_spine_ids else (wf_hops[0] if wf_hops else "")
            t_resolved = target_clean if target_clean in canonical_spine_ids else (wf_hops[-1] if wf_hops else "")
            if s_resolved in canonical_spine_ids and t_resolved in canonical_spine_ids:
                s_idx = canonical_spine_ids.index(s_resolved)
                t_idx = canonical_spine_ids.index(t_resolved)
                if s_idx <= t_idx:
                    ordered_node_ids = canonical_spine_ids[s_idx : t_idx + 1]
                else:
                    ordered_node_ids = list(reversed(canonical_spine_ids[t_idx : s_idx + 1]))
            elif wf_hops:
                ordered_node_ids = wf_hops
                for e in wf.get("edges", []):
                    if isinstance(e, dict) and e.get("source") and e.get("target"):
                        edge_lookup[(str(e["source"]), str(e["target"]))] = e

        if not ordered_node_ids:
            # Build full start-to-finish spine, ensuring any queried node is included at its proper phase
            ordered_node_ids = [
                nid
                for nid in canonical_spine_ids
                if isinstance(self.index.load_from_index("nodes", nid), dict)
            ]
            if start_clean:
                matched = self.query_juxtaposed_map(
                    query=start_clean,
                    repo_name=system_id if system_id and system_id != "all" else None,
                    top_k=1,
                ).get("results", [])
                if matched:
                    m_id = str(matched[0]["node_id"])
                    if m_id not in ordered_node_ids:
                        m_layer = self._classify_node_layer(matched[0])
                        insert_idx = len(ordered_node_ids) - 2 if len(ordered_node_ids) >= 2 else len(ordered_node_ids)
                        if m_layer == "design":
                            insert_idx = min(3, len(ordered_node_ids))
                        elif m_layer == "compiler":
                            insert_idx = min(11, len(ordered_node_ids))
                        ordered_node_ids.insert(insert_idx, m_id)

        stage_phase_labels = {
            "design": "1_DESIGN_SPECIFICATION",
            "compiler": "2_COMPILER_PIPELINE",
            "model": "3_NEURAL_COMPILER_AND_CAVITY_MODEL",
            "code": "4_RUNTIME_CODE_AND_SERVICE",
            "index": "5_QDRANT_AND_TOOLSET_INDEX",
            "ui": "6_UI_STREAMING_PRESENTATION",
        }

        hops: List[Dict[str, Any]] = []
        cumulative_geo = 0.0
        prev_coords: Optional[List[float]] = None

        for idx, nid in enumerate(ordered_node_ids):
            node_obj = self.index.load_from_index("nodes", nid)
            if not isinstance(node_obj, dict):
                continue
            coords = list(node_obj.get("poincare_coords") or [])
            step_geo = self._poincare_distance(prev_coords, coords) if prev_coords is not None else 0.0
            midpoint = (
                self._poincare_midpoint(prev_coords, coords)
                if prev_coords is not None
                else coords
            )
            cumulative_geo = round(cumulative_geo + step_geo, 6)
            prev_coords = coords

            prev_nid = ordered_node_ids[idx - 1] if idx > 0 else ""
            edge_info = edge_lookup.get((prev_nid, nid), {}) if prev_nid else {}
            layer_name = self._classify_node_layer(node_obj)
            code_meta = dict(node_obj.get("juxtaposed_code") or {})
            doc_meta = dict(node_obj.get("juxtaposed_docs") or {})
            tri_jux = self._build_tri_layer_juxtaposition_for_node(node_obj)

            hop_entry: Dict[str, Any] = {
                "hop_index": idx,
                "stage_phase": stage_phase_labels.get(layer_name, "4_RUNTIME_CODE_AND_SERVICE"),
                "layer": layer_name,
                "node_id": nid,
                "title": node_obj.get("title", ""),
                "idiomatic_name": node_obj.get("idiomatic_name", ""),
                "repo_name": node_obj.get("repo_name", ""),
                "node_type": node_obj.get("node_type", ""),
                "file_path": code_meta.get("code_path", ""),
                "relative_path": code_meta.get("relative_path", ""),
                "language": code_meta.get("language", ""),
                "exported_symbols": code_meta.get("exported_symbols", []),
                "sha256": code_meta.get("sha256", ""),
                "doc_path": doc_meta.get("doc_path", ""),
                "qdrant_namespace": node_obj.get("qdrant_namespace", self.collection_name),
                "keep_in_place": bool(node_obj.get("keep_in_place", True)),
                "poincare_coords": coords,
                "step_geodesic_distance": step_geo,
                "cumulative_geodesic_distance": cumulative_geo,
                "poincare_midpoint": midpoint,
                "incoming_edge_relation": edge_info.get("relation", "PIPELINE_START" if idx == 0 else "GEODESIC_NEXT_HOP"),
                "transition_rationale": edge_info.get(
                    "rationale",
                    "Entry point of the Design -> Compiler -> Code geodesic pipeline"
                    if idx == 0
                    else f"Geodesic transition from {prev_nid} to {nid}",
                ),
                "juxtaposed_context": tri_jux,
            }
            if include_source_excerpts:
                hop_entry["code_excerpt"] = code_meta.get("code_excerpt", "")[:650]
                hop_entry["doc_excerpt"] = doc_meta.get("doc_excerpt", "")[:500]
            hops.append(hop_entry)

        start_coords = hops[0]["poincare_coords"] if hops else []
        end_coords = hops[-1]["poincare_coords"] if hops else []
        direct_geo = self._poincare_distance(start_coords, end_coords) if len(hops) >= 2 else 0.0

        return {
            "found": len(hops) > 0,
            "start_query": start_clean or "design:large_tools:project_master_spec",
            "target_query": target_clean or "ui:components:AsyncChatWindow",
            "start_node_id": hops[0]["node_id"] if hops else "",
            "target_node_id": hops[-1]["node_id"] if hops else "",
            "hop_count": len(hops),
            "total_geodesic_distance": cumulative_geo,
            "direct_geodesic_distance": direct_geo,
            "layers_traversed": list(dict.fromkeys(h["layer"] for h in hops)),
            "repositories_traversed": list(dict.fromkeys(h["repo_name"] for h in hops)),
            "hops": hops,
        }

    def manage_system_lifecycle(
        self,
        action: str = "status",
        system_id: str = "all",
        target_environment: Optional[str | Path] = None,
        direction: str = "bidirectional",
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Manages the full lifecycle across all systems (`portfolio`, `components`, `toolset`,
        `neural_networks`, `large_tools`, `StageCompiler-development`, `qdrant_staging`):
        - `status`: Returns system catalog, Qdrant staging telemetry, and promoted harness state.
        - `build` / `rebuild`: Rebuilds the juxtaposed Design+Compiler+Code informational graph
          and unifies all Qdrant collections into `/Users/jarek/IdeaProjects/qdrant_staging`.
        - `sync`: Synchronizes Qdrant collections across repo-local stores, staging, or an external environment.
        - `verify`: Verifies SHA-256 code integrity (`CodeCorpusIndex.verify_code_unchanged`),
          chunk rejoin integrity across Qdrant collections, and `keep_in_place` invariants.
        - `promote_harness`: Executes lifecycle actions on `PromotedModelHarness` (`status`, `learn`,
          `generate`, `memorize`, `reset_memory`, `predict_next`).
        - `snapshot`: Exports portable JSONL + manifest snapshots to `/Users/jarek/IdeaProjects/qdrant_staging/snapshots`.
        """
        t0 = time.perf_counter()
        norm_action = (action or "status").strip().lower()
        system_id = mask_restricted_string(system_id) if system_id else "all"
        opts = dict(options or {})

        if norm_action in ("build", "rebuild", "rebuild_index"):
            telemetry = self.build_and_unify(force_rebuild=True, export_snapshots=True)
            return {
                "action": norm_action,
                "system_id": system_id,
                "status": "COMPLETED",
                "telemetry": telemetry,
                "systems": self.list_systems(include_collections=True, include_lifecycle=True)["systems"],
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        if norm_action == "sync":
            sync_res = self.sync_staging(
                target_environment=target_environment,
                direction=direction,
                collections=opts.get("collections"),
                rebuild_graph=bool(opts.get("rebuild_graph", True)),
            )
            return {
                "action": "sync",
                "system_id": system_id,
                "status": "COMPLETED",
                "sync_result": sync_res["sync_result"],
                "staging_overview": sync_res["staging_overview"],
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        if norm_action == "snapshot":
            manifest = self.staging_manager.unify_all_collections(export_snapshots=True)
            return {
                "action": "snapshot",
                "system_id": system_id,
                "status": "COMPLETED",
                "snapshots_dir": str(self.staging_manager.snapshots_dir),
                "bundle_manifest": str(self.staging_manager.snapshots_dir / "staging_bundle_manifest.json"),
                "informational_snapshot": str(
                    self.staging_manager.snapshots_dir / f"{INFORMATIONAL_COLLECTION_NAME}.jsonl"
                ),
                "total_collections": manifest.get("total_collections", 0),
                "total_staged_points": manifest.get("total_staged_points", 0),
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        if norm_action == "verify":
            # 1. Verify placement invariants (`keep_in_place=True`)
            all_nodes = [v for _, v in self.index.range_index("nodes") if isinstance(v, dict)]
            if not all_nodes:
                self.build_and_unify(force_rebuild=False)
                all_nodes = [v for _, v in self.index.range_index("nodes") if isinstance(v, dict)]
            missing_paths: List[str] = []
            verified_paths = 0
            for n in all_nodes:
                cp = str((n.get("juxtaposed_code") or {}).get("code_path", ""))
                if cp and Path(cp).exists():
                    verified_paths += 1
                elif cp:
                    missing_paths.append(cp)

            # 2. Verify neural_networks CodeCorpusIndex SHA-256 integrity if available
            nn_repo = self.idea_projects_root / "neural_networks"
            nn_ledger = nn_repo / "src" / "models" / "monadic_cavity" / "results" / "code_corpus_ledger.json"
            nn_qdrant = nn_repo / ".qdrant_code_index"
            code_verification: Dict[str, Any] = {"available": False}
            if nn_repo.is_dir():
                try:
                    from indexes.code_index.code_index import CodeCorpusIndex

                    c_idx = CodeCorpusIndex(
                        repo_path=nn_repo,
                        qdrant_path=nn_qdrant if nn_qdrant.is_dir() else None,
                        collection_name="neural_networks_code_corpus",
                        ledger_path=nn_ledger if nn_ledger.is_file() else None,
                    )
                    code_verification = c_idx.verify_code_unchanged(
                        subdirs=["src/models/monadic_cavity", "src/models/stage_compiler", "promoted"]
                    )
                    code_verification["available"] = True
                except Exception as exc:
                    code_verification = {"available": False, "error": str(exc)}

            return {
                "action": "verify",
                "system_id": system_id,
                "status": "VERIFIED" if len(missing_paths) == 0 else "PARTIAL",
                "verified_node_paths_count": verified_paths,
                "missing_node_paths": missing_paths,
                "keep_in_place_invariants_valid": len(missing_paths) == 0,
                "code_corpus_sha256_verification": code_verification,
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        if norm_action in ("promote_harness", "harness"):
            nn_root = self.idea_projects_root / "neural_networks"
            harness_result: Dict[str, Any] = {"available": False}
            if (nn_root / "promoted" / "harness.py").is_file():
                import sys

                for p_cand in (str(nn_root), str(nn_root / "src")):
                    if p_cand not in sys.path:
                        sys.path.insert(0, p_cand)
                import promoted.harness as h_mod

                harness = h_mod.build_promoted_harness(vocab_size=256, d_model=128, device="cpu")
                sub_action = str(opts.get("harness_action", "status")).lower()
                prompt_text = str(
                    opts.get("prompt")
                    or opts.get("text")
                    or "def execute_monadic_cycle(cavity, wave):"
                )
                if sub_action == "learn":
                    harness_result = harness.learn(prompt_text, steps=int(opts.get("steps", 2)))
                elif sub_action == "generate":
                    harness_result = {
                        "generated": harness.generate(
                            prompt_text,
                            max_new_tokens=int(opts.get("max_new_tokens", 24)),
                        )
                    }
                elif sub_action == "memorize":
                    harness_result = harness.memorize(prompt_text)
                elif sub_action == "reset_memory":
                    harness_result = harness.reset_memory()
                elif sub_action == "predict_next":
                    harness_result = {"predictions": harness.predict_next(prompt_text, top_k=5)}
                else:
                    harness_result = harness.status()
                if not isinstance(harness_result, dict):
                    harness_result = {"result": harness_result}
                harness_result["available"] = True
            return {
                "action": "promote_harness",
                "system_id": "neural_networks",
                "status": "COMPLETED" if harness_result.get("available") else "UNAVAILABLE",
                "harness_result": harness_result,
                "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            }

        # Default: `status` / `health_check`
        sys_catalog = self.list_systems(include_collections=True, include_lifecycle=True)
        systems_filtered = (
            [s for s in sys_catalog["systems"] if s["system_id"] == system_id]
            if system_id and system_id != "all"
            else sys_catalog["systems"]
        )
        return {
            "action": "status",
            "system_id": system_id,
            "status": "READY",
            "staging_overview": self.get_staging_overview(),
            "systems": systems_filtered,
            "audit_log_tail": list(self.audit_log.all())[-10:],
            "wall_time_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }

    # =========================================================================
    # Wiki File System Tree, Git-Like Code Viewer & Line-Level Geodesic Trace
    # =========================================================================

    _STOP_IDENTIFIERS: set[str] = {
        "def",
        "class",
        "return",
        "import",
        "from",
        "const",
        "let",
        "var",
        "export",
        "interface",
        "type",
        "function",
        "async",
        "await",
        "true",
        "false",
        "True",
        "False",
        "None",
        "null",
        "undefined",
        "self",
        "cls",
        "str",
        "int",
        "float",
        "bool",
        "dict",
        "list",
        "tuple",
        "set",
        "Any",
        "Optional",
        "Dict",
        "List",
        "Tuple",
        "Set",
        "Union",
        "for",
        "while",
        "elif",
        "else",
        "try",
        "except",
        "finally",
        "with",
        "pass",
        "continue",
        "break",
        "and",
        "not",
        "isinstance",
        "len",
        "max",
        "min",
        "sum",
    }

    def _ensure_nodes_loaded(self) -> List[Dict[str, Any]]:
        all_nodes = [v for _, v in self.index.range_index("nodes") if isinstance(v, dict)]
        if not all_nodes:
            self.build_and_unify(force_rebuild=False)
            all_nodes = [v for _, v in self.index.range_index("nodes") if isinstance(v, dict)]
        return all_nodes

    def get_wiki_filesystem_tree(self) -> Dict[str, Any]:
        """
        Builds a nested FileSystemRootNode tree across all 7 systems so the reusable
        `FileSystemExplorer` component can browse Design specs, Stage-0/Stage-1 Compiler
        files, Promoted Neural Models, Toolset packages, Portfolio services, and UI components.
        """
        all_nodes = self._ensure_nodes_loaded()

        system_order = [
            ("large_tools", "Large Tools (Design & Stage-0/1 Compiler)"),
            ("neural_networks", "Neural Networks (Promoted Model & Compiler)"),
            ("toolset", "Toolset (38 Python Libraries & Indexes)"),
            ("portfolio", "Portfolio (Services, API & Code Wiki UI)"),
            ("components", "Components (Reusable Meta-Components)"),
            ("StageCompiler-development", "StageCompiler Development Specs"),
            ("qdrant_staging", "Qdrant Staging & Knowledge Items"),
        ]
        layer_folder_meta = {
            "design": ("design", "Design Specifications"),
            "compiler": ("compiler", "Compiler Pipeline"),
            "model": ("models", "Promoted & Neural Models"),
            "code": ("runtime", "Runtime & Services"),
            "index": ("indexes", "Qdrant & Toolset Indexes"),
            "ui": ("ui", "UI & Meta-Components"),
        }

        by_system_folders: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
            sys_id: {} for sys_id, _ in system_order
        }
        seen_paths_per_system: Dict[str, set[str]] = {
            sys_id: set() for sys_id, _ in system_order
        }

        default_file_path = ""
        default_node_id = ""

        for node in all_nodes:
            repo_name = str(node.get("repo_name") or "toolset")
            if repo_name not in by_system_folders:
                by_system_folders[repo_name] = {}
                seen_paths_per_system[repo_name] = set()

            layer = self._classify_node_layer(node)
            folder_rel, _ = layer_folder_meta.get(layer, ("runtime", "Runtime & Services"))
            code_meta = dict(node.get("juxtaposed_code") or {})
            doc_meta = dict(node.get("juxtaposed_docs") or {})

            candidates = [
                (
                    str(code_meta.get("code_path") or ""),
                    str(code_meta.get("relative_path") or ""),
                    layer,
                )
            ]
            doc_p = str(doc_meta.get("doc_path") or "")
            if doc_p and doc_p != candidates[0][0]:
                candidates.append((doc_p, Path(doc_p).name, "design"))

            for raw_path, rel_path, item_layer in candidates:
                if not raw_path or raw_path in seen_paths_per_system[repo_name]:
                    continue
                p_obj = Path(raw_path)
                if not p_obj.is_file():
                    continue
                seen_paths_per_system[repo_name].add(raw_path)

                f_rel, _ = layer_folder_meta.get(item_layer, (folder_rel, "Runtime & Services"))
                try:
                    st = p_obj.stat()
                    size_bytes = int(st.st_size)
                    mtime_iso = time.strftime("%Y-%m-%d %H:%M", time.localtime(st.st_mtime))
                except OSError:
                    size_bytes = 0
                    mtime_iso = ""

                file_item: Dict[str, Any] = {
                    "name": p_obj.name,
                    "friendly_name": str(node.get("title") or p_obj.name),
                    "title": str(node.get("title") or p_obj.name),
                    "path": str(p_obj),
                    "absolute_path": str(p_obj),
                    "relative_path": rel_path or p_obj.name,
                    "folder": f_rel,
                    "size_bytes": size_bytes,
                    "updated_at": mtime_iso,
                    "modified_at": mtime_iso,
                    "extension": p_obj.suffix.lstrip("."),
                    "file_type": item_layer,
                    "node_id": str(node.get("node_id") or ""),
                    "layer": item_layer,
                    "repo_name": repo_name,
                }
                by_system_folders[repo_name].setdefault(f_rel, []).append(file_item)

                if (
                    not default_file_path
                    and str(node.get("node_id") or "")
                    == "model:neural_networks:PolyphonicStageCompilerCascadeModel"
                    and p_obj.suffix == ".py"
                ):
                    default_file_path = str(p_obj)
                    default_node_id = str(node.get("node_id") or "")

        tree: List[Dict[str, Any]] = []
        total_files_count = 0

        for sys_id, sys_label in system_order:
            folder_map = by_system_folders.get(sys_id, {})
            if not folder_map:
                continue
            folders_list: List[Dict[str, Any]] = []
            sys_file_count = 0
            for layer_key in ("design", "compiler", "models", "runtime", "indexes", "ui"):
                files_in_folder = folder_map.get(layer_key, [])
                if not files_in_folder:
                    continue
                files_sorted = sorted(files_in_folder, key=lambda x: str(x["name"]).lower())
                friendly_folder = next(
                    (
                        lbl
                        for _, (f_k, lbl) in layer_folder_meta.items()
                        if f_k == layer_key
                    ),
                    layer_key.title(),
                )
                folders_list.append(
                    {
                        "folder_id": f"{sys_id}:{layer_key}",
                        "node_id": f"{sys_id}:{layer_key}",
                        "name": layer_key,
                        "friendly_name": friendly_folder,
                        "relative_path": layer_key,
                        "file_count": len(files_sorted),
                        "files": files_sorted,
                        "children": files_sorted,
                        "folders": [],
                    }
                )
                sys_file_count += len(files_sorted)
                if not default_file_path and files_sorted:
                    default_file_path = str(files_sorted[0]["path"])
                    default_node_id = str(files_sorted[0].get("node_id") or "")

            total_files_count += sys_file_count
            tree.append(
                {
                    "node_id": f"wiki_sys_{sys_id}",
                    "conversation_id": f"wiki_sys_{sys_id}",
                    "short_id": sys_id,
                    "friendly_name": sys_label,
                    "workspace_project": sys_id,
                    "path": str(self.idea_projects_root / sys_id),
                    "updated_at": "live",
                    "total_files": sys_file_count,
                    "artifact_count": sys_file_count,
                    "folders": folders_list,
                    "children": folders_list,
                    "root_files": [],
                }
            )

        return {
            "total_systems": len(tree),
            "total_files": total_files_count,
            "default_file_path": default_file_path,
            "default_node_id": default_node_id,
            "tree": tree,
        }

    def _resolve_node_and_path(
        self, file_path_or_query: str
    ) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
        all_nodes = self._ensure_nodes_loaded()
        clean = mask_restricted_string((file_path_or_query or "").strip())
        if not clean:
            tree_info = self.get_wiki_filesystem_tree()
            clean = str(tree_info.get("default_file_path") or "")

        # 1. Exact node_id match
        direct_node = self.index.load_from_index("nodes", clean)
        if isinstance(direct_node, dict):
            cp = str((direct_node.get("juxtaposed_code") or {}).get("code_path") or "")
            if cp and Path(cp).is_file():
                return Path(cp).absolute(), direct_node

        # 2. Direct file path match (preserve non-restricted symlinks via .absolute())
        cand_path = Path(clean)
        if not cand_path.is_absolute():
            for base in (self.idea_projects_root, self.idea_projects_root / "portfolio" / "portfolio"):
                if (base / clean).is_file():
                    cand_path = (base / clean).absolute()
                    break

        best_node: Optional[Dict[str, Any]] = None
        if cand_path.is_file():
            abs_str = mask_restricted_string(str(cand_path.absolute()))
            resolved_str = str(cand_path.resolve())
            for node in all_nodes:
                cp = str((node.get("juxtaposed_code") or {}).get("code_path") or "")
                dp = str((node.get("juxtaposed_docs") or {}).get("doc_path") or "")
                if cp in (abs_str, resolved_str):
                    best_node = node
                    break
                if dp in (abs_str, resolved_str) and best_node is None:
                    best_node = node
            if best_node is None:
                # Match by parent repo
                for node in all_nodes:
                    r_name = str(node.get("repo_name") or "")
                    if r_name and f"/{r_name}/" in abs_str:
                        best_node = node
                        break
            return Path(abs_str) if Path(abs_str).is_file() else cand_path.absolute(), best_node

        # 3. Search juxtaposed nodes by query / filename
        matched = self.query_juxtaposed_map(query=clean, top_k=1).get("results", [])
        if matched:
            node = matched[0]
            cp = str((node.get("juxtaposed_code") or {}).get("code_path") or "")
            if cp and Path(cp).is_file():
                return Path(cp).absolute(), node

        return None, None

    _BRANCH_KEYWORD_RE = re.compile(
        r"\b(if|elif|else|for|while|try|except|finally|with|match|case|switch|catch|return|yield|raise|await|def|class|stage|Stage|export)\b"
    )

    def _detect_branch_keyword(self, line_text: str) -> Tuple[bool, Optional[str]]:
        """
        Determines whether `line_text` contains a branching / control-flow / dispatch keyword
        that can expand into a cascading code-flow node. Returns `(is_branch, branch_keyword)`.
        """
        stripped = (line_text or "").strip()
        if not stripped:
            return False, None
        if stripped.startswith("##"):
            return True, "section"
        if stripped.startswith(("#", "//", "/*", "*")):
            return False, None
        if stripped in ('"""', "'''", ")", "}", "]", ");", "},", "};"):
            return False, None
        if stripped.startswith(("import ", "from ")) and " import " in f" {stripped} ":
            return False, None

        m = self._BRANCH_KEYWORD_RE.search(stripped)
        if m:
            return True, m.group(1).lower()
        if "?" in stripped and ":" in stripped:
            return True, "ternary"
        if "(" in stripped and ")" in stripped and not stripped.startswith("@"):
            return True, "call"
        return False, None

    def _extract_line_tokens(self, line_text: str) -> List[str]:
        raw_tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{2,}\b", line_text or "")
        seen: set[str] = set()
        tokens: List[str] = []
        for tok in raw_tokens:
            if tok in self._STOP_IDENTIFIERS or tok in seen:
                continue
            seen.add(tok)
            tokens.append(tok)
        return tokens[:8]

    def get_wiki_file_content(
        self, file_path: str, max_lines: int = 500
    ) -> Dict[str, Any]:
        """
        Reads a physical source/design/compiler file for the Code Wiki Git-like viewer,
        returning numbered lines, extracted variable/symbol tokens per line, git markers,
        branching keyword metadata (`is_branch`, `branch_keyword`), and juxtaposed node metadata.
        """
        resolved_path, node_obj = self._resolve_node_and_path(file_path)
        if resolved_path is None or not resolved_path.is_file():
            raise ValueError(f"Wiki file not found: {file_path}")

        # Enforce workspace boundary check
        resolved_str = mask_restricted_string(str(resolved_path))
        real_str = str(resolved_path.resolve())
        allowed_roots = (
            str(self.idea_projects_root.resolve()),
            str(Path("/Users/jarek/.gemini").resolve()),
        )
        if not any(real_str.startswith(root) for root in allowed_roots):
            raise ValueError(f"File path outside allowed workspace roots: {resolved_str}")

        raw_text = mask_restricted_string(resolved_path.read_text(encoding="utf-8", errors="replace"))
        raw_lines = raw_text.splitlines()
        capped_lines = raw_lines[: max(1, max_lines)]

        exported_syms: set[str] = set()
        if isinstance(node_obj, dict):
            for s in (node_obj.get("juxtaposed_code") or {}).get("exported_symbols", []):
                exported_syms.add(str(s))

        lines_out: List[Dict[str, Any]] = []
        for idx, text in enumerate(capped_lines, start=1):
            stripped = text.strip()
            tokens = self._extract_line_tokens(text)
            is_def = stripped.startswith(
                ("def ", "class ", "async def ", "export ", "interface ", "func ", "stage ", "## ")
            )
            has_exported = any(t in exported_syms for t in tokens)
            is_branch, branch_keyword = self._detect_branch_keyword(text)
            git_marker = "+" if (is_def or has_exported) else ("~" if is_branch else " ")
            lines_out.append(
                {
                    "line_number": idx,
                    "text": text,
                    "git_marker": git_marker,
                    "is_anchor": bool(is_def or has_exported),
                    "is_branch": is_branch,
                    "branch_keyword": branch_keyword,
                    "tokens": tokens,
                }
            )

        try:
            st = resolved_path.stat()
            size_bytes = int(st.st_size)
            updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.st_mtime))
        except OSError:
            size_bytes = len(raw_text.encode("utf-8"))
            updated_at = ""

        node_id = str(node_obj.get("node_id") or "") if isinstance(node_obj, dict) else ""
        layer = self._classify_node_layer(node_obj) if isinstance(node_obj, dict) else "code"
        repo_name = (
            str(node_obj.get("repo_name") or resolved_path.parent.name)
            if isinstance(node_obj, dict)
            else resolved_path.parent.name
        )
        title = (
            str(node_obj.get("title") or resolved_path.name)
            if isinstance(node_obj, dict)
            else resolved_path.name
        )
        idiomatic_name = (
            str(node_obj.get("idiomatic_name") or resolved_path.stem)
            if isinstance(node_obj, dict)
            else resolved_path.stem
        )

        try:
            rel_path = mask_restricted_string(str(resolved_path.relative_to(self.idea_projects_root)))
        except ValueError:
            rel_path = mask_restricted_string(resolved_path.name)

        return {
            "path": resolved_str,
            "name": mask_restricted_string(resolved_path.name),
            "relative_path": rel_path,
            "language": resolved_path.suffix.lstrip(".") or "text",
            "size_bytes": size_bytes,
            "updated_at": updated_at,
            "total_lines": len(raw_lines),
            "displayed_lines": len(lines_out),
            "node_id": node_id,
            "title": mask_restricted_string(title),
            "idiomatic_name": mask_restricted_string(idiomatic_name),
            "repo_name": mask_restricted_string(repo_name),
            "layer": layer,
            "exported_symbols": sorted(exported_syms),
            "content": "\n".join(capped_lines),
            "lines": lines_out,
        }

    def _build_panel_code_window(
        self,
        file_path_str: str,
        search_terms: List[str],
        preferred_line: Optional[int] = None,
        window_radius: int = 5,
        target_lines_count: int = 10,
    ) -> Dict[str, Any]:
        """
        Locates the best matching line for `search_terms` in `file_path_str` (or uses
        `preferred_line`) using Qdrant-indexed node excerpts and extracts a 10-line
        surrounding code window (`target_lines_count=10`) with highlighted lines,
        branching keyword metadata (`is_branch`, `branch_keyword`), and clickable tokens.
        """
        p_obj = Path(file_path_str) if file_path_str else None
        if p_obj is None or not p_obj.is_file():
            return {
                "focal_line_number": 1,
                "focal_line_text": "",
                "highlighted_lines": [1],
                "code_lines": [],
            }

        raw_lines = mask_restricted_string(
            p_obj.read_text(encoding="utf-8", errors="replace")
        ).splitlines()
        if not raw_lines:
            return {
                "focal_line_number": 1,
                "focal_line_text": "",
                "highlighted_lines": [1],
                "code_lines": [],
            }

        clean_terms = [t.strip() for t in search_terms if t and len(t.strip()) >= 2]
        lower_terms = [t.lower() for t in clean_terms]

        matched_line_nums: List[int] = []
        best_line = preferred_line if (preferred_line and 1 <= preferred_line <= len(raw_lines)) else 0
        best_score = -1

        for idx, line_text in enumerate(raw_lines, start=1):
            l_low = line_text.lower()
            score = 0
            for rank_i, term_low in enumerate(lower_terms):
                if term_low in l_low:
                    weight = max(1, 10 - rank_i * 2)
                    if re.search(rf"\b{re.escape(term_low)}\b", l_low):
                        weight += 5
                    score += weight
            if score > 0:
                matched_line_nums.append(idx)
                if line_text.strip().startswith(("def ", "class ", "export ", "func ", "stage ", "## ")):
                    score += 4
                if best_line == 0 or (preferred_line is None and score > best_score):
                    best_score = score
                    best_line = idx

        if best_line == 0:
            # Find first definition line or non-empty line
            for idx, line_text in enumerate(raw_lines, start=1):
                if line_text.strip().startswith(("def ", "class ", "export ", "func ", "stage ", "## ")):
                    best_line = idx
                    break
            if best_line == 0:
                best_line = 1

        if best_line not in matched_line_nums:
            matched_line_nums.insert(0, best_line)

        if target_lines_count > 0:
            half_before = (target_lines_count - 1) // 2
            start_ln = max(1, best_line - half_before)
            end_ln = min(len(raw_lines), start_ln + target_lines_count - 1)
            if end_ln - start_ln + 1 < target_lines_count:
                start_ln = max(1, end_ln - target_lines_count + 1)
        else:
            start_ln = max(1, best_line - window_radius)
            end_ln = min(len(raw_lines), best_line + window_radius)
        highlighted_set = set(matched_line_nums[:24])
        highlighted_set.add(best_line)

        code_lines: List[Dict[str, Any]] = []
        for ln in range(start_ln, end_ln + 1):
            txt = raw_lines[ln - 1]
            is_hl = ln in highlighted_set
            is_focal = ln == best_line
            is_branch, branch_kw = self._detect_branch_keyword(txt)
            code_lines.append(
                {
                    "line_number": ln,
                    "text": txt,
                    "is_highlighted": is_hl,
                    "is_focal": is_focal,
                    "is_branch": is_branch,
                    "branch_keyword": branch_kw,
                    "git_marker": "+" if is_focal else ("~" if (is_hl or is_branch) else " "),
                    "tokens": self._extract_line_tokens(txt),
                }
            )

        return {
            "focal_line_number": best_line,
            "focal_line_text": raw_lines[best_line - 1].strip(),
            "highlighted_lines": sorted(highlighted_set),
            "code_lines": code_lines,
        }

    def _analyze_variable_control_flow_in_file(
        self,
        file_path_str: str,
        clamped_line: int,
        focal_symbol: str,
    ) -> Dict[str, Any]:
        """
        Analyzes the AST and line-level control flow for `focal_symbol` starting from
        `clamped_line` in `file_path_str`. Identifies:
        - `guard_line`: The conditional guard (`if`, `try`, `match`, `while`, `for`, or assignment)
          where `focal_symbol` is evaluated or prepared before branching.
        - `true_branch_line` & `true_branch_label`: First control-flow direction (`if` / `try` / primary branch).
        - `false_branch_line` & `false_branch_label`: Second control-flow direction (`else` / `elif` / `except` / alternate branch).
        - `join_line` & `join_label`: The post-branch convergence / return line where the two control-flow paths join.
        """
        p_obj = Path(file_path_str) if file_path_str else None
        raw_lines: List[str] = []
        if p_obj is not None and p_obj.is_file():
            raw_lines = mask_restricted_string(
                p_obj.read_text(encoding="utf-8", errors="replace")
            ).splitlines()
        total_lines = max(1, len(raw_lines))
        sym_low = (focal_symbol or "").lower()

        # Attempt Python AST control-flow extraction first when file is .py
        if p_obj is not None and p_obj.suffix.lower() == ".py" and raw_lines:
            try:
                tree = ast.parse("\n".join(raw_lines))
                branch_candidates: List[Tuple[int, int, int, int, int, str, str]] = []
                for node in ast.walk(tree):
                    if isinstance(node, ast.If):
                        cond_ln = int(getattr(node, "lineno", clamped_line))
                        true_ln = int(getattr(node.body[0], "lineno", cond_ln + 1)) if node.body else min(total_lines, cond_ln + 1)
                        if node.orelse:
                            false_ln = int(getattr(node.orelse[0], "lineno", true_ln + 1))
                            false_lbl = f"else branch (L{false_ln})"
                        else:
                            end_if = int(getattr(node, "end_lineno", true_ln + 1))
                            false_ln = min(total_lines, end_if + 1)
                            false_lbl = f"fallthrough branch (L{false_ln})"
                        end_ln = int(getattr(node, "end_lineno", max(true_ln, false_ln)))
                        join_ln = min(total_lines, end_ln + 1)
                        src_seg = "\n".join(raw_lines[max(0, cond_ln - 1) : min(total_lines, end_ln)]).lower()
                        score = 0
                        if sym_low and sym_low in src_seg:
                            score += 50
                        if node.orelse:
                            score += 25
                        dist = abs(cond_ln - clamped_line)
                        score -= min(40, dist // 2)
                        branch_candidates.append(
                            (
                                score,
                                cond_ln,
                                true_ln,
                                false_ln,
                                join_ln,
                                f"if branch (L{true_ln})",
                                false_lbl,
                            )
                        )
                    elif isinstance(node, ast.Try):
                        cond_ln = int(getattr(node, "lineno", clamped_line))
                        true_ln = int(getattr(node.body[0], "lineno", cond_ln + 1)) if node.body else min(total_lines, cond_ln + 1)
                        false_ln = (
                            int(getattr(node.handlers[0], "lineno", true_ln + 1))
                            if node.handlers
                            else min(total_lines, true_ln + 2)
                        )
                        end_ln = int(getattr(node, "end_lineno", max(true_ln, false_ln)))
                        join_ln = min(total_lines, end_ln + 1)
                        src_seg = "\n".join(raw_lines[max(0, cond_ln - 1) : min(total_lines, end_ln)]).lower()
                        score = 15 + (45 if (sym_low and sym_low in src_seg) else 0) - min(40, abs(cond_ln - clamped_line) // 2)
                        branch_candidates.append(
                            (
                                score,
                                cond_ln,
                                true_ln,
                                false_ln,
                                join_ln,
                                f"try branch (L{true_ln})",
                                f"except branch (L{false_ln})",
                            )
                        )
                if branch_candidates:
                    branch_candidates.sort(key=lambda item: (-item[0], abs(item[1] - clamped_line)))
                    _, cond_ln, true_ln, false_ln, join_ln, true_lbl, false_lbl = branch_candidates[0]
                    if false_ln == true_ln:
                        false_ln = min(total_lines, true_ln + 1)
                    if join_ln <= max(true_ln, false_ln):
                        join_ln = min(total_lines, max(true_ln, false_ln) + 1)
                    return {
                        "guard_line": cond_ln,
                        "true_branch_line": true_ln,
                        "false_branch_line": false_ln,
                        "join_line": join_ln,
                        "true_branch_label": true_lbl,
                        "false_branch_label": false_lbl,
                        "join_label": f"converge join (L{join_ln})",
                    }
            except Exception:
                pass

        # Line-level control-flow scanner for TypeScript/TSX, .sig, Markdown, or fallback Python
        guard_line = min(total_lines, clamped_line + 1) if clamped_line < total_lines else clamped_line
        true_line = min(total_lines, guard_line + 1)
        false_line = min(total_lines, true_line + 2)
        join_line = min(total_lines, false_line + 2)
        true_label = f"if / true branch (L{true_line})"
        false_label = f"else / alt branch (L{false_line})"

        branch_re = re.compile(r"^\s*(if\b|elif\b|else\b|try\b|except\b|case\b|switch\b|match\b|return\b|.*\?.*:)")
        for idx in range(max(1, clamped_line), min(total_lines + 1, clamped_line + 60)):
            txt = raw_lines[idx - 1]
            if branch_re.search(txt):
                guard_line = idx
                true_line = min(total_lines, idx + 1)
                # Find matching else/except/second branch nearby
                found_alt = False
                for j in range(true_line + 1, min(total_lines + 1, idx + 35)):
                    j_txt = raw_lines[j - 1].strip()
                    if j_txt.startswith(("else", "elif ", "except", "case ", "catch", "default:")):
                        false_line = min(total_lines, j + 1 if j < total_lines else j)
                        false_label = f"else / alternate branch (L{false_line})"
                        join_line = min(total_lines, false_line + 3)
                        found_alt = True
                        break
                if not found_alt:
                    false_line = min(total_lines, true_line + 2)
                    false_label = f"fallthrough branch (L{false_line})"
                    join_line = min(total_lines, false_line + 2)
                true_label = f"if / true branch (L{true_line})"
                break

        return {
            "guard_line": guard_line,
            "true_branch_line": true_line,
            "false_branch_line": false_line,
            "join_line": join_line,
            "true_branch_label": true_label,
            "false_branch_label": false_label,
            "join_label": f"converge join (L{join_line})",
        }

    def trace_geodesic_from_file_line(
        self,
        file_path: str,
        line_number: int = 1,
        symbol: str = "",
        target_query: str = "ui:components:AsyncChatWindow",
    ) -> Dict[str, Any]:
        """
        Traces the Control-Flow & Geodesic Code Path starting from a clicked file, line number,
        and variable/symbol in the Code Wiki:
        1. Identifies the variable/symbol on the clicked line and highlights all participating
           lines in the active file.
        2. Only splits into two parallel branches when the variable enters a two-direction
           control-flow branch (`if/else`, `try/except`, conditional guard), and reconverges
           into a single join node before flowing through downstream geodesic hops.
        3. Generates a Mermaid `flowchart LR` graph (`mermaid_code`), draggable node coordinates
           (`flow_graph.nodes`), and line-to-line cubic Bezier spline edges (`flow_graph.edges`).
        4. Stores/upserts the traced graph into staging Qdrant (`code_flow_graphs` collection in
           `/Users/jarek/IdeaProjects/qdrant_staging/storage`) so the code flow is graphed in
           Qdrant and displayed as an interactive spline graph in the UI.
        """
        file_info = self.get_wiki_file_content(file_path, max_lines=500)
        lines = file_info.get("lines", [])
        total_displayed = len(lines)
        clamped_line = max(1, min(int(line_number or 1), max(1, total_displayed)))
        line_obj = lines[clamped_line - 1] if lines else {"text": "", "tokens": []}
        line_text = str(line_obj.get("text") or "")
        line_tokens = list(line_obj.get("tokens") or [])

        exported_set = set(file_info.get("exported_symbols") or [])
        focal_symbol = mask_restricted_string((symbol or "").strip())
        if not focal_symbol:
            for tok in line_tokens:
                if tok in exported_set:
                    focal_symbol = tok
                    break
        if not focal_symbol and line_tokens:
            focal_symbol = line_tokens[0]
        if not focal_symbol:
            focal_symbol = str(file_info.get("idiomatic_name") or Path(file_info["path"]).stem)

        search_terms_primary = [focal_symbol] + [t for t in line_tokens if t != focal_symbol][:3]
        origin_window = self._build_panel_code_window(
            file_info["path"],
            search_terms=search_terms_primary,
            preferred_line=clamped_line,
            window_radius=6,
        )
        active_highlighted_lines = origin_window["highlighted_lines"]

        start_node_id = str(file_info.get("node_id") or file_info["path"])
        juxtaposition = self.get_design_compiler_code_juxtaposition(
            query=start_node_id,
            include_geodesic_trace=True,
        )
        geodesic_trace = self.trace_full_geodesic_code_path(
            start_query=start_node_id,
            target_query=target_query or "ui:components:AsyncChatWindow",
            include_source_excerpts=True,
        )
        hops = list(geodesic_trace.get("hops") or [])

        # Analyze actual control-flow branching for `focal_symbol` in `file_info["path"]`
        cf_info = self._analyze_variable_control_flow_in_file(
            file_info["path"],
            clamped_line=clamped_line,
            focal_symbol=focal_symbol,
        )
        guard_line = int(cf_info["guard_line"])
        true_line = int(cf_info["true_branch_line"])
        false_line = int(cf_info["false_branch_line"])
        join_line = int(cf_info["join_line"])

        guard_win = self._build_panel_code_window(
            file_info["path"],
            search_terms=[focal_symbol],
            preferred_line=guard_line,
            window_radius=5,
        )
        true_win = self._build_panel_code_window(
            file_info["path"],
            search_terms=[focal_symbol],
            preferred_line=true_line,
            window_radius=5,
        )
        false_win = self._build_panel_code_window(
            file_info["path"],
            search_terms=[focal_symbol],
            preferred_line=false_line,
            window_radius=5,
        )
        join_win = self._build_panel_code_window(
            file_info["path"],
            search_terms=[focal_symbol],
            preferred_line=join_line,
            window_radius=5,
        )

        # Build control-flow stage columns:
        # Column 0 is Origin & Guard (1 node),
        # Column 1 splits into 2 parallel panels where `focal_symbol` branches into two control-flow directions,
        # Column 2 reconverges into 1 node at the control-flow join line, followed by downstream hops.
        raw_columns: List[Dict[str, Any]] = [
            # Column 0: Origin Line & Control-Flow Guard (1 node)
            {
                "column_index": 0,
                "stage_id": "stage-0-origin",
                "stage_label": "0_VARIABLE_ORIGIN_AND_GUARD",
                "split_mode": "origin",
                "panels": [
                    {
                        "panel_id": "col-0-panel-0",
                        "branch_label": f"L{clamped_line}->{guard_line}: {focal_symbol}",
                        "control_flow_role": "origin",
                        "node_id": start_node_id,
                        "title": f"{file_info['name']}:{clamped_line}",
                        "idiomatic_name": file_info["idiomatic_name"],
                        "repo_name": file_info["repo_name"],
                        "layer": file_info["layer"],
                        "stage_phase": "0_VARIABLE_ORIGIN_AND_GUARD",
                        "file_path": file_info["path"],
                        "relative_path": file_info["relative_path"],
                        "file_name": file_info["name"],
                        "focal_symbol": focal_symbol,
                        "focal_line_number": clamped_line,
                        "focal_line_text": line_text.strip() or guard_win["focal_line_text"],
                        "highlighted_lines": active_highlighted_lines,
                        "code_lines": origin_window["code_lines"],
                        "step_geodesic_distance": 0.0,
                        "cumulative_geodesic_distance": 0.0,
                        "transition_rationale": f"Origin variable '{focal_symbol}' at line {clamped_line} (guard L{guard_line})",
                    }
                ],
            },
            # Column 1: Two-Direction Variable Control-Flow Split (ONLY splits into 2 panels for True vs False branch)
            {
                "column_index": 1,
                "stage_id": "stage-1-control-flow-branch",
                "stage_label": "1_CONTROL_FLOW_BRANCH_SPLIT",
                "split_mode": "split",
                "panels": [
                    {
                        "panel_id": "col-1-panel-0",
                        "branch_label": str(cf_info["true_branch_label"]),
                        "control_flow_role": "branch_true",
                        "node_id": f"{start_node_id}:branch_true:L{true_line}",
                        "title": f"{file_info['name']}:{true_line}",
                        "idiomatic_name": focal_symbol,
                        "repo_name": file_info["repo_name"],
                        "layer": file_info["layer"],
                        "stage_phase": "1_CONTROL_FLOW_BRANCH_SPLIT",
                        "file_path": file_info["path"],
                        "relative_path": file_info["relative_path"],
                        "file_name": file_info["name"],
                        "focal_symbol": focal_symbol,
                        "focal_line_number": true_win["focal_line_number"],
                        "focal_line_text": true_win["focal_line_text"],
                        "highlighted_lines": true_win["highlighted_lines"],
                        "code_lines": true_win["code_lines"],
                        "step_geodesic_distance": 0.16,
                        "cumulative_geodesic_distance": 0.16,
                        "transition_rationale": f"Direction 1 ({cf_info['true_branch_label']}) for '{focal_symbol}'",
                    },
                    {
                        "panel_id": "col-1-panel-1",
                        "branch_label": str(cf_info["false_branch_label"]),
                        "control_flow_role": "branch_false",
                        "node_id": f"{start_node_id}:branch_false:L{false_line}",
                        "title": f"{file_info['name']}:{false_line}",
                        "idiomatic_name": focal_symbol,
                        "repo_name": file_info["repo_name"],
                        "layer": file_info["layer"],
                        "stage_phase": "1_CONTROL_FLOW_BRANCH_SPLIT",
                        "file_path": file_info["path"],
                        "relative_path": file_info["relative_path"],
                        "file_name": file_info["name"],
                        "focal_symbol": focal_symbol,
                        "focal_line_number": false_win["focal_line_number"],
                        "focal_line_text": false_win["focal_line_text"],
                        "highlighted_lines": false_win["highlighted_lines"],
                        "code_lines": false_win["code_lines"],
                        "step_geodesic_distance": 0.16,
                        "cumulative_geodesic_distance": 0.16,
                        "transition_rationale": f"Direction 2 ({cf_info['false_branch_label']}) for '{focal_symbol}'",
                    },
                ],
            },
            # Column 2: Control-Flow Convergence / Join Line (1 node where the 2 branches reconverge)
            {
                "column_index": 2,
                "stage_id": "stage-2-control-flow-join",
                "stage_label": "2_CONTROL_FLOW_CONVERGENCE",
                "split_mode": "converged",
                "panels": [
                    {
                        "panel_id": "col-2-panel-0",
                        "branch_label": str(cf_info["join_label"]),
                        "control_flow_role": "converge_join",
                        "node_id": f"{start_node_id}:join:L{join_line}",
                        "title": f"{file_info['name']}:{join_line}",
                        "idiomatic_name": focal_symbol,
                        "repo_name": file_info["repo_name"],
                        "layer": file_info["layer"],
                        "stage_phase": "2_CONTROL_FLOW_CONVERGENCE",
                        "file_path": file_info["path"],
                        "relative_path": file_info["relative_path"],
                        "file_name": file_info["name"],
                        "focal_symbol": focal_symbol,
                        "focal_line_number": join_win["focal_line_number"],
                        "focal_line_text": join_win["focal_line_text"],
                        "highlighted_lines": join_win["highlighted_lines"],
                        "code_lines": join_win["code_lines"],
                        "step_geodesic_distance": 0.14,
                        "cumulative_geodesic_distance": 0.30,
                        "transition_rationale": f"Control flow reconverges at line {join_line}",
                    }
                ],
            },
        ]

        # Append downstream sequential geodesic hops ending at the terminal UI component
        non_start_hops = [
            h for h in hops if str(h.get("node_id") or "") != start_node_id
        ]
        if len(non_start_hops) > 3:
            remaining_hops = non_start_hops[:2] + [non_start_hops[-1]]
        else:
            remaining_hops = non_start_hops
        col_counter = 3
        cum_dist = 0.30
        for h_item in remaining_hops:
            h_path = str(h_item.get("file_path") or h_item.get("doc_path") or "")
            h_terms = [
                focal_symbol,
                str(h_item.get("idiomatic_name") or ""),
            ] + list(h_item.get("exported_symbols") or [])[:4]
            h_win = self._build_panel_code_window(
                h_path,
                search_terms=h_terms,
                preferred_line=None,
                window_radius=5,
            )
            step_d = max(0.12, float(h_item.get("step_geodesic_distance") or 0.18))
            cum_dist = round(cum_dist + step_d, 4)
            curr_phase = str(h_item.get("stage_phase") or "4_RUNTIME_CODE_AND_SERVICE")
            f_name = Path(h_path).name if h_path else str(h_item.get("idiomatic_name") or "hop")
            raw_columns.append(
                {
                    "column_index": col_counter,
                    "stage_id": f"stage-{col_counter}-{curr_phase.lower()}",
                    "stage_label": curr_phase,
                    "split_mode": "converged",
                    "panels": [
                        {
                            "panel_id": f"col-{col_counter}-panel-0",
                            "branch_label": f"Hop L{h_win['focal_line_number']}: {f_name}",
                            "control_flow_role": "downstream_hop",
                            "node_id": str(h_item.get("node_id") or f"hop-{col_counter}"),
                            "title": f"{f_name}:{h_win['focal_line_number']}",
                            "idiomatic_name": str(h_item.get("idiomatic_name") or focal_symbol),
                            "repo_name": str(h_item.get("repo_name") or ""),
                            "layer": str(h_item.get("layer") or "code"),
                            "stage_phase": curr_phase,
                            "file_path": h_path,
                            "relative_path": str(h_item.get("relative_path") or f_name),
                            "file_name": f_name,
                            "focal_symbol": (
                                focal_symbol
                                if focal_symbol.lower() in h_win["focal_line_text"].lower()
                                else str(h_item.get("idiomatic_name") or focal_symbol)
                            ),
                            "focal_line_number": h_win["focal_line_number"],
                            "focal_line_text": h_win["focal_line_text"],
                            "highlighted_lines": h_win["highlighted_lines"],
                            "code_lines": h_win["code_lines"],
                            "step_geodesic_distance": round(step_d, 4),
                            "cumulative_geodesic_distance": cum_dist,
                            "transition_rationale": str(h_item.get("transition_rationale") or ""),
                        }
                    ],
                }
            )
            col_counter += 1

        # Build interactive 2D coordinates, spline edges, Mermaid `flowchart LR` definition, and sidebar_elements
        sidebar_elements: List[Dict[str, Any]] = []
        graph_nodes: List[Dict[str, Any]] = []
        graph_edges: List[Dict[str, Any]] = []
        mermaid_lines: List[str] = ["flowchart LR"]

        step_counter = 0
        col_x_spacing = 420
        node_width = 360
        node_height = 195

        for c_idx, col in enumerate(raw_columns):
            prev_panels = raw_columns[c_idx - 1]["panels"] if c_idx > 0 else []
            next_panels = (
                raw_columns[c_idx + 1]["panels"] if c_idx + 1 < len(raw_columns) else []
            )
            prev_ids = [p["panel_id"] for p in prev_panels]
            next_ids = [p["panel_id"] for p in next_panels]
            col["incoming_panel_count"] = len(prev_ids)
            col["outgoing_panel_count"] = len(next_ids)

            num_p = len(col["panels"])
            for p_idx, p in enumerate(col["panels"]):
                p["incoming_from_panel_ids"] = prev_ids
                p["outgoing_to_panel_ids"] = next_ids
                p_is_branch, p_branch_kw = self._detect_branch_keyword(p.get("focal_line_text", ""))
                p["is_branch"] = bool(p_is_branch or len(next_ids) > 0)
                p["branch_keyword"] = p_branch_kw or (
                    "branch" if len(next_ids) > 1 else ("next" if len(next_ids) == 1 else None)
                )
                x_coord = 24 + c_idx * col_x_spacing
                if c_idx == 0:
                    y_coord = 24
                elif num_p > 1:
                    y_coord = 24 + p_idx * 235
                else:
                    y_coord = 135 + max(0, c_idx - 2) * 52
                p["x"] = x_coord
                p["y"] = y_coord
                p["width"] = node_width
                p["height"] = node_height

                safe_m_id = p["panel_id"].replace("-", "_")
                safe_label = (
                    f"{p['file_name']}:{p['focal_line_number']} ({p['focal_symbol']})"
                    .replace('"', "'")
                )
                mermaid_lines.append(f'    {safe_m_id}["{safe_label}"]')

                graph_nodes.append(
                    {
                        "id": p["panel_id"],
                        "node_id": p["panel_id"],
                        "panel_id": p["panel_id"],
                        "qdrant_node_id": p["node_id"],
                        "column_index": c_idx,
                        "branch_index": p_idx,
                        "split_mode": col["split_mode"],
                        "control_flow_role": p.get("control_flow_role", "sequential"),
                        "branch_label": p["branch_label"],
                        "title": p["title"],
                        "file_path": p["file_path"],
                        "relative_path": p["relative_path"],
                        "file_name": p["file_name"],
                        "layer": p["layer"],
                        "focal_symbol": p["focal_symbol"],
                        "focal_line_number": p["focal_line_number"],
                        "focal_line_text": p["focal_line_text"],
                        "code_lines": p["code_lines"],
                        "x": x_coord,
                        "y": y_coord,
                        "width": node_width,
                        "height": node_height,
                        "cumulative_geodesic_distance": p["cumulative_geodesic_distance"],
                    }
                )

                sidebar_elements.append(
                    {
                        "element_id": p["panel_id"],
                        "step_index": step_counter,
                        "column_index": c_idx,
                        "panel_id": p["panel_id"],
                        "split_mode": col["split_mode"],
                        "panel_count_in_column": num_p,
                        "branch_label": p["branch_label"],
                        "node_id": p["node_id"],
                        "title": p["title"],
                        "idiomatic_name": p["idiomatic_name"],
                        "repo_name": p["repo_name"],
                        "layer": p["layer"],
                        "stage_phase": p["stage_phase"],
                        "file_path": p["file_path"],
                        "relative_path": p["relative_path"],
                        "file_name": p["file_name"],
                        "focal_symbol": p["focal_symbol"],
                        "focal_line_number": p["focal_line_number"],
                        "focal_line_text": p["focal_line_text"],
                        "highlighted_lines": p["highlighted_lines"],
                        "step_geodesic_distance": p["step_geodesic_distance"],
                        "cumulative_geodesic_distance": p["cumulative_geodesic_distance"],
                        "transition_rationale": p["transition_rationale"],
                    }
                )
                step_counter += 1

            # Create directed spline edges to the next column's panels
            for src_p in col["panels"]:
                for dst_p in next_panels:
                    src_m = src_p["panel_id"].replace("-", "_")
                    dst_m = dst_p["panel_id"].replace("-", "_")
                    is_split_edge = len(col["panels"]) == 1 and len(next_panels) > 1
                    is_converge_edge = len(col["panels"]) > 1 and len(next_panels) == 1
                    edge_label = (
                        dst_p["branch_label"]
                        if is_split_edge
                        else f"L{src_p['focal_line_number']} -> L{dst_p['focal_line_number']}"
                    )
                    safe_edge_lbl = edge_label.replace('"', "'").replace("|", "/")
                    mermaid_lines.append(f"    {src_m} -->|{safe_edge_lbl}| {dst_m}")
                    graph_edges.append(
                        {
                            "edge_id": f"edge-{src_p['panel_id']}-to-{dst_p['panel_id']}",
                            "source": src_p["panel_id"],
                            "target": dst_p["panel_id"],
                            "from_line": src_p["focal_line_number"],
                            "to_line": dst_p["focal_line_number"],
                            "variable": src_p["focal_symbol"],
                            "label": edge_label,
                            "edge_label": edge_label,
                            "connector_mode": (
                                "split"
                                if is_split_edge
                                else "converge"
                                if is_converge_edge
                                else "linear"
                            ),
                            "control_flow_type": (
                                dst_p.get("control_flow_role", "branch")
                                if is_split_edge
                                else "converge"
                                if is_converge_edge
                                else "sequential"
                            ),
                            "is_split_edge": is_split_edge,
                            "is_converge_edge": is_converge_edge,
                        }
                    )

        mermaid_code = "\n".join(mermaid_lines)
        graph_id = f"code_flow:{file_info['relative_path']}:L{clamped_line}:{focal_symbol}"
        graph_point_id = str(
            uuid5_from_args(INFORMATIONAL_NAMESPACE_UUID, "code_flow_graphs", graph_id)
        )

        flow_graph_payload = {
            "graph_id": graph_id,
            "point_id": graph_point_id,
            "collection": "code_flow_graphs",
            "file_path": file_info["path"],
            "relative_path": file_info["relative_path"],
            "file_name": file_info["name"],
            "clicked_line_number": clamped_line,
            "focal_symbol": focal_symbol,
            "mermaid_code": mermaid_code,
            "nodes": graph_nodes,
            "edges": graph_edges,
            "total_nodes": len(graph_nodes),
            "total_edges": len(graph_edges),
            "updated_at": round(time.time(), 3),
        }

        # Persist the code flow graph and its nodes in staging Qdrant (`code_flow_graphs`)
        qdrant_points: List[PointStruct] = [
            PointStruct(
                id=graph_point_id,
                vector=_deterministic_384d_vector(
                    f"{graph_id} {focal_symbol} {mermaid_code}"
                ),
                payload={
                    "doc_type": "code_flow_graph",
                    **flow_graph_payload,
                },
            )
        ]
        for g_node in graph_nodes:
            node_pt_id = str(
                uuid5_from_args(
                    INFORMATIONAL_NAMESPACE_UUID,
                    "code_flow_graphs_node",
                    graph_id,
                    g_node["node_id"],
                )
            )
            qdrant_points.append(
                PointStruct(
                    id=node_pt_id,
                    vector=_deterministic_384d_vector(
                        f"{g_node['title']} {g_node['focal_symbol']} {g_node['focal_line_text']}"
                    ),
                    payload={
                        "doc_type": "code_flow_node",
                        "graph_id": graph_id,
                        **g_node,
                    },
                )
            )

        staged_graph_points = self.staging_manager.upsert_points_lockfree(
            storage_path=self.staging_manager.storage_dir,
            collection_name="code_flow_graphs",
            points=qdrant_points,
            recreate=False,
        )
        flow_graph_payload["staged_qdrant_points"] = staged_graph_points

        clicked_is_branch, clicked_branch_kw = self._detect_branch_keyword(line_text)
        return {
            "file_path": file_info["path"],
            "file_name": file_info["name"],
            "relative_path": file_info["relative_path"],
            "node_id": start_node_id,
            "clicked_line_number": clamped_line,
            "clicked_line_text": line_text,
            "is_branch": clicked_is_branch,
            "branch_keyword": clicked_branch_kw,
            "focal_symbol": focal_symbol,
            "highlighted_lines": active_highlighted_lines,
            "total_columns": len(raw_columns),
            "total_sidebar_elements": len(sidebar_elements),
            "split_columns_count": sum(1 for c in raw_columns if len(c["panels"]) > 1),
            "converged_columns_count": sum(1 for c in raw_columns if len(c["panels"]) == 1),
            "total_geodesic_distance": round(cum_dist, 4),
            "mermaid_code": mermaid_code,
            "flow_graph": flow_graph_payload,
            "qdrant_graph_id": graph_id,
            "qdrant_collection": "code_flow_graphs",
            "sidebar_elements": sidebar_elements,
            "stage_columns": raw_columns,
            "juxtaposition": juxtaposition,
            "geodesic_trace": geodesic_trace,
        }
