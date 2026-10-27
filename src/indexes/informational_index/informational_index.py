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
        return asdict(self)


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
        payload = asdict(self)
        payload["point_id"] = str(
            uuid5_from_args(INFORMATIONAL_NAMESPACE_UUID, "info_node", self.node_id)
        )
        payload["search_text"] = self.build_search_text()
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
    `large_tools`, `Phaselang-development`) and performs lock-free collection unification
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
            meta_file = ki_dir / "metadata.json"
            title = ki_dir.name
            summary = ""
            if meta_file.is_file():
                try:
                    m = json.loads(meta_file.read_text(encoding="utf-8"))
                    title = str(m.get("title") or ki_dir.name)
                    summary = str(m.get("summary") or "")
                except Exception:
                    pass
            artifacts_dir = ki_dir / "artifacts"
            first_md = None
            if artifacts_dir.is_dir():
                mds = sorted(artifacts_dir.rglob("*.md"))
                if mds:
                    first_md = mds[0]
            info = {
                "ki_id": ki_dir.name,
                "title": title,
                "summary": summary[:450],
                "doc_path": str(first_md or meta_file),
            }
            text_lower = f"{ki_dir.name} {title} {summary}".lower()
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
                "phaselang",
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
        raw = py_path.read_text(encoding="utf-8", errors="replace")
        sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        symbols: List[str] = []
        docstrings: List[str] = []
        try:
            tree = ast.parse(raw)
            mod_doc = ast.get_docstring(tree)
            if mod_doc:
                docstrings.append(mod_doc.strip())
            for node in tree.body:
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    symbols.append(node.name)
                    ndoc = ast.get_docstring(node)
                    if ndoc and len(docstrings) < 3:
                        docstrings.append(f"{node.name}: {ndoc.strip()}")
        except SyntaxError:
            pass
        excerpt_lines = [ln for ln in raw.splitlines() if ln.strip()][:28]
        return symbols[:25], "\n".join(docstrings)[:700], "\n".join(excerpt_lines)[:900], sha

    @staticmethod
    def _extract_tsx_symbols_and_excerpt(tsx_path: Path) -> Tuple[List[str], str, str, str]:
        """
        Extracts exported TypeScript/React component & interface names, leading JSDoc comments,
        code excerpt, and SHA-256 from a `.tsx`/`.ts` file.
        """
        if not tsx_path.is_file():
            return [], "", "", ""
        raw = tsx_path.read_text(encoding="utf-8", errors="replace")
        sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
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
        return sorted(set(symbols))[:25], doc_comment, "\n".join(excerpt_lines)[:900], sha

    def _construct_juxtaposed_graph(
        self,
        unified_manifest: Dict[str, Any],
    ) -> Tuple[List[JuxtaposedNode], List[JuxtaposedGraphEdge]]:
        """
        Constructs the complete set of `JuxtaposedNode`s and `JuxtaposedGraphEdge`s across:
        0. Unified Qdrant Staging Root (`qdrant_staging`)
        1. Sibling Repositories (`components`, `portfolio`, `toolset`, `neural_networks`, `large_tools`, `Phaselang-development`)
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
                "portfolio/portfolio/README.md",
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
                "Phase-Sheaf & Quantum-Inspired Neural Architectures + Promoted Gauntlet Model",
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
                "Phaselang-development",
                "Phaselang Compiler & Symbolic Phase Language Development Repository",
                "INVARIANT: Sibling repository at /Users/jarek/IdeaProjects/Phaselang-development.",
                "Phaselang-development/README.md",
                "Phaselang-development/README.md",
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
                "Stores all 25 gauntlet neural architecture models (with PhaseLangSheafCompilerCascadeModel promoted at Rank #1 alongside PhaseSheafUnifiedMonadicCavityModel) and 12 benchmark tasks.",
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
                    "(`PhaseLangSheafCompilerCascadeModel` / `Phase-Sheaf Unified Monadic Cavity`) "
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
                            self.idea_projects_root / "portfolio" / "portfolio" / "README.md"
                        ),
                        "doc_type": "readme_md",
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
                prom_manifest.get("class_name") or "PhaseLangSheafCompilerCascadeModel"
            )
            prom_title = str(
                prom_manifest.get("model_name")
                or "PhaseLangSheafCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Phase-Sheaf Wave Cascade)"
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
                        "phaselang_compiler",
                        "phase_sheaf",
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

        # Dedicated nodes for separated `phaselang_compiler` and `monadic_cavity` packages
        neural_arch_specs = [
            (
                "model_pkg:neural_networks:phaselang_compiler",
                "neural_architecture_package",
                "PhaseLang Sheaf Compiler Cascade Package (src/models/phaselang_compiler)",
                "phaselang_compiler",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "phaselang_compiler"
                / "phaselang_compiler_model.py",
                self.idea_projects_root
                / "neural_networks"
                / "src"
                / "models"
                / "phaselang_compiler"
                / "benchmark_phaselang_compiler.py",
                [
                    "phaselang_compiler",
                    "PhaseLangSheafCompilerCascadeModel",
                    "isa_ast_gate",
                    "phase_sheaf",
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
                    "PhaseSheafUnifiedMonadicCavityModel",
                    "phase_sheaf",
                    "vcpu",
                    "neural_networks",
                ],
            ),
            (
                "model:neural_networks:PhaseSheafUnifiedMonadicCavityModel",
                "neural_architecture_package",
                "Phase-Sheaf Unified Monadic Cavity (src/models/monadic_cavity)",
                "PhaseSheafUnifiedMonadicCavityModel",
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
                ["monadic_cavity", "phase_sheaf", "vcpu", "neural_networks"],
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

        filtered: List[Dict[str, Any]] = []
        for n in all_nodes:
            if node_type and n.get("node_type") != node_type:
                continue
            if repo_name and n.get("repo_name") != repo_name:
                continue
            if keep_in_place is not None and bool(n.get("keep_in_place")) != bool(keep_in_place):
                continue
            filtered.append(n)

        q_clean = (query or "").strip()
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
