"""
Reusable FSBase-Backed File System CRUD & Bidirectional Multimedia Service
Coupled with Unified Qdrant Staging (`promoted_model_artifacts`) and MCP (`fs_crud`).

Provides:
1. Solidified `FSBase` (`fsspecc.base_fsspecfs.base_fsspecfs.FSBase`) read/write/append/mkdir/delete
   operations over `/Users/jarek/IdeaProjects/neural_networks/promoted` and
   `/Users/jarek/IdeaProjects/qdrant_staging/promoted_workspace`.
2. Bidirectional multimedia & artifact support (images, audio, video, PDF, markdown, Python/TS code,
   JSON/JSONL) backed up via `FSBase` to disk and indexed in `/Users/jarek/IdeaProjects/qdrant_staging/storage`
   (`promoted_model_artifacts` collection).
3. Direct JSON schema compatibility with the reusable `FileSystemExplorer` frontend component
   (`FileSystemRootNode`, `FileSystemFolderNode`, `FileSystemFileItem`) and the `fs_crud` MCP tool.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import io
import json
import mimetypes
import pickle
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from qdrant_client.models import PointStruct

from benchmark_ledger.vocab_mask import mask_restricted_string, mask_restricted_vocabulary
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.informational_index.informational_index import (
    DEFAULT_IDEA_PROJECTS_ROOT,
    DEFAULT_STAGING_ROOT,
    INFORMATIONAL_NAMESPACE_UUID,
    UnifiedQdrantStagingManager,
    _deterministic_384d_vector,
    _encode_sqlite_point_id,
)
from simple_functions.simple_functions import uuid5_from_args

PROMOTED_ARTIFACTS_COLLECTION = "promoted_model_artifacts"
PROTECTED_FILENAMES = frozenset({"promoted_model.py", "harness.py", "promoted_manifest.json", "__init__.py"})

IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico"})
AUDIO_EXTENSIONS = frozenset({".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac"})
VIDEO_EXTENSIONS = frozenset({".mp4", ".webm", ".mov", ".mkv"})
CODE_EXTENSIONS = frozenset({".py", ".ts", ".tsx", ".js", ".jsx", ".sh", ".sql", ".yaml", ".yml", ".toml", ".css", ".html"})


def classify_file_type(file_name: str, media_type: str = "") -> str:
    ext = Path(file_name).suffix.lower()
    mt = (media_type or "").lower()
    if ext in IMAGE_EXTENSIONS or mt.startswith("image/"):
        return "image"
    if ext in AUDIO_EXTENSIONS or mt.startswith("audio/"):
        return "audio"
    if ext in VIDEO_EXTENSIONS or mt.startswith("video/"):
        return "video"
    if ext == ".pdf" or mt == "application/pdf":
        return "pdf"
    if ext in (".md", ".markdown", ".txt", ".rst"):
        return "markdown"
    if ext in (".json", ".jsonl"):
        return "json"
    if ext in CODE_EXTENSIONS:
        return "code"
    return "binary"


def is_multimedia_or_binary_type(file_type: str, ext: str) -> bool:
    if file_type in ("image", "audio", "video", "pdf", "binary"):
        return True
    return ext.lower() in IMAGE_EXTENSIONS | AUDIO_EXTENSIONS | VIDEO_EXTENSIONS | {".pdf", ".bin", ".pt", ".pth"}


class FSBaseCrudService:
    """
    Reusable `FSBase` File System CRUD service coupled with Qdrant Staging (`promoted_model_artifacts`)
    and `FileSystemExplorer`.
    """

    def __init__(
        self,
        promoted_root: str | Path = DEFAULT_IDEA_PROJECTS_ROOT / "neural_networks" / "promoted",
        staging_root: str | Path = DEFAULT_STAGING_ROOT,
        idea_projects_root: str | Path = DEFAULT_IDEA_PROJECTS_ROOT,
        collection_name: str = PROMOTED_ARTIFACTS_COLLECTION,
    ) -> None:
        self.promoted_root = Path(promoted_root).resolve()
        self.staging_root = Path(staging_root).resolve()
        self.idea_projects_root = Path(idea_projects_root).resolve()
        self.collection_name = collection_name

        self.workspace_root = self.staging_root / "promoted_workspace"
        self.promoted_artifacts_dir = self.promoted_root / "artifacts"
        self.staging_artifacts_dir = self.workspace_root / "artifacts"
        self.staging_multimedia_dir = self.workspace_root / "multimedia"
        self.staging_conversations_dir = self.workspace_root / "conversations"

        # Solidify all paths with FSBase
        self.fs = FSBase("file")
        for directory in (
            self.promoted_root,
            self.promoted_artifacts_dir,
            self.workspace_root,
            self.staging_artifacts_dir,
            self.staging_multimedia_dir,
            self.staging_conversations_dir,
        ):
            self.fs.make_dirs(str(directory), exist_ok=True)

        self.staging_manager = UnifiedQdrantStagingManager(
            staging_root=self.staging_root,
            idea_projects_root=self.idea_projects_root,
        )
        self._ensure_default_artifacts_seeded()

    def _read_bytes_via_fsbase(self, file_path: Path) -> bytes:
        buf = io.BytesIO()
        self.fs.read(str(file_path), buf)
        return buf.getvalue()

    def _write_bytes_via_fsbase(self, file_path: Path, data: bytes) -> None:
        self.fs.make_dirs(str(file_path.parent), exist_ok=True)
        buf = io.BytesIO(data)
        self.fs.write(str(file_path), buf)

    def _delete_point_from_staging_sqlite(self, point_id: str) -> None:
        sqlite_path = (
            self.staging_manager.storage_dir
            / "collection"
            / self.collection_name
            / "storage.sqlite"
        )
        if not sqlite_path.is_file():
            return
        enc_id = _encode_sqlite_point_id(str(point_id))
        try:
            with sqlite3.connect(str(sqlite_path), timeout=5.0) as conn:
                conn.execute("DELETE FROM points WHERE id = ?", (enc_id,))
                conn.commit()
        except Exception:
            pass

    def _ensure_default_artifacts_seeded(self) -> None:
        """
        Ensures the promoted workspace has an initial architecture overview artifact and SVG
        telemetry badge diagram backed by `FSBase` and indexed in `promoted_model_artifacts`.
        """
        readme_path = self.promoted_artifacts_dir / "promoted_architecture_notes.md"
        if not self.fs.client.exists(str(readme_path)):
            md_content = (
                "# Promoted Model Workspace & Artifact Ledger\n\n"
                "This workspace is managed via `FSBase` (`fsspecc.base_fsspecfs.base_fsspecfs.FSBase`) "
                "and synchronized to staging Qdrant (`promoted_model_artifacts`).\n\n"
                "- **Primary Promoted Path**: `/Users/jarek/IdeaProjects/neural_networks/promoted`\n"
                "- **Staging Workspace Path**: `/Users/jarek/IdeaProjects/qdrant_staging/promoted_workspace`\n"
                "- **MCP Tool**: `fs_crud` (`list`, `read`, `create`, `update`, `upload`, `delete`)\n"
                "- **Bidirectional Multimedia**: Upload images, audio, video, PDFs, markdown, or code via the File System Explorer.\n"
            ).encode("utf-8")
            self._write_bytes_via_fsbase(readme_path, md_content)
            self._write_bytes_via_fsbase(self.staging_artifacts_dir / readme_path.name, md_content)

        svg_path = self.promoted_artifacts_dir / "cohesive_manifold_topology.svg"
        if not self.fs.client.exists(str(svg_path)):
            svg_content = (
                '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="180" viewBox="0 0 480 180">'
                '<rect width="480" height="180" rx="8" fill="#0b0e14" stroke="#30363d" stroke-width="2"/>'
                '<circle cx="95" cy="90" r="42" fill="#161c26" stroke="#58a6ff" stroke-width="2"/>'
                '<text x="95" y="94" fill="#e6edf3" font-family="monospace" font-size="11" text-anchor="middle">Poincare B^D</text>'
                '<path d="M 137 90 C 185 45, 225 135, 275 90" fill="none" stroke="#58a6ff" stroke-width="2.5"/>'
                '<rect x="275" y="56" width="170" height="68" rx="6" fill="#11161f" stroke="#58a6ff" stroke-width="2"/>'
                '<text x="360" y="86" fill="#e6edf3" font-family="monospace" font-size="11" text-anchor="middle">6-Scale Any-Gram</text>'
                '<text x="360" y="104" fill="#9ca3af" font-family="monospace" font-size="10" text-anchor="middle">FSBase + Qdrant Staging</text>'
                "</svg>"
            ).encode("utf-8")
            self._write_bytes_via_fsbase(svg_path, svg_content)
            self._write_bytes_via_fsbase(self.staging_multimedia_dir / svg_path.name, svg_content)

        self.sync_files_to_staging_qdrant()

    def _resolve_target_path(
        self,
        path_or_rel: str,
        root_id: str = "promoted_repo",
        default_folder: str = "artifacts",
    ) -> Path:
        raw = (path_or_rel or "").strip()
        if not raw:
            raise ValueError("File path cannot be empty")

        candidate = Path(raw)
        if candidate.is_absolute():
            resolved = candidate.resolve()
            if (
                resolved == self.promoted_root
                or self.promoted_root in resolved.parents
                or resolved == self.workspace_root
                or self.workspace_root in resolved.parents
            ):
                return resolved
            # If outside, place safely inside the target root's default folder by filename
            raw = candidate.name

        clean_rel = raw.lstrip("/").replace("..", "_")
        base_root = (
            self.workspace_root
            if root_id in ("promoted_staging", "staging", "qdrant_staging")
            else self.promoted_root
        )
        direct_candidate = (base_root / clean_rel).resolve()
        if direct_candidate.exists() or "/" in clean_rel:
            if base_root in direct_candidate.parents or direct_candidate == base_root:
                return direct_candidate

        if default_folder and not clean_rel.startswith(f"{default_folder}/"):
            target = (base_root / default_folder / clean_rel).resolve()
        else:
            target = (base_root / clean_rel).resolve()
        return target

    def _build_file_metadata(self, file_path: Path, root_base: Path, root_id: str) -> Dict[str, Any]:
        try:
            st = file_path.stat()
            size_bytes = int(st.st_size)
            mtime_ns = int(st.st_mtime_ns)
            updated_at = dt.datetime.fromtimestamp(st.st_mtime, tz=dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
        except OSError:
            size_bytes = 0
            mtime_ns = int(time.time_ns())
            updated_at = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        try:
            rel_path = str(file_path.relative_to(root_base))
        except ValueError:
            rel_path = file_path.name

        folder = str(Path(rel_path).parent) if "/" in rel_path else "root"
        guessed_mime, _ = mimetypes.guess_type(file_path.name)
        media_type = guessed_mime or (
            "text/markdown"
            if file_path.suffix.lower() == ".md"
            else "text/x-python"
            if file_path.suffix.lower() == ".py"
            else "application/json"
            if file_path.suffix.lower() in (".json", ".jsonl")
            else "application/octet-stream"
        )
        file_type = classify_file_type(file_path.name, media_type)
        is_multimedia = is_multimedia_or_binary_type(file_type, file_path.suffix)
        point_id = str(
            uuid5_from_args(
                INFORMATIONAL_NAMESPACE_UUID,
                self.collection_name,
                root_id,
                rel_path,
            )
        )
        return {
            "name": mask_restricted_string(file_path.name),
            "friendly_name": mask_restricted_string(file_path.name),
            "title": mask_restricted_string(f"{root_id}/{rel_path}"),
            "path": str(file_path),
            "absolute_path": str(file_path),
            "relative_path": mask_restricted_string(rel_path),
            "folder": folder,
            "root_id": root_id,
            "size_bytes": size_bytes,
            "updated_at": updated_at,
            "modified_at": updated_at,
            "modified_ts_ms": mtime_ns // 1_000_000,
            "extension": file_path.suffix.lower(),
            "file_type": file_type,
            "media_type": media_type,
            "is_multimedia": is_multimedia,
            "is_artifact": True,
            "has_mermaid": False,
            "has_code_blocks": file_type in ("code", "markdown", "json"),
            "entry_id": point_id,
            "qdrant_point_id": point_id,
            "protected": file_path.name in PROTECTED_FILENAMES and folder == "root",
        }

    def sync_files_to_staging_qdrant(self) -> Dict[str, Any]:
        """
        Indexes all files across `promoted_root` and `workspace_root` into staging Qdrant
        (`/Users/jarek/IdeaProjects/qdrant_staging/storage`, collection `promoted_model_artifacts`).
        """
        points: List[PointStruct] = []
        roots = [
            ("promoted_repo", self.promoted_root),
            ("promoted_staging", self.workspace_root),
        ]
        for root_id, root_path in roots:
            if not root_path.is_dir():
                continue
            for walk_root, files in self.fs.walk_files(str(root_path)):
                w_path = Path(walk_root)
                if "__pycache__" in w_path.parts or any(p.startswith(".") for p in w_path.parts):
                    continue
                for fname in sorted(files):
                    if fname.startswith(".") or fname.endswith(".pyc"):
                        continue
                    fpath = w_path / fname
                    meta = self._build_file_metadata(fpath, root_path, root_id)
                    try:
                        raw = self._read_bytes_via_fsbase(fpath)
                    except Exception:
                        raw = b""
                    sha256 = hashlib.sha256(raw).hexdigest()
                    meta["sha256"] = sha256
                    if not meta["is_multimedia"] or meta["extension"] == ".svg":
                        text_preview = mask_restricted_string(
                            raw.decode("utf-8", errors="replace")[:4000]
                        )
                    else:
                        text_preview = f"[Multimedia {meta['file_type']} artifact: {fname} ({len(raw)} bytes, {meta['media_type']})]"
                    meta["snippet"] = text_preview[:240]
                    meta["content_preview"] = text_preview
                    if len(raw) <= 512_000:
                        meta["content_base64"] = base64.b64encode(raw).decode("ascii")
                    vec = _deterministic_384d_vector(
                        f"{meta['name']} {meta['relative_path']} {meta['file_type']} {text_preview[:1000]}"
                    )
                    points.append(
                        PointStruct(
                            id=meta["qdrant_point_id"],
                            vector=vec,
                            payload=meta,
                        )
                    )

        total_staged = self.staging_manager.upsert_points_lockfree(
            storage_path=self.staging_manager.storage_dir,
            collection_name=self.collection_name,
            points=points,
            recreate=True,
        )
        return {
            "collection": self.collection_name,
            "staging_storage_dir": str(self.staging_manager.storage_dir),
            "indexed_files_count": len(points),
            "staged_points_count": total_staged,
        }

    def list_tree(self, query: str = "") -> Dict[str, Any]:
        """
        Returns the `FileSystemExplorer`-compatible directory tree and flat artifact catalog
        for `/Users/jarek/IdeaProjects/neural_networks/promoted` and
        `/Users/jarek/IdeaProjects/qdrant_staging/promoted_workspace`.
        """
        q_low = (query or "").strip().lower()
        roots_spec = [
            (
                "promoted_repo",
                "promoted",
                "Promoted Model Repository (neural_networks/promoted)",
                "neural_networks",
                self.promoted_root,
            ),
            (
                "promoted_staging",
                "staging",
                "Staging Qdrant Workspace (qdrant_staging/promoted_workspace)",
                "qdrant_staging",
                self.workspace_root,
            ),
        ]

        tree_nodes: List[Dict[str, Any]] = []
        all_files: List[Dict[str, Any]] = []

        for root_id, short_id, friendly_name, project_name, root_path in roots_spec:
            if not root_path.is_dir():
                continue
            folders_map: Dict[str, List[Dict[str, Any]]] = {}
            root_files: List[Dict[str, Any]] = []

            for walk_root, files in self.fs.walk_files(str(root_path)):
                w_path = Path(walk_root)
                if "__pycache__" in w_path.parts or any(p.startswith(".") for p in w_path.parts):
                    continue
                for fname in sorted(files):
                    if fname.startswith(".") or fname.endswith(".pyc"):
                        continue
                    fpath = w_path / fname
                    item = self._build_file_metadata(fpath, root_path, root_id)
                    if q_low and (
                        q_low not in item["name"].lower()
                        and q_low not in item["relative_path"].lower()
                        and q_low not in item["file_type"].lower()
                    ):
                        continue
                    all_files.append(item)
                    if item["folder"] == "root":
                        root_files.append(item)
                    else:
                        folders_map.setdefault(item["folder"], []).append(item)

            folder_nodes: List[Dict[str, Any]] = []
            for folder_rel, f_items in sorted(folders_map.items()):
                folder_nodes.append(
                    {
                        "folder_id": f"{root_id}:{folder_rel}",
                        "node_id": f"{root_id}:{folder_rel}",
                        "name": folder_rel,
                        "friendly_name": folder_rel,
                        "relative_path": folder_rel,
                        "file_count": len(f_items),
                        "files": f_items,
                        "children": f_items,
                        "folders": [],
                    }
                )

            total_root_files = len(root_files) + sum(len(v) for v in folders_map.values())
            latest_ts = max(
                ([f["modified_ts_ms"] for f in root_files] + [
                    f["modified_ts_ms"] for flist in folders_map.values() for f in flist
                ])
                or [int(time.time() * 1000)]
            )
            updated_str = dt.datetime.fromtimestamp(
                latest_ts / 1000.0, tz=dt.timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")

            tree_nodes.append(
                {
                    "node_id": root_id,
                    "conversation_id": root_id,
                    "short_id": short_id,
                    "friendly_name": friendly_name,
                    "workspace_project": project_name,
                    "path": str(root_path),
                    "updated_at": updated_str,
                    "mtime_ns": latest_ts * 1_000_000,
                    "total_files": total_root_files,
                    "artifact_count": total_root_files,
                    "task_log_count": 0,
                    "artifact_files_count": total_root_files,
                    "latest_ts_ms": latest_ts,
                    "latest_timestamp": updated_str,
                    "folders": folder_nodes,
                    "children": folder_nodes,
                    "root_files": root_files,
                }
            )

        return {
            "status": "ok",
            "collection": self.collection_name,
            "promoted_root": str(self.promoted_root),
            "staging_workspace_root": str(self.workspace_root),
            "total_files": len(all_files),
            "tree": tree_nodes,
            "files": all_files,
        }

    def read_file(
        self,
        path_or_rel: str,
        root_id: str = "promoted_repo",
    ) -> Dict[str, Any]:
        """
        Reads a text, code, markdown, or binary/multimedia file via `FSBase` and returns
        both UTF-8 content (when applicable) and base64 `data_uri` for bidirectional multimedia rendering.
        """
        target = self._resolve_target_path(path_or_rel, root_id=root_id, default_folder="")
        if not self.fs.client.exists(str(target)):
            # Try under artifacts/ or multimedia/ across both roots
            for cand_root in (self.promoted_root, self.workspace_root):
                for sub in ("", "artifacts", "multimedia", "conversations"):
                    cand = (cand_root / sub / Path(path_or_rel).name).resolve() if sub else (cand_root / path_or_rel.lstrip("/")).resolve()
                    if self.fs.client.exists(str(cand)) and cand.is_file():
                        target = cand
                        break
                if self.fs.client.exists(str(target)) and target.is_file():
                    break

        if not self.fs.client.exists(str(target)) or not target.is_file():
            raise FileNotFoundError(f"File not found in FSBase workspace: {path_or_rel}")

        eff_root_base = (
            self.workspace_root
            if (target == self.workspace_root or self.workspace_root in target.parents)
            else self.promoted_root
        )
        eff_root_id = (
            "promoted_staging" if eff_root_base == self.workspace_root else "promoted_repo"
        )
        meta = self._build_file_metadata(target, eff_root_base, eff_root_id)
        raw_bytes = self._read_bytes_via_fsbase(target)
        sha256 = hashlib.sha256(raw_bytes).hexdigest()
        b64_str = base64.b64encode(raw_bytes).decode("ascii")
        media_type = meta["media_type"]
        data_uri = f"data:{media_type};base64,{b64_str}"

        if not meta["is_multimedia"] or meta["extension"] == ".svg":
            text_content = mask_restricted_string(raw_bytes.decode("utf-8", errors="replace"))
        else:
            text_content = ""

        return {
            "status": "ok",
            **meta,
            "mime_type": media_type,
            "is_binary": bool(meta["is_multimedia"] and meta["extension"] != ".svg"),
            "sha256": sha256,
            "content": text_content,
            "content_base64": b64_str,
            "base64_content": b64_str,
            "data_uri": data_uri,
            "raw_size_bytes": len(raw_bytes),
        }

    def write_file(
        self,
        path_or_rel: str,
        content: Optional[str] = None,
        content_base64: Optional[str] = None,
        root_id: str = "promoted_repo",
        folder: str = "artifacts",
        media_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Creates or updates a text or binary/multimedia file via `FSBase`, backs it up across
        both `neural_networks/promoted` and `qdrant_staging/promoted_workspace`, and indexes
        it into staging Qdrant (`promoted_model_artifacts`).
        """
        target = self._resolve_target_path(
            path_or_rel,
            root_id=root_id,
            default_folder=folder,
        )

        if content_base64:
            clean_b64 = content_base64.strip()
            if clean_b64.startswith("data:") and "," in clean_b64:
                header, clean_b64 = clean_b64.split(",", 1)
                if not media_type and ";" in header:
                    media_type = header[5:].split(";", 1)[0]
            raw_bytes = base64.b64decode(clean_b64)
        elif content is not None:
            raw_bytes = content.encode("utf-8")
        else:
            raw_bytes = b""

        self._write_bytes_via_fsbase(target, raw_bytes)

        # Mirror backup to the sibling FSBase root so promoted_repo and qdrant_staging stay backed up
        file_type = classify_file_type(target.name, media_type or "")
        is_media = is_multimedia_or_binary_type(file_type, target.suffix)
        if target == self.promoted_root or self.promoted_root in target.parents:
            backup_sub = "multimedia" if is_media else "artifacts"
            backup_target = self.workspace_root / backup_sub / target.name
            self._write_bytes_via_fsbase(backup_target, raw_bytes)
        else:
            backup_target = self.promoted_artifacts_dir / target.name
            self._write_bytes_via_fsbase(backup_target, raw_bytes)

        qdrant_sync = self.sync_files_to_staging_qdrant()
        file_info = self.read_file(str(target), root_id=root_id)
        return {
            "status": "saved",
            "path": str(target),
            "file": file_info,
            "backup_path": str(backup_target),
            "qdrant_staging": qdrant_sync,
        }

    def delete_file(
        self,
        path_or_rel: str,
        root_id: str = "promoted_repo",
    ) -> Dict[str, Any]:
        """
        Deletes a file from `FSBase` and removes its point from staging Qdrant
        (while protecting core harness files `promoted_model.py`, `harness.py`, `promoted_manifest.json`).
        """
        target = self._resolve_target_path(path_or_rel, root_id=root_id, default_folder="")
        if not self.fs.client.exists(str(target)):
            target = self._resolve_target_path(path_or_rel, root_id=root_id, default_folder="artifacts")
        if not self.fs.client.exists(str(target)):
            raise FileNotFoundError(f"File not found for deletion: {path_or_rel}")

        if target.name in PROTECTED_FILENAMES and target.parent == self.promoted_root:
            raise ValueError(f"Protected promoted harness file cannot be deleted: {target.name}")

        eff_root_base = (
            self.workspace_root
            if (target == self.workspace_root or self.workspace_root in target.parents)
            else self.promoted_root
        )
        eff_root_id = (
            "promoted_staging" if eff_root_base == self.workspace_root else "promoted_repo"
        )
        meta = self._build_file_metadata(target, eff_root_base, eff_root_id)
        self.fs.client.rm(str(target))
        self._delete_point_from_staging_sqlite(meta["qdrant_point_id"])

        # Also remove mirror backup if present
        for mirror_cand in (
            self.promoted_artifacts_dir / target.name,
            self.staging_artifacts_dir / target.name,
            self.staging_multimedia_dir / target.name,
        ):
            if self.fs.client.exists(str(mirror_cand)) and mirror_cand.is_file():
                try:
                    self.fs.client.rm(str(mirror_cand))
                except Exception:
                    pass

        qdrant_sync = self.sync_files_to_staging_qdrant()
        return {
            "status": "deleted",
            "deleted_path": str(target),
            "deleted_name": target.name,
            "qdrant_staging": qdrant_sync,
        }

    def execute_crud(self, action: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Unified CRUD entrypoint for MCP (`fs_crud`) and REST API (`/api/fs-crud` & `/api/benchmarks/promoted/fs`).
        Supported actions: `list`, `tree`, `read`, `create`, `update`, `write`, `upload`, `delete`, `mkdir`, `sync`.
        """
        act = (action or "list").strip().lower()
        scope = str(kwargs.get("scope") or "").strip().lower()
        raw_root_id = str(kwargs.get("root_id") or "").strip()
        if raw_root_id:
            root_id = raw_root_id
        elif scope in ("workspace", "staging", "promoted_staging"):
            root_id = "promoted_staging"
        else:
            root_id = "promoted_repo"

        if act in ("list", "tree"):
            return self.list_tree(query=str(kwargs.get("query") or ""))
        if act == "read":
            path = str(kwargs.get("path") or kwargs.get("file_path") or kwargs.get("name") or "")
            return self.read_file(path_or_rel=path, root_id=root_id)
        if act in ("create", "update", "write", "upload"):
            path = str(kwargs.get("path") or kwargs.get("file_path") or kwargs.get("name") or "")
            folder = str(kwargs.get("folder") or "artifacts")
            content = kwargs.get("content")
            content_base64 = kwargs.get("content_base64") or kwargs.get("base64_content")
            media_type = kwargs.get("media_type") or kwargs.get("mime_type")
            return self.write_file(
                path_or_rel=path,
                content=str(content) if content is not None else None,
                content_base64=str(content_base64) if content_base64 is not None else None,
                root_id=root_id,
                folder=folder,
                media_type=str(media_type) if media_type else None,
            )
        if act == "delete":
            path = str(kwargs.get("path") or kwargs.get("file_path") or kwargs.get("name") or "")
            return self.delete_file(path_or_rel=path, root_id=root_id)
        if act == "mkdir":
            path = str(kwargs.get("path") or kwargs.get("file_path") or kwargs.get("name") or "")
            target = self._resolve_target_path(path, root_id=root_id, default_folder="")
            self.fs.make_dirs(str(target), exist_ok=True)
            return {"status": "mkdir_ok", "path": str(target)}
        if act == "sync":
            return self.sync_files_to_staging_qdrant()
        raise ValueError(
            f"Unsupported fs_crud action '{action}'. Use 'list', 'read', 'create', 'update', 'upload', 'delete', 'mkdir', or 'sync'."
        )
