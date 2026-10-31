"""Qdrant-backed email template store for Agent Brain.

Templates are stored as points in a dedicated local Qdrant collection
(``email_templates``). Each point payload carries the template metadata
(name, category, subject, body, variables) and the vector is a TF-IDF
embedding of the subject + body, enabling semantic search for the right
template (alerts, job application outbound emails, etc.).

Template bodies use Python ``str.format``-style ``{variable}`` placeholders;
unknown variables are left intact when rendering.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.ingest.doc_pipeline.embed import TfidfEmbedder  # noqa: E402
from applications.agent_brain.core.settings import get  # noqa: E402


class EmailTemplateError(RuntimeError):
    """Raised for template store operations failures."""


class EmailTemplateStore:
    """CRUD + semantic search for email templates in Qdrant."""

    DEFAULT_VECTOR_SIZE = 384

    def __init__(
        self,
        qdrant_path: Optional[str] = None,
        collection: Optional[str] = None,
        vector_size: Optional[int] = None,
    ):
        self.qdrant_path = qdrant_path or str(
            Path(get("email.qdrant_path", "data/.qdrant_email_templates"))
        )
        self.collection = collection or get(
            "email.qdrant_collection", "email_templates"
        )
        self.vector_size = int(vector_size or get("email.qdrant_vector_size", 384))
        self._client: Optional[QdrantClient] = None
        self._embedder = TfidfEmbedder()
        self._fitted_texts: list[str] = []
        self._initialized = False

    # ------------------------------------------------------------------
    # Qdrant client / collection
    # ------------------------------------------------------------------
    def _ensure_client(self) -> QdrantClient:
        if self._client is None:
            qdrant_path = Path(self.qdrant_path)
            qdrant_path.parent.mkdir(parents=True, exist_ok=True)
            self._client = QdrantClient(path=str(qdrant_path))
            self._ensure_collection()
        return self._client

    def _ensure_collection(self) -> None:
        client = self._ensure_client()
        try:
            client.get_collection(self.collection)
        except Exception:
            client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE,
                ),
            )
        self._initialized = True

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------
    def _embed_text(self, text: str) -> list[float]:
        # Fit the embedder on all known template texts so vectors stay
        # comparable across the collection.
        all_texts = self._fitted_texts + [text]
        self._embedder.fit(all_texts)
        vec = np.asarray(self._embedder.embed(text), dtype=np.float64)
        if vec.size < self.vector_size:
            vec = np.pad(vec, (0, self.vector_size - vec.size))
        else:
            vec = vec[: self.vector_size]
        return vec.tolist()

    def _refresh_fit(self) -> None:
        """Re-fit the embedder against every stored template text."""
        records = self._scroll_all()
        self._fitted_texts = [
            r.payload.get("subject", "") + " " + r.payload.get("body", "")
            for r in records
            if r.payload.get("subject") or r.payload.get("body")
        ]
        if self._fitted_texts:
            self._embedder.fit(self._fitted_texts)

    def _scroll_all(self):
        client = self._ensure_client()
        records, _ = client.scroll(
            collection_name=self.collection,
            limit=10000,
            with_payload=True,
            with_vectors=False,
        )
        return records or []

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------
    def write_template(
        self,
        template_id: str,
        name: str,
        category: str,
        subject: str,
        body: str,
        variables: Optional[list[str]] = None,
    ) -> dict:
        """Upsert a template. Returns the stored template dict."""
        client = self._ensure_client()
        text_for_vector = f"{subject} {body}"
        vector = self._embed_text(text_for_vector)

        # Re-fit against all templates so existing vectors stay comparable.
        self._refresh_fit()
        # Re-embed this template with the fitted vocab.
        vector = self._embed_text(text_for_vector)

        payload = {
            "template_id": template_id,
            "name": name,
            "category": category,
            "subject": subject,
            "body": body,
            "variables": variables or [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        client.upsert(
            collection_name=self.collection,
            points=[
                PointStruct(
                    id=self._point_id(template_id),
                    vector=vector,
                    payload=payload,
                )
            ],
        )
        return self.get_template(template_id)

    def list_templates(self) -> list[dict]:
        """Return all stored templates."""
        records = self._scroll_all()
        return [r.payload or {} for r in records]

    def get_template(self, template_id: str) -> Optional[dict]:
        """Return a single template by id, or None if not found."""
        records = self._scroll_all()
        for r in records:
            if (r.payload or {}).get("template_id") == template_id:
                return r.payload
        return None

    def delete_template(self, template_id: str) -> bool:
        """Delete a template by id. Returns True if deleted."""
        client = self._ensure_client()
        existing = self.get_template(template_id)
        if existing is None:
            return False
        client.delete(
            collection_name=self.collection,
            points_selector=[self._point_id(template_id)],
        )
        return True

    def search_templates(self, query: str, top_k: int = 5) -> list[dict]:
        """Semantically search templates by query text."""
        records = self._scroll_all()
        if not records:
            return []

        texts = [
            (r.payload or {}).get("subject", "") + " " + (r.payload or {}).get("body", "")
            for r in records
        ]
        self._embedder.fit(texts)
        q_vec = np.asarray(self._embedder.embed(query), dtype=np.float64)
        if q_vec.size < self.vector_size:
            q_vec = np.pad(q_vec, (0, self.vector_size - q_vec.size))
        else:
            q_vec = q_vec[: self.vector_size]

        scored = []
        for r in records:
            vec = None
            # Re-embed each stored text with the fitted vocab for comparison.
            text = texts[len(scored)]
            v = np.asarray(self._embedder.embed(text), dtype=np.float64)
            if v.size < self.vector_size:
                v = np.pad(v, (0, self.vector_size - v.size))
            else:
                v = v[: self.vector_size]
            denom = (np.linalg.norm(q_vec) * np.linalg.norm(v)) or 1.0
            sim = float(np.dot(q_vec, v) / denom)
            scored.append((sim, r))

        scored.sort(key=lambda x: x[0], reverse=True)
        results = []
        for sim, r in scored[:top_k]:
            payload = dict(r.payload or {})
            payload["score"] = round(sim, 4)
            results.append(payload)
        return results

    def render_template(self, template_id: str, variables: Optional[dict] = None) -> dict:
        """Render a stored template's subject and body with variables."""
        template = self.get_template(template_id)
        if template is None:
            raise EmailTemplateError(f"Template not found: {template_id}")
        vars_map = dict(variables or {})
        subject = self._safe_format(template.get("subject", ""), vars_map)
        body = self._safe_format(template.get("body", ""), vars_map)
        return {
            "template_id": template_id,
            "name": template.get("name", ""),
            "category": template.get("category", ""),
            "subject": subject,
            "body": body,
            "variables": template.get("variables", []),
        }

    @staticmethod
    def _point_id(template_id: str) -> str:
        import hashlib

        return hashlib.md5(template_id.encode("utf-8")).hexdigest()

    @staticmethod
    def _safe_format(template: str, variables: dict) -> str:
        from string import Formatter

        class _SafeFormatter(Formatter):
            def get_value(self, key, args, kwargs):
                if isinstance(key, str):
                    return kwargs.get(key, "{" + key + "}")
                return super().get_value(key, args, kwargs)

        return _SafeFormatter().vformat(template, (), variables)
