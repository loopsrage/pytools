import re
from dataclasses import dataclass, field

import numpy as np

from .embed import TfidfEmbedder, cosine_similarity


@dataclass
class Chunk:
    id: str
    doc_id: str
    filename: str
    text: str
    vector: np.ndarray = field(repr=False)
    metadata: dict = field(default_factory=dict)


class HybridIndex:
    def __init__(self):
        self._chunks: list[Chunk] = []
        self._embedder = TfidfEmbedder()
        self._vectors: np.ndarray | None = None
        self._dirty = True

    def _rebuild(self) -> None:
        if not self._chunks:
            self._vectors = None
            self._dirty = False
            return
        self._embedder.fit([c.text for c in self._chunks])
        self._vectors = self._embedder.embed_many([c.text for c in self._chunks])
        self._dirty = False

    def add(self, doc_id: str, filename: str, chunks: list[str]) -> int:
        base = len(self._chunks)
        for i, text in enumerate(chunks):
            self._chunks.append(
                Chunk(
                    id=f"{doc_id}:{i}",
                    doc_id=doc_id,
                    filename=filename,
                    text=text,
                    vector=np.zeros(0),
                    metadata={},
                )
            )
        self._dirty = True
        return len(chunks)

    def _ensure(self) -> None:
        if self._dirty or self._vectors is None:
            self._rebuild()

    def _keyword_score(self, query: str, text: str) -> float:
        q_terms = set(re.findall(r"[a-z0-9]+", query.lower()))
        if not q_terms:
            return 0.0
        t_terms = set(re.findall(r"[a-z0-9]+", text.lower()))
        if not t_terms:
            return 0.0
        overlap = len(q_terms & t_terms)
        return overlap / len(q_terms)

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        self._ensure()
        if not self._chunks:
            return []
        q_vec = self._embedder.embed(query)
        scored = []
        for i, chunk in enumerate(self._chunks):
            vec = self._vectors[i]
            sim = cosine_similarity(q_vec, vec)
            kw = self._keyword_score(query, chunk.text)
            score = 0.7 * sim + 0.3 * kw
            scored.append((score, i))
        scored.sort(key=lambda x: x[0], reverse=True)
        results = []
        for score, i in scored[:top_k]:
            c = self._chunks[i]
            results.append(
                {
                    "id": c.id,
                    "doc_id": c.doc_id,
                    "filename": c.filename,
                    "text": c.text,
                    "score": round(score, 4),
                    "metadata": c.metadata,
                }
            )
        return results
