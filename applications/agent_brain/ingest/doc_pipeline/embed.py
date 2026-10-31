import math
import re
from collections import Counter
from typing import Optional

import numpy as np


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class TfidfEmbedder:
    def __init__(self):
        self._df: Counter = Counter()
        self._idf: dict[str, float] = {}
        self._vocab: list[str] = []
        self._fitted = False

    def fit(self, texts: list[str]) -> None:
        doc_counts: list[Counter] = []
        for text in texts:
            tokens = _tokenize(text)
            doc_counts.append(Counter(set(tokens)))
            self._df.update(Counter(set(tokens)))
        n = len(texts)
        self._idf = {
            term: math.log((n + 1) / (df + 1)) + 1.0 for term, df in self._df.items()
        }
        self._vocab = sorted(self._idf.keys())
        self._fitted = True

    def _tf_vector(self, tokens: list[str]) -> dict[str, float]:
        total = max(len(tokens), 1)
        counts = Counter(tokens)
        return {term: cnt / total for term, cnt in counts.items()}

    def embed(self, text: str) -> np.ndarray:
        if not self._fitted:
            self.fit([text])
        tokens = _tokenize(text)
        tf = self._tf_vector(tokens)
        dim = len(self._vocab)
        index = {term: i for i, term in enumerate(self._vocab)}
        vec = np.zeros(dim, dtype=np.float64)
        norm = 0.0
        for term, val in tf.items():
            if term in index:
                weighted = val * self._idf[term]
                vec[index[term]] = weighted
                norm += weighted * weighted
        norm = math.sqrt(norm) or 1.0
        return vec / norm

    def embed_many(self, texts: list[str]) -> np.ndarray:
        if not self._fitted:
            self.fit(texts)
        return np.vstack([self.embed(t) for t in texts])

    @property
    def dim(self) -> int:
        return len(self._vocab)


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) or 1.0
    return float(np.dot(a, b) / denom)
