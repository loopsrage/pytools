import re
import uuid
from dataclasses import dataclass


@dataclass
class IngestResult:
    doc_id: str
    filename: str
    chunks: int
    chars: int


@dataclass
class QueryResult:
    answer: str
    sources: list[dict]


class RAGPipeline:
    def __init__(self, store, index):
        self.store = store
        self.index = index

    async def ingest(self, data: bytes, filename: str, doc_id: str | None = None) -> IngestResult:
        if doc_id is None:
            doc_id = uuid.uuid4().hex

        key = f"docs/{doc_id}/{filename}"
        self.store.save_bytes(key, data)

        from .ocr import extract_text
        from .chunk import chunk_text

        text = extract_text(data, filename)
        chunks = chunk_text(text)
        self.index.add(doc_id, filename, chunks)

        return IngestResult(
            doc_id=doc_id,
            filename=filename,
            chunks=len(chunks),
            chars=len(text),
        )

    async def query(self, question: str, top_k: int = 5) -> QueryResult:
        results = self.index.search(question, top_k=top_k)
        answer = self._extractive_answer(question, results)
        return QueryResult(answer=answer, sources=results)

    def _extractive_answer(self, question: str, results: list[dict]) -> str:
        if not results:
            return "No relevant document content found."
        q_terms = set(re.findall(r"[a-z0-9]+", question.lower()))
        best = results[0]
        snippet = best["text"][:1000]
        return (
            f"Based on the indexed documents, the most relevant passage is from "
            f"{best['filename']}:\n\n{snippet}"
        )
