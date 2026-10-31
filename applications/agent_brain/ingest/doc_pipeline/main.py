import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel

from .doc_store import DocumentStore
from .index import HybridIndex
from .rag import RAGPipeline

app = FastAPI(title="Document RAG Pipeline")

store = DocumentStore(Path(__file__).resolve().parent / "data")
index = HybridIndex()
pipeline = RAGPipeline(store, index)


class IngestResponse(BaseModel):
    doc_id: str
    filename: str
    chunks: int
    chars: int


class QueryRequest(BaseModel):
    question: str
    top_k: int = 5


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict]


@app.post("/ingest", response_model=IngestResponse)
async def ingest(file: UploadFile = File(...)):
    try:
        data = await file.read()
        result = await pipeline.ingest(data, file.filename or "upload.bin")
        return IngestResponse(
            doc_id=result.doc_id,
            filename=result.filename,
            chunks=result.chunks,
            chars=result.chars,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    try:
        result = await pipeline.query(req.question, top_k=req.top_k)
        return QueryResponse(answer=result.answer, sources=result.sources)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/docs")
def list_docs():
    return store.list_docs("docs")


@app.get("/health")
def health():
    return {"status": "ok", "chunks": len(index._chunks)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("applications.doc_pipeline.main:app", host="0.0.0.0", port=8124, reload=True)
