import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from fastapi import FastAPI, UploadFile, File, HTTPException

from .storage import LocalStorage
from .jobs import JobStore
from .service import ETLService, MAX_FILE_SIZE
from .models import TransformRequest, DedupeRequest

app = FastAPI(title="Intelligent CSV/JSON ETL Service")

storage = LocalStorage(Path(__file__).resolve().parent / "data")
jobs = JobStore()
service = ETLService(storage, jobs)


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    try:
        job_id = await service.upload(file)
        return {"job_id": job_id, "status": "uploaded"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/transform/{job_id}")
def transform(job_id: str, req: TransformRequest):
    try:
        return service.transform(job_id, req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/schema/{job_id}")
def schema(job_id: str):
    try:
        return service.schema(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/dedupe/{job_id}")
def dedupe(job_id: str, req: DedupeRequest):
    try:
        return service.dedupe(job_id, req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/jobs")
def list_jobs():
    return [j.model_dump() for j in service.list_jobs()]


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("applications.etl_service.main:app", host="0.0.0.0", port=8000, reload=True)
