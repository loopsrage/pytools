import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import uuid
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, HTTPException

from .store import ScrapeStore
from .scraper import WebScraper
from superlative_times.superlative_times import SuperlativeTimes
from .models import ScrapeRequest, ScrapeResult, ScrapeListItem, ScrapeStatus

app = FastAPI(title="Web Scraper Service")

store = ScrapeStore(Path(__file__).resolve().parent / "data")
index = SuperlativeTimes()
scraper = WebScraper(store, index)


@app.post("/scrape", response_model=ScrapeResult)
async def scrape(req: ScrapeRequest):
    scrape_id = str(uuid.uuid4())
    try:
        record = await scraper.scrape(str(req.url), wait_until=req.wait_until)
    except Exception as exc:
        return ScrapeResult(
            scrape_id=scrape_id,
            url=str(req.url),
            status=ScrapeStatus.FAILED,
            created_at=datetime.utcnow(),
            error=str(exc),
        )

    stored_path = None
    try:
        store.save_text(f"scrapes/{scrape_id}.txt", record["text"] or "")
        store.save_text(f"scrapes/{scrape_id}.meta", str({
            "url": record["url"],
            "title": record["title"],
            "hash": record["hash"],
            "status_code": record["status_code"],
            "created_at": record["created_at"].isoformat(),
            "is_duplicate": record["is_duplicate"],
            "similarity": record["similarity"],
        }))
        stored_path = f"scrapes/{scrape_id}.txt"
    except Exception:
        pass

    return ScrapeResult(
        scrape_id=scrape_id,
        url=record["url"],
        status=ScrapeStatus.COMPLETED,
        title=record["title"],
        text=record["text"],
        content_hash=record["hash"],
        is_duplicate=record["is_duplicate"],
        similarity_to_existing=record["similarity"],
        created_at=record["created_at"],
        stored_path=stored_path,
    )


@app.get("/scrapes/{scrape_id}", response_model=ScrapeListItem)
async def get_scrape(scrape_id: str):
    try:
        meta = store.read_text(f"scrapes/{scrape_id}.meta")
    except Exception:
        raise HTTPException(status_code=404, detail="Scrape not found")
    import ast
    data = ast.literal_eval(meta)
    return ScrapeListItem(
        scrape_id=scrape_id,
        url=data["url"],
        status=ScrapeStatus.COMPLETED,
        title=data.get("title"),
        is_duplicate=data.get("is_duplicate", False),
        created_at=datetime.fromisoformat(data["created_at"]),
    )


@app.get("/scrapes")
def list_scrapes():
    ids = []
    for p in store.list_files("scrapes"):
        if p.endswith(".meta"):
            ids.append(Path(p).stem.replace(".meta", ""))
    return ids


@app.get("/health")
def health():
    return {"status": "ok", "scrapes": len(scraper._previous)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("applications.web_scraper.main:app", host="0.0.0.0", port=8125, reload=True)
