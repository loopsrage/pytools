import sys
import traceback
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import JSONResponse, Response, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.helpers import new_controller

from applications.agent_brain.core.settings import (
    get, all as all_settings, to_yaml_bytes, to_json_bytes, load
)
from applications.agent_brain.core.brain.store import BrainStore
from applications.agent_brain.ingest.scraper.store import ScrapeStore
from applications.agent_brain.ingest.scraper.scraper import WebScraper
from superlative_times.superlative_times import SuperlativeTimes
from applications.agent_brain.core.brain.robots import RobotsChecker
from applications.agent_brain.core.brain.models import Topic, SearchResult
from applications.agent_brain.core.brain.orchestrator import BrainOrchestrator
from applications.agent_brain.ingest.qdrant_ingest.ingest import (
    QdrantIngestPipeline, QdrantIngestConfig,
)
from applications.agent_brain.mcp.server import (
    AgentBrainMCPServer, MCP_SERVER_NAME, MCP_SERVER_VERSION,
)
from applications.agent_brain.models.model_registry import set_models_dir
from applications.agent_brain.models.model_harness import ThreadSafeModelHarness
from applications.agent_brain.models.architectures import *  # noqa: F401  (registers architectures)
from applications.agent_brain.mcp.fs_jail import (
    AgentBrainFsJailMCPServer, MCP_SERVER_NAME as FS_JAIL_MCP_SERVER_NAME,
    MCP_SERVER_VERSION as FS_JAIL_MCP_SERVER_VERSION,
)
from applications.agent_brain.ops.audit import AgentAuditLog
from applications.agent_brain.ops.resilience import AgentResilience
from applications.agent_brain.ops.feature_flags import AgentFeatureFlags
from applications.agent_brain.ops.secrets import AgentSecrets
from applications.agent_brain.ops.webhook import AgentWebhookQueue
from applications.agent_brain.core.email import GmailSender, EmailTemplateStore

load()

app = FastAPI(title=get("api.title", "Agent Brain - Site Intelligence Pipeline"))

BASE = Path(__file__).resolve().parent
# Set models directory for the model registry
set_models_dir(BASE / "models" / "store")

brain_data_dir = Path(get("brain.base_dir", BASE / "data" / "brain"))
scrape_data_dir = Path(get("brain.scrape_data_dir", BASE / "data" / "scrapes"))

brain_store = BrainStore(brain_data_dir)
scrape_store = ScrapeStore(scrape_data_dir)
scraper = WebScraper(
    scrape_store, SuperlativeTimes(),
    headless=get("scraper.headless", True),
    timeout_seconds=int(get("scraper.timeout_seconds", 30)),
    user_agent=get("scraper.user_agent", "agent-brain-bot"),
)
robots = RobotsChecker()
orchestrator = BrainOrchestrator(brain_store, scrape_store, scraper,
                                 SuperlativeTimes(), robots)

# Job boards from settings (central location for all job board searches)
job_boards_config = all_settings().get("agent_brain", {}).get("job_boards", [])
scrape_producer_interval = int(get("scrape_producer.interval_seconds", 300))

# Scrape queue for periodic scraping (Python runtime cron)
scrape_queue = new_controller(
    identity="scrape_queue",
    action=lambda qd: None,
    worker_count=2,
)


async def _scrape_all_due_safe():
    try:
        return await orchestrator.scrape_all_due()
    except Exception as exc:
        traceback.print_exc()
        return None


scrape_producer = None

# Search producer config (periodic crawl + search every 24h)
search_terms_config = all_settings().get("agent_brain", {}).get("search", {})
search_terms = search_terms_config.get("terms", [
    "ai", "ml", "software engineering", "developer", "researcher"])
search_producer_interval = int(search_terms_config.get(
    "interval_seconds", 86400))

# Search results storage directory
search_data_dir = brain_data_dir / "searches"
search_data_dir.mkdir(parents=True, exist_ok=True)


def _load_previous_search_results() -> dict:
    """Load all previous search results, keyed by term."""
    results: dict[str, list[dict]] = {}
    try:
        for path in brain_store.list_files("searches"):
            if path.endswith(".json"):
                data = brain_store.read_json(path)
                term = data.get("term", "")
                results.setdefault(term, []).append(data)
    except Exception:
        pass
    return results


def _save_search_result(result: SearchResult) -> None:
    """Persist a single search result as JSON."""
    brain_store.save_json(f"searches/{result.search_id}.json",
                          result.model_dump())


async def _run_periodic_search():
    """Crawl all topics, wait for Qdrant indexing, then search each term."""
    try:
        # Step 1: Crawl all topics
        await orchestrator.scrape_all_due()

        # Step 2: Wait for Qdrant pipeline to index new pages
        await asyncio.sleep(120)

        # Step 3: Load previous results for comparison
        previous = _load_previous_search_results()
        previous_ids: dict[str, set[str]] = {}
        for term, results in previous.items():
            previous_ids[term] = {
                r.get("qdrant_point_id", "") for r in results
                if r.get("qdrant_point_id")
            }

        # Step 4: Search each term and store results
        for term in search_terms:
            try:
                search_results = await mcp_server._qdrant_search({
                    "query": term,
                    "top_k": 20,
                })
                if "error" in search_results:
                    continue

                new_ids = previous_ids.get(term, set())
                for item in search_results.get("results", []):
                    payload = item.get("payload", {})
                    point_id = item.get("id", "")
                    is_new = point_id not in new_ids

                    result = SearchResult(
                        term=term,
                        qdrant_point_id=point_id,
                        title=payload.get("title"),
                        url=payload.get("url"),
                        topic_id=payload.get("topic_id"),
                        score=item.get("score"),
                        snippet=(payload.get("text", "") or "")[:300],
                        new=is_new,
                        seen=False,
                    )
                    _save_search_result(result)

            except Exception as exc:
                traceback.print_exc()
                continue

    except Exception as exc:
        traceback.print_exc()


search_producer = None

qdrant_config = QdrantIngestConfig(
    qdrant_path=str(BASE / "data" / ".qdrant_brain"),
    qdrant_collection=get("qdrant.collection", "agent_brain_chunks"),
    vector_size=int(get("qdrant.vector_size", 384)),
    scan_interval_seconds=float(get("qdrant.scan_interval_seconds", 60.0)),
    chunk_size=int(get("qdrant.chunk_size", 500)),
    chunk_overlap=int(get("qdrant.chunk_overlap", 100)),
    brain_base_dir=str(brain_data_dir),
)
qdrant_pipeline = QdrantIngestPipeline(qdrant_config)

model_harness = ThreadSafeModelHarness()

# Operations tooling
audit_log = AgentAuditLog()
resilience = AgentResilience()
feature_flags = AgentFeatureFlags()
secrets = AgentSecrets()
webhook_queue = AgentWebhookQueue()

# Email capability (Gmail sender + Qdrant-backed templates)
email_sender = GmailSender(secrets=secrets)
template_store = EmailTemplateStore()

mcp_server = AgentBrainMCPServer(
    orchestrator=orchestrator,
    qdrant_pipeline=qdrant_pipeline,
    brain_store=brain_store,
    model_harness=model_harness,
    audit_log=audit_log,
    resilience=resilience,
    feature_flags=feature_flags,
    secrets=secrets,
    webhook_queue=webhook_queue,
    email_sender=email_sender,
    template_store=template_store,
)

fs_jail_mcp_server = AgentBrainFsJailMCPServer(
    base_dir=Path(get("fs_jail.base_dir", BASE / "data" / "brain" / "fs_jails")),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scrape_producer
    # Register job boards from central config
    for jb in job_boards_config:
        existing = [t for t in orchestrator.list_topics() if str(t.url) == jb["url"]]
        if not existing:
            await orchestrator.register_topic(
                name=jb["name"],
                url=jb["url"],
                scrape_interval_seconds=int(jb.get("scrape_interval_seconds", 3600)),
                crawl=bool(jb.get("crawl", True)),
                max_pages=int(jb.get("max_pages", 20)),
                ignore_robots=bool(jb.get("ignore_robots", False)),
            )
    # Create scrape PeriodicProducer inside the running event loop
    scrape_producer = PeriodicProducer(
        action=_scrape_all_due_safe,
        queue=scrape_queue,
        interval=scrape_producer_interval,
        start_now=False,
    )

    # Create search PeriodicProducer (24h periodic crawl + search)
    search_producer = PeriodicProducer(
        action=_run_periodic_search,
        queue=scrape_queue,
        interval=search_producer_interval,
        start_now=False,
    )
    await qdrant_pipeline.start_async()
    yield
    await qdrant_pipeline.stop()
    scrape_producer.close()
    search_producer.close()
    await webhook_queue.close()


app.router.lifespan_context = lifespan


@app.get("/settings")
def settings():
    """Return all agent_brain settings as JSON."""
    return all_settings()


@app.get("/settings/yaml")
def settings_yaml():
    """Return all agent_brain settings as YAML."""
    return Response(content=to_yaml_bytes(), media_type="application/yaml")


@app.post("/settings")
def update_settings(settings: dict):
    """Update agent_brain settings from the UI control panel."""
    from applications.agent_brain.core.settings import fs_settings, load
    load()
    fs = fs_settings()
    import io, yaml
    # Persist to settings.yaml
    yaml_bytes = yaml.dump(settings, sort_keys=False).encode("utf-8")
    fs.write("settings.yaml", io.BytesIO(yaml_bytes), use_pipe=False)
    # Reload into the settings index
    load()
    return {"status": "ok"}


@app.get("/settings/json")
def settings_json():
    """Return all agent_brain settings as JSON bytes."""
    return Response(content=to_json_bytes(), media_type="application/json")


@app.post("/settings/resume")
async def upload_resume(file: UploadFile = File(...)):
    """Upload a resume file to attach to job application emails."""
    from applications.agent_brain.core.settings import fs_settings, load
    import io, yaml, os

    allowed = {".pdf", ".doc", ".docx", ".txt"}
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext or 'none'}")

    content = await file.read()
    max_bytes = 10 * 1024 * 1024
    if len(content) > max_bytes:
        raise HTTPException(status_code=400, detail="File too large. Maximum size is 10 MB.")

    resume_dir = BASE / "data" / "resumes"
    resume_dir.mkdir(parents=True, exist_ok=True)
    dest = resume_dir / file.filename
    dest.write_bytes(content)

    # Persist resume_path into settings.yaml email section
    load()
    fs = fs_settings()
    settings = all_settings()
    email_cfg = settings.setdefault("agent_brain", {}).setdefault("email", {})
    email_cfg["resume_path"] = str(dest)
    yaml_bytes = yaml.dump(settings, sort_keys=False).encode("utf-8")
    fs.write("settings.yaml", io.BytesIO(yaml_bytes), use_pipe=False)
    load()

    return {"filename": file.filename, "path": str(dest), "size": len(content)}


@app.get("/settings/resume")
def get_resume():
    """Return the configured resume path, if any."""
    from applications.agent_brain.core.settings import get
    path = get("email.resume_path", "")
    if not path:
        raise HTTPException(status_code=404, detail="No resume configured")
    return PlainTextResponse(content=path, media_type="text/plain")


@app.post("/topics")
async def register_topic(name: str, url: str,
                         scrape_interval_seconds: int | None = None,
                         crawl: bool = True,
                         max_pages: int = 20,
                         ignore_robots: bool = False):
    interval = scrape_interval_seconds or get(
        "scraper.default_interval_seconds", 3600)
    try:
        topic = await orchestrator.register_topic(
            name, url, interval, crawl=crawl, max_pages=max_pages,
            ignore_robots=ignore_robots)
        return topic.model_dump(mode="json")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/topics")
def list_topics():
    return [t.model_dump(mode="json") for t in orchestrator.list_topics()]


@app.post("/topics/{topic_id}/scrape")
async def scrape_topic(topic_id: str):
    topics = {t.topic_id: t for t in orchestrator.list_topics()}
    if topic_id not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
    try:
        return await orchestrator.scrape_topic(topics[topic_id])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/topics/{topic_id}/crawl")
async def crawl_topic(topic_id: str):
    topics = {t.topic_id: t for t in orchestrator.list_topics()}
    if topic_id not in topics:
        raise HTTPException(status_code=404, detail="Topic not found")
    try:
        return await orchestrator.crawl_topic(topics[topic_id])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/scrape-all")
async def scrape_all():
    try:
        return await orchestrator.scrape_all_due()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/topics/{topic_id}/pages")
def list_pages(topic_id: str):
    pages = []
    try:
        for path in brain_store.list_files("pages"):
            if path.endswith(".json"):
                data = brain_store.read_json(path)
                if data.get("topic_id") == topic_id:
                    pages.append(data)
    except Exception:
        pass
    return pages


@app.get("/topics/{topic_id}/artifacts")
def list_artifacts(topic_id: str):
    artifacts = []
    try:
        for path in brain_store.list_files("artifacts"):
            if path.endswith(".json"):
                data = brain_store.read_json(path)
                if data.get("topic_id") == topic_id:
                    artifacts.append(data)
    except Exception:
        pass
    return artifacts


@app.get("/artifacts/{artifact_id}/content")
def get_artifact_content(artifact_id: str):
    try:
        meta = brain_store.read_json(f"artifacts/{artifact_id}.json")
        path = meta.get("stored_path", "")
        if path.endswith(".csv"):
            return {"content": brain_store.read_text(path), "format": "csv"}
        return {"content": brain_store.read_json(path), "format": "json"}
    except Exception:
        raise HTTPException(status_code=404, detail="Artifact not found")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "topics": len(orchestrator.list_topics()),
        "qdrant_stats": qdrant_pipeline.stats(),
    }


@app.post("/qdrant/start")
async def qdrant_start():
    """Manually start the Qdrant ingestion pipeline."""
    try:
        await qdrant_pipeline.start_async()
        return {"status": "started"}
    except RuntimeError:
        return {"status": "already_running"}


@app.post("/qdrant/stop")
async def qdrant_stop():
    """Stop the Qdrant ingestion pipeline."""
    await qdrant_pipeline.stop()
    return {"status": "stopped"}


@app.get("/qdrant/stats")
def qdrant_stats():
    """Return Qdrant ingestion pipeline statistics."""
    return qdrant_pipeline.stats()


# ------------------------------------------------------------------
# Search results endpoints
# ------------------------------------------------------------------
@app.get("/searches")
def list_searches(term: str | None = None, unseen_only: bool = False):
    """List search results, optionally filtered by term and unseen_only."""
    results = []
    try:
        for path in brain_store.list_files("searches"):
            if path.endswith(".json"):
                data = brain_store.read_json(path)
                if term and data.get("term") != term:
                    continue
                if unseen_only and not data.get("new", False):
                    continue
                results.append(data)
    except Exception:
        pass
    results.sort(key=lambda x: x.get("searched_at", ""), reverse=True)
    return results


@app.post("/searches/{search_id}/seen")
def mark_search_seen(search_id: str):
    """Mark a search result as seen."""
    try:
        data = brain_store.read_json(f"searches/{search_id}.json")
        data["seen"] = True
        data["new"] = False
        brain_store.save_json(f"searches/{search_id}.json", data)
        return {"status": "ok", "search_id": search_id}
    except Exception:
        raise HTTPException(status_code=404, detail="Search result not found")


@app.post("/searches/mark-all-seen")
def mark_all_searches_seen():
    """Mark all search results as seen."""
    count = 0
    try:
        for path in brain_store.list_files("searches"):
            if path.endswith(".json"):
                data = brain_store.read_json(path)
                if not data.get("seen", False):
                    data["seen"] = True
                    data["new"] = False
                    brain_store.save_json(path, data)
                    count += 1
    except Exception:
        pass
    return {"status": "ok", "marked": count}


@app.post("/searches/run-now")
async def run_search_now():
    """Manually trigger the periodic search (crawl + search all terms)."""
    await _run_periodic_search()
    return {"status": "ok"}



@app.post("/api/mcp")
async def mcp_endpoint(request: Request):
    """MCP JSON-RPC 2.0 endpoint for agent control."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"jsonrpc": "2.0", "id": None,
                     "error": {"code": -32700, "message": "Parse error"}},
        )
    response = mcp_server.handle_rpc(body)
    if response is None:
        return JSONResponse(status_code=200, content={})
    return JSONResponse(status_code=200, content=response)


@app.get("/api/mcp")
def mcp_info():
    """MCP server info - returns tools manifest."""
    return {
        "server": {
            "name": MCP_SERVER_NAME,
            "version": MCP_SERVER_VERSION,
        },
        "tools": mcp_server.get_tools_manifest(),
        "resources": mcp_server.get_resources_manifest(),
        "prompts": mcp_server.get_prompts_manifest(),
        "endpoint": "POST /api/mcp (JSON-RPC 2.0)",
    }


@app.post("/api/mcp/fs_jail")
async def fs_jail_mcp_endpoint(request: Request):
    """MCP JSON-RPC 2.0 endpoint for jailed filesystem access."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"jsonrpc": "2.0", "id": None,
                     "error": {"code": -32700, "message": "Parse error"}},
        )
    response = fs_jail_mcp_server.handle_rpc(body)
    if response is None:
        return JSONResponse(status_code=200, content={})
    return JSONResponse(status_code=200, content=response)


@app.get("/api/mcp/fs_jail")
def fs_jail_mcp_info():
    """Jailed filesystem MCP server info - returns tools manifest."""
    return {
        "server": {
            "name": FS_JAIL_MCP_SERVER_NAME,
            "version": FS_JAIL_MCP_SERVER_VERSION,
        },
        "tools": fs_jail_mcp_server.get_tools_manifest(),
        "resources": fs_jail_mcp_server.get_resources_manifest(),
        "prompts": fs_jail_mcp_server.get_prompts_manifest(),
        "endpoint": "POST /api/mcp/fs_jail (JSON-RPC 2.0)",
    }


# Serve the built React UI (control panel) from /ui
UI_DIST = Path(__file__).resolve().parent / "ui" / "dist"
if UI_DIST.exists():
    app.mount("/ui", StaticFiles(directory=str(UI_DIST), html=True), name="ui")


@app.get("/ui/{full_path:path}")
async def ui_catch_all(full_path: str):
    """Serve the React app for any client-side route under /ui."""
    from fastapi.responses import FileResponse
    index = UI_DIST / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"detail": "UI not built"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("applications.agent_brain.main:app",
                host=get("api.host", "0.0.0.0"),
                port=int(get("api.port", 8127)),
                reload=True)
