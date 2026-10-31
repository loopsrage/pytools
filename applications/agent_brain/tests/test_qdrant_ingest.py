from applications.agent_brain.ingest.qdrant_ingest.ingest import (
    QdrantIngestConfig,
    QdrantIngestPipeline,
)
from applications.agent_brain.core.brain.models import url_hash, content_hash


def test_config_defaults():
    cfg = QdrantIngestConfig()
    assert cfg.qdrant_collection == "agent_brain_chunks"
    assert cfg.vector_size == 384
    assert cfg.scan_interval_seconds == 60.0
    assert cfg.chunk_size == 500
    assert cfg.chunk_overlap == 100


def test_config_from_settings():
    cfg = QdrantIngestConfig(
        qdrant_collection="test_collection",
        vector_size=128,
        scan_interval_seconds=30.0,
        chunk_size=250,
        chunk_overlap=50,
    )
    assert cfg.qdrant_collection == "test_collection"
    assert cfg.vector_size == 128
    assert cfg.scan_interval_seconds == 30.0
    assert cfg.chunk_size == 250
    assert cfg.chunk_overlap == 50


def test_pipeline_initial_state():
    pipe = QdrantIngestPipeline()
    assert pipe._client is None
    assert pipe._service is None
    assert pipe.stats()["scans"] == 0
    assert pipe.stats()["items_scanned"] == 0
    assert pipe.stats()["chunks_created"] == 0
    assert pipe.stats()["points_upserted"] == 0
    assert pipe.stats()["errors"] == 0


def test_pipeline_custom_config():
    cfg = QdrantIngestConfig(
        qdrant_path="/tmp/test_qdrant",
        qdrant_collection="custom_coll",
        vector_size=64,
        scan_interval_seconds=10.0,
    )
    pipe = QdrantIngestPipeline(cfg)
    assert pipe.config.qdrant_path == "/tmp/test_qdrant"
    assert pipe.config.qdrant_collection == "custom_coll"
    assert pipe.config.vector_size == 64
    assert pipe.config.scan_interval_seconds == 10.0


def test_dual_hash_deterministic():
    uh1 = url_hash("https://example.com/page")
    uh2 = url_hash("https://example.com/page")
    assert uh1 == uh2
    assert len(uh1) == 64

    ch1 = content_hash("same content")
    ch2 = content_hash("same content")
    assert ch1 == ch2
    assert ch1 != content_hash("different content")
