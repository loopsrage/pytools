from applications.agent_brain.core.settings import (
    load, get, all as all_settings, to_yaml_bytes, to_json_bytes, fs_settings,
)


def test_settings_load_and_get():
    load()
    assert get("scraper.headless") is True
    assert get("scraper.user_agent") == "agent-brain-bot"
    assert get("scraper.timeout_seconds") == 60
    assert get("api.port") == 8127
    assert get("api.host") == "0.0.0.0"
    assert get("brain.base_dir") == "data/brain"
    assert get("etl.chunk_size") == 500
    assert get("doc_pipeline.hybrid_top_k") == 5
    assert get("logging.level") == "INFO"
    assert get("antigravity.collection") == "portfolio_antigravity_brain"
    assert get("benchmarks.collection") == "portfolio_neural_benchmarks"
    assert get("staging_qdrant.collection") == "agent_informational_graph_index"


def test_settings_default():
    load()
    assert get("nonexistent.key", "default") == "default"


def test_settings_all():
    load()
    settings = all_settings()
    assert "agent_brain" in settings
    assert "scraper" in settings["agent_brain"]
    assert "brain" in settings["agent_brain"]


def test_settings_yaml_export():
    load()
    yaml_bytes = to_yaml_bytes()
    assert b"agent_brain:" in yaml_bytes
    assert b"scraper:" in yaml_bytes


def test_settings_json_export():
    load()
    json_bytes = to_json_bytes()
    assert b'"agent_brain"' in json_bytes


def test_fs_settings():
    load()
    fs = fs_settings()
    assert fs is not None
