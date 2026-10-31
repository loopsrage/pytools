import threading
import torch
import torch.nn as nn

from applications.agent_brain.core.brain.models import url_hash, content_hash
from applications.agent_brain.models.model_registry import ModelRegistry, set_models_dir
from applications.agent_brain.models.model_harness import (
    ThreadSafeModelHarness,
    register_architecture,
)


class TinyClassifier(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc = nn.Linear(10, 3)

    def forward(self, x):
        return self.fc(x)


class TinyEmbedder(nn.Module):
    def __init__(self):
        super().__init__()
        self.proj = nn.Linear(8, 4)

    def forward(self, x):
        return self.proj(x)


def test_registry_scans_models(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    # sentiment-classifier (pt)
    sc = models_dir / "sentiment-classifier"
    sc.mkdir()
    m = TinyClassifier()
    torch.save(m.state_dict(), sc / "model.pt")
    (sc / "config.json").write_text(
        '{"name":"sentiment-classifier","framework":"pytorch","format":"pt",'
        '"architecture":"TinyClassifier","input_dim":10,"output_dim":3,'
        '"description":"test classifier"}'
    )
    # text-embedder (safetensors)
    te = models_dir / "text-embedder"
    te.mkdir()
    from safetensors.torch import save_file
    e = TinyEmbedder()
    save_file({k: v for k, v in e.state_dict().items()}, te / "model.safetensors")
    (te / "config.json").write_text(
        '{"name":"text-embedder","framework":"pytorch","format":"safetensors",'
        '"architecture":"TinyEmbedder","input_dim":8,"output_dim":4,'
        '"description":"test embedder"}'
    )
    # A folder without config.json should be ignored
    (models_dir / "junk").mkdir()

    reg = ModelRegistry(str(models_dir))
    ids = [m.model_id for m in reg.list()]
    assert ids == ["sentiment-classifier", "text-embedder"]
    assert reg.count() == 2
    info = reg.get("sentiment-classifier")
    assert info is not None
    assert info.format == "pt"
    assert info.architecture == "TinyClassifier"
    assert reg.get("nonexistent") is None


def test_harness_load_forward_unload(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sc = models_dir / "sentiment-classifier"
    sc.mkdir()
    m = TinyClassifier()
    torch.save(m.state_dict(), sc / "model.pt")
    (sc / "config.json").write_text(
        '{"name":"sentiment-classifier","framework":"pytorch","format":"pt",'
        '"architecture":"TinyClassifier","input_dim":10,"output_dim":3,'
        '"description":"test"}'
    )

    register_architecture("TinyClassifier", TinyClassifier)
    set_models_dir(str(models_dir))
    h = ThreadSafeModelHarness()
    assert h.model_count() == 1
    assert h.active_model("agent-1") is None

    loaded = h.load_model("agent-1", "sentiment-classifier")
    assert loaded["model_id"] == "sentiment-classifier"
    assert h.is_loaded("agent-1")
    assert h.active_model_id("agent-1") == "sentiment-classifier"

    out = h.run_action("agent-1", "forward", [0.5] * 10)
    assert "output" in out
    assert len(out["output"]) == 3

    tensors = h.run_action("agent-1", "tensors")
    assert len(tensors["tensors"]) > 0

    info = h.run_action("agent-1", "info")
    assert info["active"]["model_id"] == "sentiment-classifier"

    # Hot swap - load same model again (replace active)
    loaded2 = h.load_model("agent-1", "sentiment-classifier")
    assert loaded2["model_id"] == "sentiment-classifier"

    # Unload
    unloaded = h.unload_model("agent-1")
    assert unloaded["model_id"] == "sentiment-classifier"
    assert h.active_model("agent-1") is None
    assert h.active_model_id("agent-1") is None

    # Running action with no model
    result = h.run_action("agent-1", "forward", [0.5] * 10)
    assert "error" in result

    # Unknown action
    h.load_model("agent-1", "sentiment-classifier")
    result = h.run_action("agent-1", "bogus")
    assert "error" in result

    # Missing model
    try:
        h.load_model("agent-1", "does-not-exist")
    except ValueError as exc:
        assert "not found" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_concurrent_agents_isolated(tmp_path):
    """Two agents can load different models independently."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sc = models_dir / "sentiment-classifier"
    sc.mkdir()
    m = TinyClassifier()
    torch.save(m.state_dict(), sc / "model.pt")
    (sc / "config.json").write_text(
        '{"name":"sentiment-classifier","framework":"pytorch","format":"pt",'
        '"architecture":"TinyClassifier","input_dim":10,"output_dim":3,'
        '"description":"test"}'
    )
    te = models_dir / "text-embedder"
    te.mkdir()
    from safetensors.torch import save_file
    e = TinyEmbedder()
    save_file({k: v for k, v in e.state_dict().items()}, te / "model.safetensors")
    (te / "config.json").write_text(
        '{"name":"text-embedder","framework":"pytorch","format":"safetensors",'
        '"architecture":"TinyEmbedder","input_dim":8,"output_dim":4,'
        '"description":"test embedder"}'
    )

    register_architecture("TinyClassifier", TinyClassifier)
    register_architecture("TinyEmbedder", TinyEmbedder)
    set_models_dir(str(models_dir))
    h = ThreadSafeModelHarness()

    h.load_model("agent-a", "sentiment-classifier")
    h.load_model("agent-b", "text-embedder")

    assert h.active_model_id("agent-a") == "sentiment-classifier"
    assert h.active_model_id("agent-b") == "text-embedder"
    assert h.agent_count() == 2

    out_a = h.run_action("agent-a", "forward", [0.5] * 10)
    out_b = h.run_action("agent-b", "forward", [0.3] * 8)
    assert len(out_a["output"]) == 3
    assert len(out_b["output"]) == 4

    # Unloading one agent does not affect the other
    h.unload_model("agent-a")
    assert h.active_model("agent-a") is None
    assert h.active_model_id("agent-b") == "text-embedder"


def test_concurrent_load_deduplication(tmp_path):
    """Loading the same model for the same agent concurrently should only load once."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sc = models_dir / "sentiment-classifier"
    sc.mkdir()
    m = TinyClassifier()
    torch.save(m.state_dict(), sc / "model.pt")
    (sc / "config.json").write_text(
        '{"name":"sentiment-classifier","framework":"pytorch","format":"pt",'
        '"architecture":"TinyClassifier","input_dim":10,"output_dim":3,'
        '"description":"test"}'
    )

    register_architecture("TinyClassifier", TinyClassifier)
    set_models_dir(str(models_dir))
    h = ThreadSafeModelHarness()

    load_count = 0
    original_load = h._load_module

    def counting_load(info):
        nonlocal load_count
        load_count += 1
        return original_load(info)

    h._load_module = counting_load

    # Concurrent loads from multiple threads
    results = []
    errors = []

    def worker():
        try:
            res = h.load_model("agent-x", "sentiment-classifier")
            results.append(res)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Errors: {errors}"
    assert len(results) == 10
    # Only one actual load should have happened (deduplication)
    assert load_count == 1, f"Expected 1 load, got {load_count}"
    assert h.active_model_id("agent-x") == "sentiment-classifier"


def test_harness_safetensors(tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    te = models_dir / "text-embedder"
    te.mkdir()
    from safetensors.torch import save_file
    e = TinyEmbedder()
    save_file({k: v for k, v in e.state_dict().items()}, te / "model.safetensors")
    (te / "config.json").write_text(
        '{"name":"text-embedder","framework":"pytorch","format":"safetensors",'
        '"architecture":"TinyEmbedder","input_dim":8,"output_dim":4,'
        '"description":"test embedder"}'
    )

    register_architecture("TinyEmbedder", TinyEmbedder)
    set_models_dir(str(models_dir))
    h = ThreadSafeModelHarness()
    h.load_model("agent-1", "text-embedder")
    assert h.active_model("agent-1")["format"] == "safetensors"
    out = h.run_action("agent-1", "forward", [0.3] * 8)
    assert len(out["output"]) == 4


# ------------------------------------------------------------------
# Dual hash layer tests
# ------------------------------------------------------------------
def test_url_hash_deterministic():
    u1 = url_hash("https://example.com/page")
    u2 = url_hash("https://example.com/page")
    u3 = url_hash("https://example.com/other")
    assert u1 == u2
    assert u1 != u3
    assert len(u1) == 64  # SHA-256 hex


def test_content_hash_deterministic():
    c1 = content_hash("hello world")
    c2 = content_hash("hello world")
    c3 = content_hash("hello world!")
    assert c1 == c2
    assert c1 != c3
    assert len(c1) == 16  # truncated SHA-256


def test_page_model_has_url_hash():
    from applications.agent_brain.core.brain.models import Page
    p = Page(
        topic_id="topic-1",
        url="https://example.com/page",
        text="some content",
    )
    assert p.url_hash == url_hash("https://example.com/page")
    assert p.content_hash == content_hash("some content")
