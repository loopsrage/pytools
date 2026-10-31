from __future__ import annotations

import json
import pickle
import shutil
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from qdrant_client.models import PointStruct

from applications.agent_brain.core.brain.options_router import (
    AgentBrainOptionsRuntime,
    set_brain_options_runtime,
)
from applications.agent_brain.main import app

AGENT_BRAIN_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture()
def isolated_runtime(tmp_path: Path):
    """
    Create an isolated real Qdrant-backed AgentBrainOptionsRuntime rooted in tmp_path
    so tests execute physical vector indexing, WebSocket streaming, FS CRUD, and MCP
    JSON-RPC 2.0 without mutating runtime data files.
    """
    brain_root = tmp_path / "brain"
    conv_dir = brain_root / "conv-alpha-001"
    logs_dir = conv_dir / ".system_generated" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    artifact_md = conv_dir / "architecture_plan.md"
    artifact_md.write_text(
        "# Geodesic Migration Plan\n\n```python\ndef compute_spinor():\n    return 42\n```\n",
        encoding="utf-8",
    )
    transcript = logs_dir / "transcript.jsonl"
    transcript.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "step_index": 0,
                        "source": "USER_EXPLICIT",
                        "type": "USER_INPUT",
                        "status": "DONE",
                        "created_at": "2026-10-31T12:00:00Z",
                        "content": "Explain the migrated agent_brain geodesic options.",
                    }
                ),
                json.dumps(
                    {
                        "step_index": 1,
                        "source": "MODEL",
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "created_at": "2026-10-31T12:00:05Z",
                        "thinking": "Inspecting Qdrant options router.",
                        "content": "All 4 options now run in toolset/applications/agent_brain.\n```python\nx = 1 + 1\n```",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    canonical_ledger = AGENT_BRAIN_DIR / "models" / "benchmark_history_ledger.json"
    data_ledger = AGENT_BRAIN_DIR / "data" / "benchmark_history_ledger.json"
    brain_ledger = tmp_path / "antigravity_brain_ledger.json"
    bench_ledger = tmp_path / "benchmark_history_ledger.json"
    if canonical_ledger.is_file():
        shutil.copy2(canonical_ledger, bench_ledger)
    elif data_ledger.is_file():
        shutil.copy2(data_ledger, bench_ledger)

    staging_root = tmp_path / "qdrant_staging"
    slot_coll_dir = staging_root / "storage" / "collection" / "unified_harness_benchmarks"
    slot_coll_dir.mkdir(parents=True, exist_ok=True)
    slot_conn = sqlite3.connect(str(slot_coll_dir / "storage.sqlite"))
    slot_conn.execute("CREATE TABLE points (id TEXT PRIMARY KEY, point BLOB)")
    for idx, harness_name in enumerate(("linear_harness", "hyperbolic_harness"), start=1):
        pt = PointStruct(
            id=f"00000000-0000-0000-0000-00000000000{idx}",
            vector=[0.0] * 384,
            payload={
                "point_id": f"00000000-0000-0000-0000-00000000000{idx}",
                "run_id": "run-slot-001",
                "run_timestamp": "2026-10-31T16:00:00+00:00",
                "model_slot_id": "promoted",
                "model_class": "PolyphonicStageCompilerCascadeModel",
                "slot_mode": "token_sequence_module",
                "param_count": 253809,
                "harness": harness_name,
                "test_id": "standard::xor",
                "benchmark": "xor",
                "dataset": "standard",
                "input_kind": "continuous",
                "loss_type": "mse",
                "split_digest": "deadbeefcafe1234",
                "test_passed": True,
                "learned": True,
                "loss_before": 0.42,
                "loss_after": 0.01,
                "test_loss": 0.012,
                "accuracy": 1.0,
                "train_seconds": 0.25,
            },
        )
        slot_conn.execute(
            "INSERT INTO points (id, point) VALUES (?, ?)",
            (str(pt.id), pickle.dumps(pt)),
        )
    slot_conn.commit()
    slot_conn.close()
    mini_projects = tmp_path / "mini_projects"
    mini_pkg = mini_projects / "sample_repo"
    mini_pkg.mkdir(parents=True, exist_ok=True)
    (mini_pkg / "README.md").write_text("# Sample System\nSpinor Strut Lattice reference.\n", encoding="utf-8")
    (mini_pkg / "engine.py").write_text(
        "class SampleGeodesicEngine:\n    def trace(self) -> str:\n        return 'spinor_strut_lattice'\n",
        encoding="utf-8",
    )

    skill_dir = tmp_path / "skill"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: sample-skill\ndescription: Sample test skill\n---\n# Sample Skill\nSpinor Strut Lattice.\n",
        encoding="utf-8",
    )

    nn_repo = Path("/Users/jarek/IdeaProjects/neural_networks")
    if not nn_repo.exists():
        nn_repo = tmp_path / "neural_networks"
        nn_repo.mkdir(parents=True, exist_ok=True)

    runtime = AgentBrainOptionsRuntime(
        qdrant_storage_path=tmp_path / "qdrant_options",
        brain_root=brain_root,
        brain_ledger_path=brain_ledger,
        brain_collection="test_antigravity_brain",
        neural_networks_repo_path=nn_repo,
        benchmark_ledger_path=bench_ledger,
        benchmark_collection="test_neural_benchmarks",
        staging_root=staging_root,
        idea_projects_root=mini_projects,
        skill_dir_path=skill_dir,
        informational_collection="test_informational_graph",
        watch_files=False,
        prefer_memory=True,
    )
    set_brain_options_runtime(runtime)
    try:
        yield runtime
    finally:
        set_brain_options_runtime(None)


def test_antigravity_brain_and_artifacts_endpoints_and_ws(isolated_runtime):
    client = TestClient(app)

    overview = client.get("/api/antigravity/overview")
    assert overview.status_code == 200
    ov_data = overview.json()
    assert ov_data["total_entries"] >= 2
    assert len(ov_data["entries"]) >= 2

    artifacts = client.get("/api/antigravity/artifacts?query=Geodesic")
    assert artifacts.status_code == 200
    art_data = artifacts.json()
    assert art_data["total_artifacts"] >= 1
    first_art = art_data["artifacts"][0]
    art_path = first_art.get("relative_path") or first_art.get("path")
    assert art_path

    content_res = client.get(f"/api/antigravity/files/content?path={art_path}")
    assert content_res.status_code == 200
    assert "compute_spinor" in content_res.json().get("content", "")

    dl_res = client.get(f"/api/antigravity/files/download?path={art_path}")
    assert dl_res.status_code == 200
    assert b"Geodesic Migration Plan" in dl_res.content

    entry_id = ov_data["entries"][0]["entry_id"]
    detail_res = client.get(f"/api/antigravity/entries/{entry_id}")
    assert detail_res.status_code == 200
    assert detail_res.json()["entry_id"] == entry_id

    with client.websocket_connect("/api/antigravity/ws/chat") as ws:
        snap = ws.receive_json()
        assert snap["type"] == "snapshot"
        assert snap["total_results"] >= 2
        ws.send_json({"type": "ping"})
        pong = ws.receive_json()
        assert pong["type"] == "pong"


def test_benchmarks_and_promoted_model_fs_and_ws(isolated_runtime):
    client = TestClient(app)

    ov_res = client.get("/api/benchmarks/overview")
    assert ov_res.status_code == 200
    ov = ov_res.json()
    assert ov["total_models"] >= 1
    promoted = ov.get("promoted_model") or ov.get("best_model")
    assert promoted is not None
    model_id = promoted["model_id"]

    model_res = client.get(f"/api/benchmarks/models/{model_id}")
    assert model_res.status_code == 200
    model_detail = model_res.json()
    assert model_detail["model_id"] == model_id
    assert isinstance(model_detail.get("benchmark_entries"), list)
    assert len(model_detail["benchmark_entries"]) >= 1
    assert isinstance(model_detail.get("qdrant_rejoin"), dict)

    slot_res = client.get("/api/benchmarks/slot-harnesses")
    assert slot_res.status_code == 200
    slot_body = slot_res.json()
    assert slot_body["available"] is True
    assert slot_body["total_points"] == 2
    assert slot_body["identical_splits_verified"] is True
    assert len(slot_body["summary_matrix"]) == 2
    assert {r["harness"] for r in slot_body["summary_matrix"]} == {
        "linear_harness",
        "hyperbolic_harness",
    }

    spec_res = client.get("/api/benchmarks/recall-spec?max_lines=15")
    assert spec_res.status_code == 200
    spec_body = spec_res.json()
    assert spec_body["exists"] is True
    assert spec_body["line_count"] > 6000
    assert len(spec_body["preview_lines"]) == 15

    search_res = client.get("/api/benchmarks/search?query=Polyphonic&top_k=10")
    assert search_res.status_code == 200
    assert isinstance(search_res.json().get("results"), list)

    status_res = client.get("/api/benchmarks/promoted/status")
    assert status_res.status_code == 200
    st = status_res.json()
    assert st.get("active_conversation_id")

    create_fs = client.post(
        "/api/benchmarks/promoted/fs",
        json={
            "action": "create",
            "path": "artifacts/migrated_note.md",
            "content": "Verified FS CRUD inside agent_brain.",
            "scope": "workspace",
        },
    )
    assert create_fs.status_code == 200
    assert create_fs.json()["status"] in ("saved", "ok")

    read_fs = client.get(
        "/api/benchmarks/promoted/fs?action=read&path=artifacts/migrated_note.md&scope=workspace"
    )
    assert read_fs.status_code == 200
    assert "Verified FS CRUD inside agent_brain." in read_fs.json().get("content", "")

    del_fs = client.post(
        "/api/benchmarks/promoted/fs",
        json={
            "action": "delete",
            "path": "artifacts/migrated_note.md",
            "scope": "workspace",
        },
    )
    assert del_fs.status_code == 200
    assert del_fs.json()["status"] in ("deleted", "ok")

    with client.websocket_connect("/api/benchmarks/promoted/ws/chat") as ws:
        snap = ws.receive_json()
        assert snap["type"] == "snapshot"
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_staging_qdrant_and_geodesic_mcp_and_ui_spa(isolated_runtime):
    client = TestClient(app)

    sys_res = client.get("/api/qdrant/staging/systems")
    assert sys_res.status_code == 200
    assert "systems" in sys_res.json()

    jux_res = client.get("/api/qdrant/staging/juxtaposition?query=Spinor")
    assert jux_res.status_code == 200

    mcp_info = client.get("/api/mcp/geodesic")
    assert mcp_info.status_code == 200
    tool_names = {t["name"] for t in mcp_info.json().get("tools", [])}
    assert "qdrant_search" in tool_names
    assert "list_systems" in tool_names

    rpc_res = client.post(
        "/api/mcp/geodesic",
        json={"jsonrpc": "2.0", "id": 7, "method": "tools/list", "params": {}},
    )
    assert rpc_res.status_code == 200
    rpc_body = rpc_res.json()
    assert rpc_body["id"] == 7
    assert len(rpc_body["result"]["tools"]) >= 4

    for spa_route in ("/ui", "/ui/brain", "/ui/artifacts", "/ui/promoted-model", "/ui/benchmarks", "/ui/staging-qdrant"):
        ui_res = client.get(spa_route)
        assert ui_res.status_code == 200
        assert "<div id=\"root\">" in ui_res.text or "<div id='root'>" in ui_res.text
