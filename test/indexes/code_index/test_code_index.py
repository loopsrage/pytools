from __future__ import annotations

import asyncio
from pathlib import Path

from indexes.code_index import (
    CodeCorpusIndex,
    CodeCorpusPeriodicService,
)


def test_code_corpus_index_crud_sha256_qdrant_and_geodesic_traces(tmp_path: Path) -> None:
    repo_dir = tmp_path / "sample_repo"
    qdrant_dir = tmp_path / "qdrant_code"
    ledger_file = tmp_path / "code_corpus_ledger.json"
    (repo_dir / "pkg").mkdir(parents=True, exist_ok=True)

    idx = CodeCorpusIndex(
        repo_path=repo_dir,
        qdrant_path=qdrant_dir,
        collection_name="test_code_corpus_index",
        ledger_path=ledger_file,
    )

    # 1. CRUD CREATE files and build import/inheritance/call chain: a.py -> b.py -> c.py
    res_a = idx.create_file(
        "pkg/a.py",
        "class BaseLayer:\n    def forward_base(self, x):\n        return x + 1\n",
        write_to_fs=True,
    )
    assert res_a["status"] == "indexed"
    assert res_a["changed"] is True
    assert res_a["temporal_version"] == 1

    res_b = idx.create_file(
        "pkg/b.py",
        "from pkg.a import BaseLayer\n\nclass MidBlock(BaseLayer):\n    def compute_mid(self, x):\n        return self.forward_base(x) * 2\n",
        write_to_fs=True,
    )
    assert res_b["status"] == "indexed"
    assert res_b["changed"] is True

    res_c = idx.create_file(
        "pkg/c.py",
        "from pkg.b import MidBlock\n\nclass TopModel:\n    def run_model(self, x):\n        blk = MidBlock()\n        return blk.compute_mid(x)\n",
        write_to_fs=True,
    )
    assert res_c["status"] == "indexed"
    assert res_c["changed"] is True

    # 2. Verify Qdrant SHA-256 pre-save lookup skips re-saving when unchanged
    repeat_a = idx.upsert_file("pkg/a.py")
    assert repeat_a["status"] == "unchanged_sha256"
    assert repeat_a["changed"] is False
    assert idx.has_sha256_in_qdrant(res_a["sha256"], rel_path="pkg/a.py") is True

    # 3. Snapshot temporal baseline and verify `verify_code_unchanged()` returns True
    snap = idx.snapshot_temporal_baseline()
    assert snap["total_files"] == 3
    assert snap["unchanged_files_skipped"] == 3
    ver_1 = idx.verify_code_unchanged()
    assert ver_1["unchanged"] is True
    assert ver_1["verified_files_count"] == 3
    assert ver_1["modified_files"] == []

    # 4. Modify a file on disk without indexing -> verify `verify_code_unchanged()` detects modification
    (repo_dir / "pkg" / "a.py").write_text(
        "class BaseLayer:\n    def forward_base(self, x):\n        return x + 99\n",
        encoding="utf-8",
    )
    ver_2 = idx.verify_code_unchanged()
    assert ver_2["unchanged"] is False
    assert "pkg/a.py" in ver_2["modified_files"]

    # 5. CRUD UPDATE via `update_file` -> increments temporal_version and preserves previous_sha256 in Qdrant
    upd_a = idx.update_file(
        "pkg/a.py",
        "class BaseLayer:\n    def forward_base(self, x):\n        return x + 99\n",
        write_to_fs=True,
    )
    assert upd_a["changed"] is True
    assert upd_a["temporal_version"] == 2
    assert upd_a["previous_sha256"] == res_a["sha256"]

    # Verify both old and new SHA-256 hashes are queryable in Qdrant (`get_by_sha256`)
    old_hits = idx.get_by_sha256(res_a["sha256"])
    new_hits = idx.get_by_sha256(upd_a["sha256"])
    assert len(old_hits) >= 1 and old_hits[0]["is_head"] is False
    assert len(new_hits) >= 1 and new_hits[0]["is_head"] is True

    # 6. CRUD READ + chunk rejoin verification
    read_b = idx.get_file("pkg/b.py", rejoin_from_qdrant=True)
    assert read_b is not None
    assert read_b["rejoined"]["integrity_verified"] is True
    assert "MidBlock" in read_b["classes"]

    # 7. Geodesic Code Trace across AST graph (`TopModel` -> `MidBlock` -> `BaseLayer`)
    trace = idx.get_geodesic_code_trace("TopModel", "BaseLayer")
    assert trace["start_node"] == "pkg/c.py"
    assert trace["target_node"] == "pkg/a.py"
    assert trace["path_nodes"] == ["pkg/c.py", "pkg/b.py", "pkg/a.py"]
    assert trace["hop_count"] == 2
    assert trace["total_geodesic_distance"] > 0.0
    assert len(trace["hops"][0]["poincare_midpoint"]) == 16

    # 8. Model artifact indexing in Qdrant
    m_res = idx.index_model_artifact(
        model_id="test_model_v1",
        model_name="TestPromotedModel",
        class_name="TopModel",
        source_file="pkg/c.py",
        num_params=123456,
        metrics={"mean_rank": 1.0, "wins": 5},
        weight_signature_sha256="deadbeef01",
        is_promoted=True,
    )
    assert m_res["changed"] is True
    m_repeat = idx.index_model_artifact(
        model_id="test_model_v1",
        model_name="TestPromotedModel",
        class_name="TopModel",
        source_file="pkg/c.py",
        num_params=123456,
        metrics={"mean_rank": 1.0, "wins": 5},
        weight_signature_sha256="deadbeef01",
        is_promoted=True,
    )
    assert m_repeat["changed"] is False
    assert len(idx.list_indexed_models()) == 1

    # 9. CRUD DELETE
    del_res = idx.delete_file("pkg/c.py", delete_from_fs=True)
    assert del_res["status"] == "deleted"
    assert idx.get_file("pkg/c.py") is None
    assert not (repo_dir / "pkg" / "c.py").exists()


def test_code_corpus_periodic_service(tmp_path: Path) -> None:
    repo_dir = tmp_path / "periodic_repo"
    qdrant_dir = tmp_path / "qdrant_periodic"
    repo_dir.mkdir(parents=True, exist_ok=True)
    (repo_dir / "mod.py").write_text("def alpha():\n    return 1\n", encoding="utf-8")

    events: list[dict] = []
    idx = CodeCorpusIndex(repo_path=repo_dir, qdrant_path=qdrant_dir)
    svc = CodeCorpusPeriodicService(
        code_index=idx,
        interval_seconds=0.1,
        on_code_change=lambda payload: events.append(payload),
    )

    first = svc.trigger_once()
    assert first["changed_files_indexed"] == 1
    assert len(events) == 1

    # Second trigger without code change -> zero changed files indexed
    second = svc.trigger_once()
    assert second["changed_files_indexed"] == 0
    assert second["unchanged_files_skipped"] == 1
    assert len(events) == 1

    # Run async ServiceController lifecycle briefly
    async def _run_briefly() -> None:
        stop_event = asyncio.Event()
        async with asyncio.TaskGroup() as tg:
            svc.init(stop_event, tg)
            await asyncio.sleep(0.05)
            stop_event.set()

    asyncio.run(_run_briefly())
