from __future__ import annotations

from pathlib import Path

from benchmark_ledger import (
    AntigravityBrainLedger,
    BenchmarkLedger,
    BenchmarkSaveRequest,
)


def test_benchmark_ledger_scrape_and_save(tmp_path: Path) -> None:
    repo_path = Path("/Users/jarek/IdeaProjects/neural_networks")
    ledger_file = tmp_path / "benchmark_history_ledger.json"

    ledger = BenchmarkLedger(repo_path=repo_path, ledger_path=ledger_file)
    overview = ledger.scrape_and_build(include_git_history=True)

    assert ledger_file.exists()
    assert overview["total_models"] >= 16
    assert overview["total_benchmarks"] == 12
    assert overview["language_benchmarks_count"] == 9
    assert overview["continuous_benchmarks_count"] == 3
    assert overview["total_runs"] >= 6

    # Ensure pre-audit invalidated PolyphonicPipeline is completely absent
    model_names = [m["model_name"] for m in overview["models"]]
    assert "PolyphonicPipeline" not in model_names
    run_hashes = [r["commit_hash"] for r in overview["runs"]]
    assert "53f105d" not in run_hashes
    assert "2d063d1" not in run_hashes

    best_model = overview["best_model"]
    assert best_model is not None
    assert best_model["model_name"] in {
        "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)",
        "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)",
    }
    assert best_model["class_name"] in {
        "CohesiveUnifiedMonadicCavityModel",
        "PolyphonicStageCompilerCascadeModel",
    }
    assert best_model["is_overall_best"] is True
    assert best_model["is_promoted"] is True
    assert best_model["overall_rank"] == 1
    assert 400000 <= best_model["avg_num_params"] <= 2000000
    assert best_model["benchmarks_evaluated"] == 12
    assert best_model["top3_count"] >= 1
    assert f"class {best_model['class_name']}" in best_model["source_code"]
    assert best_model["source_file"].endswith((".py",))

    # Verify CohesiveMonadicVCPUModel and StageCompilerPostGRUModel lookup + AST source extraction
    vcpu = ledger.get_model("CohesiveMonadicVCPUModel (AnyGram 2-GRU + StageCompiler vCPU)")
    assert vcpu is not None
    assert vcpu["class_name"] == "CohesiveMonadicVCPUModel"
    assert vcpu["avg_num_params"] == 751105
    assert "class CohesiveMonadicVCPUModel" in vcpu["source_code"]
    assert "class OmnipresentImplicitOpcodeVCPU" in vcpu["source_code"]
    assert vcpu["source_file"].endswith("monadic_cavity_models.py")

    post_gru = ledger.get_model("StageCompilerPostGRUModel (Full Compiler + Cavity -> Post 2-GRU)")
    assert post_gru is not None
    assert post_gru["class_name"] == "StageCompilerPostGRUModel"
    assert "class StageCompilerPostGRUModel" in post_gru["source_code"]
    assert "class UnifiedStageCompilerModel" in post_gru["source_code"]

    isa_cascaded = ledger.get_model(
        "PolyphonicStageCompilerCascadeModel (ISA Spectral/AST Gate + Cohesive-Manifold Wave Cascade)"
    )
    assert isa_cascaded is not None
    assert isa_cascaded["class_name"] in {
        "PolyphonicStageCompilerCascadeModel",
        "PolyphonicStageCompilerCascadeModel",
    }
    assert isa_cascaded["benchmarks_evaluated"] == 12
    assert (
        "class PolyphonicStageCompilerCascadeModel" in isa_cascaded["source_code"]
        or "class PolyphonicStageCompilerCascadeModel" in isa_cascaded["source_code"]
    )
    assert overview["timeseries_count"] >= 200
    assert len(overview["timeseries"]) == overview["timeseries_count"]

    # Verify task lookup on Mamba Associative Recall and Lexical Scope Binding
    recall = ledger.get_task("mamba_associative_recall")
    assert recall is not None
    assert recall["best_primary_metric"] <= 0.9220
    assert len(recall["leaderboard"]) >= 13
    assert len(recall["history_points"]) >= 5

    lexical = ledger.get_task("lexical_scope_binding")
    assert lexical is not None
    assert lexical["best_primary_metric"] <= 0.2250

    # Verify direct Qdrant sync and incremental SHA-256 diffing
    qdrant_dir = tmp_path / "qdrant_direct"
    sync_1 = ledger.sync_to_qdrant(qdrant_path=qdrant_dir, collection_name="test_neural_benchmarks")
    assert sync_1["status"] == "synced"
    assert sync_1["total_documents"] >= 16 + 12 + 6
    assert sync_1["incremental_updated_documents"] == sync_1["total_documents"]

    sync_2 = ledger.sync_to_qdrant(qdrant_path=qdrant_dir, collection_name="test_neural_benchmarks")
    assert sync_2["status"] == "synced"
    assert sync_2["incremental_updated_documents"] < sync_1["total_documents"]

    # Verify Qdrant document generation
    q_docs = ledger.build_qdrant_documents()
    assert len(q_docs) >= 14 + 12 + 5
    assert q_docs[0].doc_type == "benchmark_model"
    assert q_docs[0].title in {
        "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)",
        "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)",
    }
    assert q_docs[0].is_overall_best is True

    # Verify saving a new run via BenchmarkSaveRequest updates history and persists
    initial_runs = overview["total_runs"]
    save_result = ledger.save_run(
        BenchmarkSaveRequest(
            run_label="API Verification Run",
            commit_hash="verify-01",
            notes="Integration test snapshot",
            task_overrides=[
                {
                    "benchmark_name": "tinyshakespeare_max",
                    "model_name": "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)",
                    "primary_metric": 2.1145,
                    "secondary_metric": 3.0505,
                }
            ],
        )
    )
    assert save_result["total_runs"] == initial_runs + 1
    assert save_result["best_model"]["model_name"] in {
        "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)",
        "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)",
    }

    # Reload from persisted ledger_file and verify state persists across instances
    reloaded = BenchmarkLedger(repo_path=repo_path, ledger_path=ledger_file)
    reloaded_overview = reloaded.scrape_and_build(include_git_history=True)
    assert reloaded_overview["total_runs"] == initial_runs + 1
    updated_shakespeare = reloaded.get_task("tinyshakespeare_max")
    assert updated_shakespeare is not None
    assert abs(updated_shakespeare["best_primary_metric"] - 2.1145) < 1e-6


def test_antigravity_brain_ledger_fsbase_and_qdrant_sync(tmp_path: Path) -> None:
    brain_ledger_file = tmp_path / "antigravity_brain_ledger.json"
    brain_ledger = AntigravityBrainLedger(
        brain_root="/Users/jarek/.gemini/antigravity-cli/brain",
        ledger_path=brain_ledger_file,
    )
    overview = brain_ledger.scan_and_build()
    assert brain_ledger_file.is_file()
    assert overview["total_entries"] >= 20
    assert overview["primary_conversations_count"] >= 8
    assert overview["artifacts_count"] >= 10
    assert overview["fsbase_telemetry"]["filesystem"] == "file"
    assert overview["fsbase_telemetry"]["indexed_directories_count"] >= 10

    first_entry = overview["entries"][0]
    mtimes = [int(e["mtime_ns"]) for e in overview["entries"]]
    assert mtimes == sorted(mtimes)
    assert len(first_entry["snippet"]) <= 210
    assert len(first_entry["code_blocks"]) >= 1
    assert first_entry["locations"]["brain_dir"].startswith("/Users/jarek/.gemini/antigravity-cli/brain/")
    assert len(first_entry["relations"]["related_modules"]) >= 1
    assert len(first_entry["relations"]["related_kis"]) >= 1

    q_docs = brain_ledger.build_qdrant_documents()
    assert len(q_docs) == overview["total_entries"]
    q_dir = tmp_path / "qdrant_brain"
    sync_res = brain_ledger.sync_to_qdrant(qdrant_path=q_dir, collection_name="test_antigravity_brain")
    assert sync_res["status"] == "SYNCED"
    assert sync_res["total_documents"] == overview["total_entries"]
    assert sync_res["qdrant_point_count"] >= sync_res["total_documents"]

