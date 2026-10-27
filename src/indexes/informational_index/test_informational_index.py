from __future__ import annotations

from pathlib import Path
from qdrant_client import QdrantClient

from indexes.informational_index import (
    DEFAULT_IDEA_PROJECTS_ROOT,
    DEFAULT_STAGING_ROOT,
    INFORMATIONAL_COLLECTION_NAME,
    AgentInformationalGraphIndex,
)


def test_unified_qdrant_staging_and_juxtaposed_informational_index() -> None:
    """
    Verifies that `/Users/jarek/IdeaProjects/qdrant_staging` unifies all Qdrant namespaces
    adjacent to sibling repos while keeping `components`, Portfolio UI elements, and
    `toolset` indexes in place and juxtaposing code with documentation.
    """
    idx = AgentInformationalGraphIndex(
        staging_root=DEFAULT_STAGING_ROOT,
        idea_projects_root=DEFAULT_IDEA_PROJECTS_ROOT,
        auto_build=True,
    )
    ov = idx.get_staging_overview()

    # 1. Staging directory sits above `portfolio` adjacent to sibling repositories
    staging_path = Path(ov["staging_root"])
    assert staging_path == Path("/Users/jarek/IdeaProjects/qdrant_staging")
    assert staging_path.parent == Path("/Users/jarek/IdeaProjects")
    assert (staging_path / "storage" / "meta.json").is_file()
    assert (staging_path / "staging_manifest.json").is_file()
    assert (staging_path / "informational_graph_ledger.json").is_file()
    assert (staging_path / "snapshots" / f"{INFORMATIONAL_COLLECTION_NAME}.jsonl").is_file()

    # 2. All 6 namespaces are unified in `/Users/jarek/IdeaProjects/qdrant_staging/storage`
    expected_collections = {
        "portfolio_wiki_chunks",
        "portfolio_neural_benchmarks",
        "portfolio_antigravity_brain",
        "neural_networks_code_corpus",
        "code_corpus_temporal_index",
        INFORMATIONAL_COLLECTION_NAME,
    }
    unified_colls = set(ov["unified_collections"].keys())
    assert expected_collections.issubset(unified_colls)
    assert ov["total_staged_points"] > 10000

    # Verify native QdrantClient compatibility on unified staging directory
    client = QdrantClient(path=str(staging_path / "storage"))
    try:
        live_names = {c.name for c in client.get_collections().collections}
        assert expected_collections.issubset(live_names)
        info_coll = client.get_collection(INFORMATIONAL_COLLECTION_NAME)
        assert info_coll.points_count == ov["total_juxtaposed_nodes"]
    finally:
        client.close()

    # 3. Verify UI elements and repo-resident indexes remain in place (`keep_in_place=True`)
    ui_comp_search = idx.query_juxtaposed_map(
        query="AsyncChatWindow",
        node_type="ui_component",
        repo_name="components",
    )
    assert ui_comp_search["total_matches"] >= 1
    chat_node = ui_comp_search["results"][0]
    assert chat_node["idiomatic_name"] == "AsyncChatWindow"
    assert chat_node["keep_in_place"] is True
    assert "/Users/jarek/IdeaProjects/components" in chat_node["juxtaposed_code"]["code_path"]
    assert Path(chat_node["juxtaposed_code"]["code_path"]).is_file()
    assert chat_node["juxtaposed_docs"]["doc_excerpt"]

    bench_ui_search = idx.query_juxtaposed_map(
        query="BenchmarksSection",
        node_type="ui_component",
        repo_name="portfolio",
    )
    assert bench_ui_search["total_matches"] >= 1
    bench_node = bench_ui_search["results"][0]
    assert bench_node["idiomatic_name"] == "BenchmarksSection"
    assert bench_node["keep_in_place"] is True
    assert (
        "/Users/jarek/IdeaProjects/portfolio/portfolio/react/src/components/BenchmarksSection.tsx"
        == bench_node["juxtaposed_code"]["code_path"]
    )
    assert Path(bench_node["juxtaposed_code"]["code_path"]).is_file()

    # 4. Verify toolset indexes are juxtaposed with SKILL.md and Poincare ball coords (||x|| < 1)
    idx_search = idx.query_juxtaposed_map(
        query="AgentInformationalGraphIndex",
        node_type="toolset_index",
    )
    assert idx_search["total_matches"] >= 1
    info_idx_node = idx_search["results"][0]
    assert info_idx_node["keep_in_place"] is True
    assert info_idx_node["juxtaposed_docs"]["doc_type"] == "skill_md"
    norm_sq = sum(c * c for c in info_idx_node["poincare_coords"])
    assert 0.01 < norm_sq < 1.0

    # 5. Verify multi-hop wayfinding across repositories so no agent can get lost
    trace_promoted = idx.trace_wayfinding_path(
        "PhaseLangSheafCompilerCascadeModel", "BenchmarksSection"
    )
    assert trace_promoted["found"] is True
    assert trace_promoted["hop_count"] >= 2
    prom_hop_ids = [h["node_id"] for h in trace_promoted["hops"]]
    assert prom_hop_ids[0] == "model:neural_networks:PhaseLangSheafCompilerCascadeModel"
    assert prom_hop_ids[-1] == "ui:portfolio:BenchmarksSection"

    trace1 = idx.trace_wayfinding_path("PhaseSheafUnifiedMonadicCavityModel", "BenchmarksSection")
    assert trace1["found"] is True
    assert trace1["hop_count"] >= 2
    hop_ids = [h["node_id"] for h in trace1["hops"]]
    assert hop_ids[0] == "model:neural_networks:PhaseSheafUnifiedMonadicCavityModel"
    assert hop_ids[-1] == "ui:portfolio:BenchmarksSection"

    trace2 = idx.trace_wayfinding_path(
        "portfolio_antigravity_brain",
        "ui:components:AsyncChatWindow",
    )
    assert trace2["found"] is True
    assert trace2["hops"][0]["node_id"] == "qdrant_ns:portfolio_antigravity_brain"
    assert trace2["hops"][-1]["node_id"] == "ui:components:AsyncChatWindow"


def test_qdrant_staging_cross_environment_sync(tmp_path: Path) -> None:
    """
    Verifies that `/Users/jarek/IdeaProjects/qdrant_staging` acts as a staging hub and
    synchronizes collections and points with an external environment (`stage_to_target`
    and `bidirectional`).
    """
    idx = AgentInformationalGraphIndex(auto_build=False)
    target_env_dir = tmp_path / "secondary_qdrant_env"

    sync_out = idx.sync_staging(
        target_environment=target_env_dir,
        direction="stage_to_target",
        collections=[INFORMATIONAL_COLLECTION_NAME, "portfolio_wiki_chunks"],
        rebuild_graph=False,
    )
    sync_res = sync_out["sync_result"]
    assert sync_res["status"] == "synced_with_filesystem_environment"
    colls_synced = sync_res["collections_synced"]
    assert colls_synced[INFORMATIONAL_COLLECTION_NAME]["target_points"] >= 70
    assert colls_synced["portfolio_wiki_chunks"]["target_points"] >= 100

    # Open the synced target environment directly with QdrantClient to verify physical ground truth
    target_client = QdrantClient(path=str(target_env_dir))
    try:
        names = {c.name for c in target_client.get_collections().collections}
        assert INFORMATIONAL_COLLECTION_NAME in names
        assert "portfolio_wiki_chunks" in names
        records, _ = target_client.scroll(
            collection_name=INFORMATIONAL_COLLECTION_NAME,
            limit=5,
            with_payload=True,
        )
        assert len(records) == 5
        assert "juxtaposed_code" in records[0].payload
        assert "juxtaposed_docs" in records[0].payload
    finally:
        target_client.close()
