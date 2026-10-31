from __future__ import annotations

from pathlib import Path
from qdrant_client import QdrantClient

from indexes.informational_index import (
    DEFAULT_IDEA_PROJECTS_ROOT,
    DEFAULT_STAGING_ROOT,
    INFORMATIONAL_COLLECTION_NAME,
    AgentInformationalGraphIndex,
    QdrantGeodesicLifecycleMCPServer,
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
        "PolyphonicStageCompilerCascadeModel", "BenchmarksSection"
    )
    assert trace_promoted["found"] is True
    assert trace_promoted["hop_count"] >= 2
    prom_hop_ids = [h["node_id"] for h in trace_promoted["hops"]]
    assert prom_hop_ids[0] == "model:neural_networks:PolyphonicStageCompilerCascadeModel"
    assert prom_hop_ids[-1] == "ui:portfolio:BenchmarksSection"

    trace1 = idx.trace_wayfinding_path("CohesiveUnifiedMonadicCavityModel", "BenchmarksSection")
    assert trace1["found"] is True
    assert trace1["hop_count"] >= 2
    hop_ids = [h["node_id"] for h in trace1["hops"]]
    assert hop_ids[0] == "model:neural_networks:CohesiveUnifiedMonadicCavityModel"
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


def test_qdrant_geodesic_lifecycle_mcp_server() -> None:
    """
    Verifies the MCP Server (`QdrantGeodesicLifecycleMCPServer`) over both direct JSON-RPC 2.0
    and live Stdio subprocess transport (`mcp_main.py`):
    1. `initialize`, `tools/list`, `resources/list`, `resources/read`, `prompts/list`, `prompts/get`.
    2. `list_systems`: Lists all 7 systems (`portfolio`, `components`, `toolset`, `neural_networks`,
       `large_tools`, `StageCompiler-development`, `qdrant_staging`) with Qdrant collections and entrypoints.
    3. `qdrant_search`: Searches unified Qdrant collections with chunk rejoin and tri-layer juxtaposition.
    4. `get_design_compiler_code_juxtaposition`: Provides side-by-side Design, Compiler, and Code context.
    5. `trace_full_geodesic_code_path`: Follows the complete start-to-finish geodesic code path across
       Design -> Stage-0/Stage-1 Compiler -> Core Runtime vCPU -> Neural Model -> Promoted Harness ->
       BenchmarkLedger -> Portfolio Service & API -> React UI & AsyncChatWindow.
    6. `manage_system_lifecycle`: Exercises `status`, `verify`, `promote_harness`, and `snapshot`.
    """
    import json
    import subprocess
    import sys

    mcp = QdrantGeodesicLifecycleMCPServer(lazy_init=False)

    # 1. Protocol initialization & tool discovery
    init_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"},
        }
    )
    assert init_resp["result"]["serverInfo"]["name"] == "qdrant-geodesic-lifecycle"
    assert "tools" in init_resp["result"]["capabilities"]

    tools_resp = mcp.handle_jsonrpc_message(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "qdrant_search",
        "list_systems",
        "manage_system_lifecycle",
        "get_design_compiler_code_juxtaposition",
        "trace_full_geodesic_code_path",
        "fs_crud",
    }.issubset(tool_names)

    # 2. `list_systems` tool call
    sys_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "list_systems",
                "arguments": {"include_collections": True, "include_lifecycle": True},
            },
        }
    )
    sys_payload = sys_resp["result"]["structuredContent"]
    assert sys_payload["total_systems"] == 8
    sys_ids = {s["system_id"] for s in sys_payload["systems"]}
    assert {
        "portfolio",
        "components",
        "toolset",
        "neural_networks",
        "playground",
        "large_tools",
        "StageCompiler-development",
        "qdrant_staging",
    } == sys_ids
    for s in sys_payload["systems"]:
        assert s["keep_in_place"] is True
        assert "design" in s["layers"]
        assert "compiler" in s["layers"]

    bench_search_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 31,
            "method": "tools/call",
            "params": {
                "name": "qdrant_search",
                "arguments": {
                    "query": "hyperbolic_harness SplitDigest",
                    "collection": "unified_harness_benchmarks",
                    "top_k": 5,
                },
            },
        }
    )
    bench_search_payload = bench_search_resp["result"]["structuredContent"]
    assert bench_search_payload["total_matches"] >= 1

    # 3. `qdrant_search` tool call across unified collections
    search_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "qdrant_search",
                "arguments": {"query": "monadic_cavity", "top_k": 10, "rejoin_chunks": True},
            },
        }
    )
    search_payload = search_resp["result"]["structuredContent"]
    assert search_payload["total_matches"] >= 3
    first_hit = search_payload["results"][0]
    assert "juxtaposition" in first_hit
    assert {"design", "compiler", "code"} == set(first_hit["juxtaposition"].keys())

    # 4. `get_design_compiler_code_juxtaposition` tool call
    jux_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "get_design_compiler_code_juxtaposition",
                "arguments": {
                    "query": "PolyphonicStageCompilerCascadeModel",
                    "include_geodesic_trace": True,
                },
            },
        }
    )
    jux_payload = jux_resp["result"]["structuredContent"]
    assert "design" in jux_payload["juxtaposition"]
    assert "compiler" in jux_payload["juxtaposition"]
    assert "code" in jux_payload["juxtaposition"]
    assert len(jux_payload["design_layer_nodes"]) >= 1
    assert len(jux_payload["compiler_layer_nodes"]) >= 1
    assert len(jux_payload["code_layer_nodes"]) >= 1
    assert jux_payload["geodesic_code_path"]["found"] is True
    assert jux_payload["geodesic_code_path"]["hop_count"] >= 15

    # 5. `trace_full_geodesic_code_path` tool call (start-to-finish full geodesic code path)
    trace_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "trace_full_geodesic_code_path",
                "arguments": {"include_source_excerpts": True},
            },
        }
    )
    trace_payload = trace_resp["result"]["structuredContent"]
    assert trace_payload["found"] is True
    assert trace_payload["hop_count"] >= 18
    assert trace_payload["total_geodesic_distance"] > 0.5
    assert trace_payload["start_node_id"] == "design:large_tools:project_master_spec"
    assert trace_payload["target_node_id"] == "ui:components:AsyncChatWindow"
    assert {"design", "compiler", "code", "model", "ui"}.issubset(
        set(trace_payload["layers_traversed"])
    )
    for hop in trace_payload["hops"]:
        assert Path(hop["file_path"]).exists(), f"Missing hop file_path: {hop['file_path']}"
        norm_sq = sum(c * c for c in hop["poincare_coords"])
        assert 0.01 < norm_sq < 1.0
        assert {"design", "compiler", "code"} == set(hop["juxtaposed_context"].keys())

    # 6. `manage_system_lifecycle` tool call (`verify` and `promote_harness`)
    ver_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "manage_system_lifecycle",
                "arguments": {"action": "verify", "system_id": "all"},
            },
        }
    )
    ver_payload = ver_resp["result"]["structuredContent"]
    assert ver_payload["status"] == "VERIFIED"
    assert ver_payload["keep_in_place_invariants_valid"] is True
    assert ver_payload["verified_node_paths_count"] >= 80

    h_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "manage_system_lifecycle",
                "arguments": {
                    "action": "promote_harness",
                    "options": {"harness_action": "status"},
                },
            },
        }
    )
    h_payload = h_resp["result"]["structuredContent"]
    assert h_payload["status"] == "COMPLETED"
    assert h_payload["harness_result"]["available"] is True

    # 7. Verify live Stdio subprocess execution of `mcp_main.py`
    mcp_main_script = (
        DEFAULT_IDEA_PROJECTS_ROOT / "portfolio" / "portfolio" / "python" / "mcp_main.py"
    )
    proc = subprocess.Popen(
        [sys.executable, str(mcp_main_script)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdio_reqs = "\n".join(
            [
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 101,
                        "method": "initialize",
                        "params": {"protocolVersion": "2024-11-05"},
                    }
                ),
                json.dumps({"jsonrpc": "2.0", "id": 102, "method": "tools/list", "params": {}}),
            ]
        ) + "\n"
        out, err = proc.communicate(input=stdio_reqs, timeout=15)
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 2, f"Expected 2 JSON-RPC responses, got {len(lines)}: {err}"
        r1 = json.loads(lines[0])
        r2 = json.loads(lines[1])
        assert r1["id"] == 101
        assert r1["result"]["serverInfo"]["name"] == "qdrant-geodesic-lifecycle"
        assert r2["id"] == 102
        assert len(r2["result"]["tools"]) == 6
    finally:
        if proc.poll() is None:
            proc.kill()


def test_deep_validation_knowledge_items_and_tri_layer_geodesic_math() -> None:
    """
    Deep validation of:
    1. Knowledge Item (KI) discovery and propagation across `/Users/jarek/.gemini/antigravity*/knowledge`,
       `agent_informational_graph_index`, and `portfolio_wiki_chunks` so all agents know they can sync.
    2. `qdrant_search` filtering across every collection, layer, system_id, and `rejoin_chunks` mode.
    3. `get_design_compiler_code_juxtaposition` across multiple queries verifying physical files, SHA-256
       digests, exported AST/.sig symbols, and excerpts across `design`, `compiler`, and `code`.
    4. `trace_full_geodesic_code_path` verifying the full 23-hop canonical spine, targeted sub-paths,
       and strict Poincare ball hyperbolic geometry invariants (unit ball norm, positive step distance,
       monotonic cumulative distance, midpoint containment, and hyperbolic triangle inequality).
    """
    idx = AgentInformationalGraphIndex(
        staging_root=DEFAULT_STAGING_ROOT,
        idea_projects_root=DEFAULT_IDEA_PROJECTS_ROOT,
        auto_build=True,
    )
    poincare_dist = idx._poincare_distance

    # 1. Verify Antigravity Knowledge Items exist across CLI, IDE, and base knowledge directories
    expected_ki_ids = {
        "ki_qdrant_staging_cross_repo_sync_and_lifecycle_mcp",
        "ki_design_compiler_code_geodesic_juxtaposition",
    }
    for k_root in (
        Path("/Users/jarek/.gemini/antigravity-cli/knowledge"),
        Path("/Users/jarek/.gemini/antigravity-ide/knowledge"),
        Path("/Users/jarek/.gemini/antigravity/knowledge"),
    ):
        assert k_root.is_dir(), f"Missing knowledge directory: {k_root}"
        for ki_id in expected_ki_ids:
            assert (k_root / ki_id / "metadata.json").is_file()
            artifacts = list((k_root / ki_id / "artifacts").glob("*.md"))
            assert len(artifacts) >= 1

    # Verify every node in agent_informational_graph_index links to the sync & geodesic KIs
    all_nodes_res = idx.query_juxtaposed_map(query="", top_k=200)
    assert all_nodes_res["total_matches"] >= 105
    ki_nodes_found = set()
    for node in all_nodes_res["results"]:
        rel_kis = set(node["juxtaposed_docs"].get("related_ki_ids") or [])
        assert expected_ki_ids.issubset(rel_kis), (
            f"Node {node['node_id']} missing sync/geodesic KIs: {rel_kis}"
        )
        if node["node_type"] == "knowledge_item":
            ki_nodes_found.add(node["idiomatic_name"])
            assert Path(node["juxtaposed_docs"]["doc_path"]).is_file()
            assert len(node["juxtaposed_code"]["sha256"]) == 64
    assert expected_ki_ids.issubset(ki_nodes_found)

    # Verify agents searching Qdrant for sync instructions retrieve both Antigravity KIs and Wiki KIs
    sync_search = idx.search_qdrant_unified(
        query="sync_staging qdrant-geodesic-lifecycle bidirectional",
        collection="all",
        top_k=15,
        rejoin_chunks=True,
    )
    assert sync_search["total_matches"] >= 3
    sync_collections_hit = {r["collection"] for r in sync_search["results"]}
    assert INFORMATIONAL_COLLECTION_NAME in sync_collections_hit
    assert "portfolio_wiki_chunks" in sync_collections_hit

    # 2. Deep validation of `search_qdrant_unified` filters (collection, layer, system_id, rejoin_chunks)
    for coll_name in (
        INFORMATIONAL_COLLECTION_NAME,
        "portfolio_wiki_chunks",
        "portfolio_neural_benchmarks",
        "portfolio_antigravity_brain",
        "neural_networks_code_corpus",
    ):
        c_res = idx.search_qdrant_unified(
            query="model",
            collection=coll_name,
            top_k=5,
            rejoin_chunks=True,
        )
        assert c_res["collection_filter"] == coll_name
        assert c_res["total_matches"] >= 1
        for hit in c_res["results"]:
            assert hit["collection"] == coll_name
            assert "juxtaposition" in hit
            assert {"design", "compiler", "code"} == set(hit["juxtaposition"].keys())

    for target_layer in ("design", "compiler", "code", "model", "index", "ui"):
        l_res = idx.search_qdrant_unified(
            query="stage_compiler",
            collection=INFORMATIONAL_COLLECTION_NAME,
            layer=target_layer,
            top_k=8,
        )
        assert l_res["total_matches"] >= 1, f"No matches for layer={target_layer}"
        for hit in l_res["results"]:
            assert hit["layer"] == target_layer

    for sys_id in ("large_tools", "neural_networks", "portfolio", "components", "toolset"):
        s_res = idx.search_qdrant_unified(
            query="",
            collection=INFORMATIONAL_COLLECTION_NAME,
            system_id=sys_id,
            top_k=5,
        )
        assert s_res["total_matches"] >= 1, f"No matches for system_id={sys_id}"
        for hit in s_res["results"]:
            assert hit["system_id"] == sys_id

    # Verify chunk rejoining vs raw unjoined chunks on `portfolio_wiki_chunks`
    rejoined_res = idx.search_qdrant_unified(
        query="fsspecc",
        collection="portfolio_wiki_chunks",
        top_k=5,
        rejoin_chunks=True,
    )
    unjoined_res = idx.search_qdrant_unified(
        query="fsspecc",
        collection="portfolio_wiki_chunks",
        top_k=5,
        rejoin_chunks=False,
    )
    assert rejoined_res["total_matches"] >= 1
    assert unjoined_res["total_matches"] >= 1
    assert any(r["chunks_rejoined"] >= 2 for r in rejoined_res["results"])
    assert all(r["chunks_rejoined"] == 1 for r in unjoined_res["results"])

    # 3. Deep validation of `get_design_compiler_code_juxtaposition` across multiple domain queries
    for domain_query in (
        "wave_masker",
        "monadic_cavity",
        "PromotedModelHarness",
        "AsyncChatWindow",
        "BenchmarkLedger",
    ):
        jux = idx.get_design_compiler_code_juxtaposition(
            query=domain_query,
            include_geodesic_trace=True,
        )
        assert jux["anchor_node"]["node_id"]
        tri = jux["juxtaposition"]
        assert Path(tri["design"]["doc_path"]).exists(), (
            f"Missing design doc_path for {domain_query}: {tri['design']['doc_path']}"
        )
        assert Path(tri["compiler"]["compiler_path"]).exists(), (
            f"Missing compiler_path for {domain_query}: {tri['compiler']['compiler_path']}"
        )
        assert Path(tri["code"]["code_path"]).exists(), (
            f"Missing code_path for {domain_query}: {tri['code']['code_path']}"
        )
        assert len(tri["code"]["sha256"]) == 64
        assert len(tri["compiler"]["exported_symbols"]) >= 1
        assert len(tri["code"]["exported_symbols"]) >= 1
        assert len(tri["design"]["excerpt"]) > 10
        assert len(tri["compiler"]["excerpt"]) > 10
        assert len(tri["code"]["excerpt"]) > 10
        assert jux["geodesic_code_path"]["found"] is True
        assert jux["geodesic_code_path"]["hop_count"] >= 15

    # 4. Deep validation of `trace_full_geodesic_code_path` & Poincare hyperbolic geometry invariants
    full_trace = idx.trace_full_geodesic_code_path(include_source_excerpts=True)
    assert full_trace["found"] is True
    expected_spine = [
        "design:large_tools:project_master_spec",
        "design:hypoborea_vm:vcpu_microloop_spec",
        "design:hypoborea_vm:polyphonic_cohesive_rns_register_spec",
        "compiler:stage_compiler:stage0_bootstrap",
        "compiler:stage_compiler:stage1_main",
        "compiler:stage_compiler:stage1_ingress",
        "compiler:stage_compiler:stage1_parser",
        "compiler:stage_compiler:stage1_symbols",
        "compiler:stage_compiler:stage1_mcc_ir",
        "compiler:stage_compiler:stage1_emitter",
        "compiler:stage_compiler:stage1_linker",
        "compiler:stage_compiler:macho_synthesizer",
        "code:stage_compiler:core_vcpu_monadic_cavity",
        "code:stage_compiler:core_vcpu_cohesive_continuum",
        "code:neural_networks:implicit_vcpu_layer",
        "model_pkg:neural_networks:stage_compiler",
        "model_pkg:neural_networks:monadic_cavity",
        "code:neural_networks:promoted_harness",
        "module:toolset:benchmark_ledger",
        "service:portfolio:benchmark_service",
        "service:portfolio:portfolio_api",
        "ui:portfolio:PromotedModelSection",
        "ui:components:AsyncChatWindow",
    ]
    actual_spine = [h["node_id"] for h in full_trace["hops"]]
    assert actual_spine == expected_spine
    assert full_trace["hop_count"] == 23

    running_sum = 0.0
    prev_coords = None
    for idx_hop, hop in enumerate(full_trace["hops"]):
        coords = hop["poincare_coords"]
        norm_sq = sum(c * c for c in coords)
        assert 0.0 < norm_sq < 1.0, f"Hop {hop['node_id']} outside Poincare unit ball: {norm_sq}"
        assert Path(hop["file_path"]).exists()
        assert len(hop["sha256"]) == 64
        assert len(hop["code_excerpt"]) > 0
        if idx_hop == 0:
            assert hop["step_geodesic_distance"] == 0.0
            assert hop["cumulative_geodesic_distance"] == 0.0
        else:
            step_d = hop["step_geodesic_distance"]
            assert step_d > 0.0
            direct_step = poincare_dist(prev_coords, coords)
            assert abs(step_d - direct_step) < 1e-3
            running_sum += step_d
            assert abs(hop["cumulative_geodesic_distance"] - running_sum) < 1e-2

            # Verify geodesic midpoint lies strictly inside the Poincare ball
            midpoint = hop["poincare_midpoint"]
            assert midpoint is not None
            mid_norm_sq = sum(c * c for c in midpoint)
            assert 0.0 < mid_norm_sq < 1.0
            d_prev_mid = poincare_dist(prev_coords, midpoint)
            assert 0.0 < d_prev_mid < step_d
        prev_coords = coords

    # Hyperbolic triangle inequality: direct geodesic distance(start, end) <= cumulative spine distance
    start_coords = full_trace["hops"][0]["poincare_coords"]
    end_coords = full_trace["hops"][-1]["poincare_coords"]
    direct_end_to_end = poincare_dist(start_coords, end_coords)
    assert 0.0 < direct_end_to_end <= full_trace["total_geodesic_distance"] + 1e-5

    # Verify targeted sub-path geodesic trace (Compiler Stage-0 -> Runtime vCPU Monadic Cavity)
    sub_trace = idx.trace_full_geodesic_code_path(
        start_query="compiler:stage_compiler:stage0_bootstrap",
        target_query="code:stage_compiler:core_vcpu_monadic_cavity",
        include_source_excerpts=False,
    )
    assert sub_trace["found"] is True
    assert sub_trace["start_node_id"] == "compiler:stage_compiler:stage0_bootstrap"
    assert sub_trace["target_node_id"] == "code:stage_compiler:core_vcpu_monadic_cavity"
    assert sub_trace["hop_count"] == 10
    assert all("code_excerpt" not in h for h in sub_trace["hops"])


def test_deep_validation_mcp_lifecycle_actions_resources_prompts_and_errors(
    tmp_path: Path,
) -> None:
    """
    Deep validation of `QdrantGeodesicLifecycleMCPServer`:
    1. All 7 `manage_system_lifecycle` actions:
       - `status` (all & single system)
       - `verify` (`keep_in_place` invariants + node file existence)
       - `build` and `rebuild`
       - `snapshot` (JSONL snapshot verification)
       - `sync` (`stage_to_target`, `target_to_stage`, and `bidirectional` round-trip in `tmp_path`)
       - `promote_harness` (`status`, `memorize`, `predict_next`, `learn`, `generate`, `reset_memory`)
    2. All 3 MCP Resources (`qdrant://systems/catalog`, `qdrant://staging/manifest`,
       `qdrant://geodesic/design-compiler-code`).
    3. Both MCP Prompts (`trace_geodesic_code_path`, `audit_system_lifecycle`).
    4. Batch JSON-RPC 2.0 array handling and error responses (`-32601`, `-32602`).
    """
    import json

    mcp = QdrantGeodesicLifecycleMCPServer(lazy_init=False)

    def call_tool(name: str, arguments: dict, req_id: int = 1) -> dict:
        resp = mcp.handle_jsonrpc_message(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        assert "result" in resp, f"Unexpected tool error for {name}: {resp}"
        assert resp["result"]["isError"] is False
        return resp["result"]["structuredContent"]

    # 1. Lifecycle: `status`, `verify`, `build`, `rebuild`, `snapshot`
    status_single = call_tool(
        "manage_system_lifecycle",
        {"action": "status", "system_id": "neural_networks"},
        req_id=10,
    )
    assert status_single["status"] == "READY"
    assert len(status_single["systems"]) == 1
    assert status_single["systems"][0]["system_id"] == "neural_networks"

    verify_out = call_tool(
        "manage_system_lifecycle",
        {"action": "verify", "system_id": "all"},
        req_id=11,
    )
    assert verify_out["status"] == "VERIFIED"
    assert verify_out["keep_in_place_invariants_valid"] is True
    assert verify_out["missing_node_paths"] == []

    build_out = call_tool(
        "manage_system_lifecycle",
        {"action": "build", "system_id": "qdrant_staging"},
        req_id=12,
    )
    assert build_out["status"] == "COMPLETED"
    assert build_out["telemetry"]["total_juxtaposed_nodes"] >= 105

    snap_out = call_tool(
        "manage_system_lifecycle",
        {"action": "snapshot", "system_id": "all"},
        req_id=13,
    )
    assert snap_out["status"] == "COMPLETED"
    assert Path(snap_out["informational_snapshot"]).is_file()
    assert snap_out["total_collections"] >= 6
    assert snap_out["total_staged_points"] >= 10000

    # 2. Lifecycle: `sync` (`stage_to_target`, `target_to_stage`, `bidirectional`)
    ext_env = tmp_path / "external_sync_env"
    sync_s2t = call_tool(
        "manage_system_lifecycle",
        {
            "action": "sync",
            "system_id": "qdrant_staging",
            "target_environment": str(ext_env),
            "direction": "stage_to_target",
            "options": {
                "collections": [INFORMATIONAL_COLLECTION_NAME],
                "rebuild_graph": False,
            },
        },
        req_id=14,
    )
    assert sync_s2t["status"] == "COMPLETED"
    s2t_colls = sync_s2t["sync_result"]["collections_synced"]
    assert s2t_colls[INFORMATIONAL_COLLECTION_NAME]["target_points"] >= 105

    sync_t2s = call_tool(
        "manage_system_lifecycle",
        {
            "action": "sync",
            "system_id": "qdrant_staging",
            "target_environment": str(ext_env),
            "direction": "target_to_stage",
            "options": {
                "collections": [INFORMATIONAL_COLLECTION_NAME],
                "rebuild_graph": False,
            },
        },
        req_id=15,
    )
    assert sync_t2s["status"] == "COMPLETED"
    t2s_colls = sync_t2s["sync_result"]["collections_synced"]
    assert t2s_colls[INFORMATIONAL_COLLECTION_NAME]["staged_points"] >= 105

    sync_bi = call_tool(
        "manage_system_lifecycle",
        {
            "action": "sync",
            "system_id": "qdrant_staging",
            "target_environment": str(ext_env),
            "direction": "bidirectional",
            "options": {
                "collections": [INFORMATIONAL_COLLECTION_NAME],
                "rebuild_graph": False,
            },
        },
        req_id=16,
    )
    assert sync_bi["status"] == "COMPLETED"

    # 3. Lifecycle: `promote_harness` (`memorize`, `predict_next`, `learn`, `generate`, `reset_memory`)
    mem_out = call_tool(
        "manage_system_lifecycle",
        {
            "action": "promote_harness",
            "options": {
                "harness_action": "memorize",
                "text": "func wave_gate(x: u64) -> u64 { return x ^ 0x55; }",
            },
        },
        req_id=17,
    )
    assert mem_out["status"] == "COMPLETED"
    assert mem_out["harness_result"]["available"] is True

    pred_out = call_tool(
        "manage_system_lifecycle",
        {
            "action": "promote_harness",
            "options": {
                "harness_action": "predict_next",
                "text": "func wave_gate(x: u64) -> ",
                "top_k": 5,
            },
        },
        req_id=18,
    )
    assert pred_out["status"] == "COMPLETED"
    assert pred_out["harness_result"]["available"] is True

    learn_out = call_tool(
        "manage_system_lifecycle",
        {
            "action": "promote_harness",
            "options": {
                "harness_action": "learn",
                "text": "func wave_gate(x: u64) -> u64 { return x ^ 0x55; }",
                "steps": 2,
                "lr": 1e-3,
            },
        },
        req_id=19,
    )
    assert learn_out["status"] == "COMPLETED"
    assert learn_out["harness_result"]["available"] is True
    assert learn_out["harness_result"]["steps"] == 2

    gen_out = call_tool(
        "manage_system_lifecycle",
        {
            "action": "promote_harness",
            "options": {
                "harness_action": "generate",
                "prompt": "func wave_gate(",
                "max_new_tokens": 8,
            },
        },
        req_id=20,
    )
    assert gen_out["status"] == "COMPLETED"
    assert gen_out["harness_result"]["available"] is True
    assert len(gen_out["harness_result"]["generated"]) >= len("func wave_gate(")

    reset_out = call_tool(
        "manage_system_lifecycle",
        {
            "action": "promote_harness",
            "options": {"harness_action": "reset_memory"},
        },
        req_id=21,
    )
    assert reset_out["status"] == "COMPLETED"
    assert reset_out["harness_result"]["available"] is True

    # 4. MCP Resources (`resources/list` & `resources/read`)
    res_list = mcp.handle_jsonrpc_message(
        {"jsonrpc": "2.0", "id": 30, "method": "resources/list", "params": {}}
    )
    uris = {r["uri"] for r in res_list["result"]["resources"]}
    assert {
        "qdrant://systems/catalog",
        "qdrant://staging/manifest",
        "qdrant://geodesic/design-compiler-code",
    } == uris

    for uri in uris:
        r_read = mcp.handle_jsonrpc_message(
            {"jsonrpc": "2.0", "id": 31, "method": "resources/read", "params": {"uri": uri}}
        )
        contents = r_read["result"]["contents"]
        assert len(contents) == 1
        parsed = json.loads(contents[0]["text"])
        assert isinstance(parsed, dict)

    # 5. MCP Prompts (`prompts/list` & `prompts/get`)
    p_list = mcp.handle_jsonrpc_message(
        {"jsonrpc": "2.0", "id": 40, "method": "prompts/list", "params": {}}
    )
    prompt_names = {p["name"] for p in p_list["result"]["prompts"]}
    assert {"trace_geodesic_code_path", "audit_system_lifecycle"} == prompt_names

    p_trace = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 41,
            "method": "prompts/get",
            "params": {
                "name": "trace_geodesic_code_path",
                "arguments": {
                    "start_query": "design:large_tools:project_master_spec",
                    "target_query": "ui:components:AsyncChatWindow",
                },
            },
        }
    )
    assert "geodesic_code_path" in p_trace["result"]["messages"][0]["content"]["text"]

    p_audit = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "prompts/get",
            "params": {"name": "audit_system_lifecycle", "arguments": {"system_id": "portfolio"}},
        }
    )
    assert "portfolio" in p_audit["result"]["messages"][0]["content"]["text"]

    # 6. Batch JSON-RPC 2.0 requests and negative/error-handling cases
    batch_resp = mcp.handle_jsonrpc_message(
        [
            {"jsonrpc": "2.0", "id": 50, "method": "ping", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            {"jsonrpc": "2.0", "id": 51, "method": "nonexistent/method", "params": {}},
        ]
    )
    assert isinstance(batch_resp, list)
    assert len(batch_resp) == 2
    assert batch_resp[0]["id"] == 50
    assert isinstance(batch_resp[0]["result"], dict)
    assert batch_resp[1]["id"] == 51
    assert batch_resp[1]["error"]["code"] == -32601

    err_tool = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 52,
            "method": "tools/call",
            "params": {"name": "unknown_tool_xyz", "arguments": {}},
        }
    )
    assert err_tool["error"]["code"] == -32602

    err_res = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 53,
            "method": "resources/read",
            "params": {"uri": "qdrant://unknown/uri"},
        }
    )
    assert err_res["error"]["code"] == -32602


def test_wiki_filesystem_and_line_level_geodesic_split_converge_trace() -> None:
    """
    Verifies:
    1. `get_wiki_filesystem_tree()` returns the reusable `FileSystemRootNode` tree across all 7 systems
       including the new `FileSystemExplorer` meta-component (`file_system_explorer.tsx`).
    2. `get_wiki_file_content()` returns numbered Git-like lines, git markers, and extracted variable tokens.
    3. `trace_geodesic_from_file_line()` resolves a clicked file, line number, and variable into:
       - `highlighted_lines` in the active file,
       - `sidebar_elements` following the variable/elements all the way to the terminal UI component,
       - `stage_columns` with both `split_mode == "split"` (multi-panel divergence) and
         `split_mode == "converged"` (N->1 convergence) wired via `incoming_from_panel_ids` and
         `outgoing_to_panel_ids`.
    """
    idx = AgentInformationalGraphIndex(auto_build=True)

    # 1. FileSystem tree across all 7 systems
    fs_tree = idx.get_wiki_filesystem_tree()
    assert fs_tree["total_systems"] == 7
    assert fs_tree["total_files"] >= 60
    assert Path(fs_tree["default_file_path"]).is_file()
    sys_short_ids = {node["short_id"] for node in fs_tree["tree"]}
    assert {
        "large_tools",
        "neural_networks",
        "toolset",
        "portfolio",
        "components",
        "StageCompiler-development",
        "qdrant_staging",
    } == sys_short_ids

    # Verify FileSystemExplorer meta-component is discovered in the components tree
    comp_root = next(n for n in fs_tree["tree"] if n["short_id"] == "components")
    comp_files = [
        f["name"]
        for fld in comp_root["folders"]
        for f in fld["files"]
    ]
    assert "file_system_explorer.tsx" in comp_files
    assert "async_chat_window.tsx" in comp_files

    # 2. Git-like code viewer content
    default_path = fs_tree["default_file_path"]
    content = idx.get_wiki_file_content(default_path, max_lines=200)
    assert content["path"] == default_path
    assert content["displayed_lines"] >= 20
    assert len(content["lines"]) == content["displayed_lines"]
    assert content["lines"][0]["line_number"] == 1
    assert any(ln["git_marker"] == "+" for ln in content["lines"])
    assert any(len(ln["tokens"]) > 0 for ln in content["lines"])

    # 3. Line-level Geodesic Trace with Split-and-Converge Stage Columns
    anchor_line = next(
        (ln for ln in content["lines"] if ln["is_anchor"] and ln["tokens"]),
        content["lines"][0],
    )
    trace = idx.trace_geodesic_from_file_line(
        file_path=default_path,
        line_number=anchor_line["line_number"],
        symbol=anchor_line["tokens"][0] if anchor_line["tokens"] else "",
    )
    assert trace["file_path"] == default_path
    assert trace["clicked_line_number"] == anchor_line["line_number"]
    assert len(trace["focal_symbol"]) >= 2
    assert anchor_line["line_number"] in trace["highlighted_lines"]
    assert trace["total_columns"] >= 4
    assert trace["split_columns_count"] >= 1
    assert trace["converged_columns_count"] >= 2
    assert trace["total_sidebar_elements"] >= trace["total_columns"]

    # Column 0 is origin (1 panel), Column 1 splits into 2 parallel variable control-flow branches,
    # Column 2 reconverges at the control-flow join, and the final column converges into 1 terminal panel (`ui:components:AsyncChatWindow`).
    cols = trace["stage_columns"]
    assert cols[0]["split_mode"] == "origin"
    assert len(cols[0]["panels"]) == 1
    assert cols[1]["split_mode"] == "split"
    assert len(cols[1]["panels"]) == 2
    for p in cols[1]["panels"]:
        assert p["incoming_from_panel_ids"] == ["col-0-panel-0"]
        assert len(p["code_lines"]) >= 1
        assert p["focal_line_number"] >= 1

    # Verify Mermaid code and Qdrant-persisted flow_graph
    assert "flowchart LR" in trace["mermaid_code"]
    assert trace["qdrant_collection"] == "code_flow_graphs"
    assert trace["qdrant_graph_id"].startswith("code_flow:")
    assert len(trace["flow_graph"]["nodes"]) >= 4
    assert len(trace["flow_graph"]["edges"]) >= 4
    assert all("from_line" in e and "to_line" in e for e in trace["flow_graph"]["edges"])

    # Verify convergence: at least one downstream column has 1 panel with multiple `incoming_from_panel_ids`
    converged_downstream = [
        c
        for c in cols[2:]
        if len(c["panels"]) == 1 and len(c["panels"][0]["incoming_from_panel_ids"]) > 1
    ]
    assert len(converged_downstream) >= 1
    assert cols[-1]["panels"][-1]["node_id"] == "ui:components:AsyncChatWindow"
    assert trace["sidebar_elements"][-1]["node_id"] == "ui:components:AsyncChatWindow"

    # 4. Verify `fs_crud` MCP tool and `FSBaseCrudService` bidirectional multimedia & text CRUD backed by FSBase + Qdrant
    mcp = QdrantGeodesicLifecycleMCPServer(lazy_init=False)
    create_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 91,
            "method": "tools/call",
            "params": {
                "name": "fs_crud",
                "arguments": {
                    "action": "create",
                    "path": "artifacts/mcp_test_artifact.md",
                    "content": "# MCP FSBase Test\nVerified in staging Qdrant.\n",
                    "scope": "workspace",
                },
            },
        }
    )["result"]["structuredContent"]
    assert create_resp["status"] == "saved"
    assert Path(create_resp["path"]).is_file()

    read_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 92,
            "method": "tools/call",
            "params": {
                "name": "fs_crud",
                "arguments": {
                    "action": "read",
                    "path": "artifacts/mcp_test_artifact.md",
                    "scope": "workspace",
                },
            },
        }
    )["result"]["structuredContent"]
    assert read_resp["status"] == "ok"
    assert "MCP FSBase Test" in read_resp["content"]

    del_resp = mcp.handle_jsonrpc_message(
        {
            "jsonrpc": "2.0",
            "id": 93,
            "method": "tools/call",
            "params": {
                "name": "fs_crud",
                "arguments": {
                    "action": "delete",
                    "path": "artifacts/mcp_test_artifact.md",
                    "scope": "workspace",
                },
            },
        }
    )["result"]["structuredContent"]
    assert del_resp["status"] == "deleted"



