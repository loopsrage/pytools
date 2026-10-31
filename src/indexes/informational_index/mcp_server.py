"""
Model Context Protocol (MCP) Server for Unified Qdrant Search, Multi-System Discovery,
Full Lifecycle Management, and Design-Compiler-Code Geodesic Path Tracing.

Supports both:
1. Stdio JSON-RPC 2.0 transport (`python -m indexes.informational_index.mcp_server`)
   for local IDE / CLI MCP clients (`~/.gemini/config/mcp_config.json` & `.agents/mcp_config.json`).
2. Streamable HTTP JSON-RPC 2.0 transport (`POST /api/mcp`) mounted on FastAPI.
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from indexes.informational_index.fs_crud_service import FSBaseCrudService
from indexes.informational_index.informational_index import (
    DEFAULT_IDEA_PROJECTS_ROOT,
    DEFAULT_KNOWLEDGE_ROOT,
    DEFAULT_SKILL_MD_PATH,
    DEFAULT_STAGING_ROOT,
    INFORMATIONAL_COLLECTION_NAME,
    AgentInformationalGraphIndex,
)

MCP_SERVER_NAME = "qdrant-geodesic-lifecycle"
MCP_SERVER_VERSION = "1.0.0"
DEFAULT_MCP_PROTOCOL_VERSION = "2024-11-05"


class QdrantGeodesicLifecycleMCPServer:
    """
    Spec-compliant MCP Server providing:
    - `qdrant_search`: Search unified Qdrant collections with chunk rejoin & tri-layer juxtaposition.
    - `list_systems`: List all systems (`portfolio`, `components`, `toolset`, `neural_networks`,
      `large_tools`, `StageCompiler-development`, `qdrant_staging`), their Qdrant collections,
      design/compiler/code entrypoints, and lifecycle status.
    - `manage_system_lifecycle`: Execute full lifecycles (`status`, `build`, `rebuild`, `sync`,
      `verify`, `promote_harness`, `snapshot`).
    - `get_design_compiler_code_juxtaposition`: Retrieve side-by-side Design Spec, Compiler
      Stage, and Runtime Code context plus the full geodesic path.
    - `trace_full_geodesic_code_path`: Follow the complete code path from start to finish across
      Design -> Compiler -> Runtime Code -> Neural Model -> Harness -> Service/API -> UI.
    """

    def __init__(
        self,
        index: Optional[AgentInformationalGraphIndex] = None,
        staging_root: str | Path = DEFAULT_STAGING_ROOT,
        idea_projects_root: str | Path = DEFAULT_IDEA_PROJECTS_ROOT,
        skill_md_path: str | Path = DEFAULT_SKILL_MD_PATH,
        knowledge_root: str | Path = DEFAULT_KNOWLEDGE_ROOT,
        lazy_init: bool = True,
    ) -> None:
        self._index = index
        self._fs_crud: Optional[FSBaseCrudService] = None
        self.staging_root = Path(staging_root).resolve()
        self.idea_projects_root = Path(idea_projects_root).resolve()
        self.skill_md_path = Path(skill_md_path).resolve()
        self.knowledge_root = Path(knowledge_root).resolve()
        if not lazy_init and self._index is None:
            self._ensure_index()

    def _ensure_fs_crud(self) -> FSBaseCrudService:
        if self._fs_crud is None:
            self._fs_crud = FSBaseCrudService(
                promoted_root=self.idea_projects_root / "neural_networks" / "promoted",
                staging_root=self.staging_root,
                idea_projects_root=self.idea_projects_root,
            )
        return self._fs_crud

    def _ensure_index(self) -> AgentInformationalGraphIndex:
        if self._index is None:
            self._index = AgentInformationalGraphIndex(
                staging_root=self.staging_root,
                idea_projects_root=self.idea_projects_root,
                skill_md_path=self.skill_md_path,
                knowledge_root=self.knowledge_root,
                auto_build=True,
            )
        return self._index

    @staticmethod
    def get_tools_manifest() -> List[Dict[str, Any]]:
        """Returns the MCP `tools/list` schema definitions."""
        return [
            {
                "name": "qdrant_search",
                "description": (
                    "Search across unified Qdrant collections (agent_informational_graph_index, "
                    "portfolio_wiki_chunks, portfolio_neural_benchmarks, portfolio_antigravity_brain, "
                    "neural_networks_code_corpus, code_corpus_temporal_index) with automatic chunk rejoin "
                    "and side-by-side Design, Compiler, and Code juxtaposition context."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query (symbol name, file path, architectural concept, or natural language).",
                        },
                        "collection": {
                            "type": "string",
                            "description": (
                                "Optional Qdrant collection filter ('all', 'agent_informational_graph_index', "
                                "'portfolio_wiki_chunks', 'portfolio_neural_benchmarks', "
                                "'portfolio_antigravity_brain', 'neural_networks_code_corpus', "
                                "'code_corpus_temporal_index'). Defaults to 'all'."
                            ),
                        },
                        "system_id": {
                            "type": "string",
                            "description": (
                                "Optional system filter ('all', 'portfolio', 'components', 'toolset', "
                                "'neural_networks', 'large_tools', 'StageCompiler-development', 'qdrant_staging')."
                            ),
                        },
                        "layer": {
                            "type": "string",
                            "description": (
                                "Optional architectural layer filter ('all', 'design', 'compiler', 'code', "
                                "'model', 'index', 'ui')."
                            ),
                        },
                        "node_type": {
                            "type": "string",
                            "description": "Optional node_type filter (e.g. 'design_spec', 'compiler_stage', 'runtime_code', 'ui_component', 'toolset_index').",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Maximum number of results to return (default: 15).",
                            "default": 15,
                        },
                        "rejoin_chunks": {
                            "type": "boolean",
                            "description": "Whether to rejoin multi-chunk Qdrant documents in order (default: true).",
                            "default": True,
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "list_systems",
                "description": (
                    "List every system across /Users/jarek/IdeaProjects (portfolio, components, toolset, "
                    "neural_networks, large_tools, StageCompiler-development, qdrant_staging) with its Qdrant "
                    "collections, point counts, design/compiler/code entrypoints, keep_in_place invariants, "
                    "and lifecycle health."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "include_collections": {
                            "type": "boolean",
                            "description": "Include detailed Qdrant collection metadata per system (default: true).",
                            "default": True,
                        },
                        "include_lifecycle": {
                            "type": "boolean",
                            "description": "Include lifecycle status and supported actions per system (default: true).",
                            "default": True,
                        },
                    },
                },
            },
            {
                "name": "manage_system_lifecycle",
                "description": (
                    "Manage full system and Qdrant lifecycles: 'status' (health & telemetry), "
                    "'build' / 'rebuild' (rebuild Design-Compiler-Code informational graph & unify Qdrant), "
                    "'sync' (bidirectional or directional Qdrant sync across repos/staging/environments), "
                    "'verify' (verify SHA-256 disk-vs-Qdrant code integrity and keep_in_place invariants), "
                    "'promote_harness' (run status/learn/generate/memorize/reset_memory/predict_next on "
                    "PromotedModelHarness), 'learn' (unified text/tokens/continuous learning dispatcher), "
                    "'learn_topic' (async Pinhole Aperture topic training with 3-modality "
                    "separation & Public/Internal bucket gating), 'train_qdrant_topics' (async post-benchmark "
                    "Qdrant topic training), 'training_status' (async worker & relativistic proper-time tau telemetry), "
                    "'blend_topics' (topic isolation + simultaneous Topic HRRF & Topic-Data HRRF with "
                    "Midpoint blending), 'topic_crud' (lock/unlock/delete/read/create topics "
                    "with ROOT_SCOPE_ID=1009 internal code immunity), and 'snapshot' (export JSONL snapshots)."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": [
                                "status",
                                "build",
                                "rebuild",
                                "sync",
                                "verify",
                                "promote_harness",
                                "learn",
                                "learn_topic",
                                "train_qdrant_topics",
                                "training_status",
                                "blend_topics",
                                "topic_crud",
                                "snapshot",
                            ],
                            "description": "Lifecycle operation to execute.",
                        },
                        "system_id": {
                            "type": "string",
                            "description": "Target system ID or 'all' (default: 'all').",
                            "default": "all",
                        },
                        "target_environment": {
                            "type": "string",
                            "description": "Optional target filesystem path or Qdrant URL when action='sync'.",
                        },
                        "direction": {
                            "type": "string",
                            "enum": ["bidirectional", "stage_to_target", "target_to_stage"],
                            "description": "Sync direction when action='sync' (default: 'bidirectional').",
                            "default": "bidirectional",
                        },
                        "options": {
                            "type": "object",
                            "description": "Optional action-specific parameters (e.g. {'harness_action': 'learn', 'prompt': '...', 'steps': 2}).",
                        },
                    },
                    "required": ["action"],
                },
            },
            {
                "name": "get_design_compiler_code_juxtaposition",
                "description": (
                    "Provide side-by-side tri-layer juxtaposition context for Design Specification, "
                    "Stage-0/Stage-1/Neural Compiler Pipeline, and Runtime Code implementation for any "
                    "symbol, module, or concept, including the full start-to-finish geodesic code path."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Symbol, module, component, model, or architectural concept to juxtapose.",
                        },
                        "system_id": {
                            "type": "string",
                            "description": "Optional system filter ('portfolio', 'toolset', 'neural_networks', 'large_tools', 'components').",
                        },
                        "include_geodesic_trace": {
                            "type": "boolean",
                            "description": "Include the full start-to-finish Poincaré geodesic code path (default: true).",
                            "default": True,
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "trace_full_geodesic_code_path",
                "description": (
                    "Follow the complete Poincaré hyperbolic geodesic code path from start to finish "
                    "(1_DESIGN_SPECIFICATION -> 2_COMPILER_PIPELINE -> 3_NEURAL_COMPILER_AND_CAVITY_MODEL -> "
                    "4_RUNTIME_CODE_AND_SERVICE -> 5_QDRANT_AND_TOOLSET_INDEX -> 6_UI_STREAMING_PRESENTATION), "
                    "or between any two specific start_query and target_query endpoints, returning every hop's "
                    "file path, exported symbols, SHA-256, juxtaposed Design/Compiler/Code context, and "
                    "hyperbolic geodesic distance."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "start_query": {
                            "type": "string",
                            "description": (
                                "Optional starting node ID, symbol, or concept. If omitted or if target_query "
                                "is omitted, returns the full start-to-finish end-to-end geodesic pipeline."
                            ),
                        },
                        "target_query": {
                            "type": "string",
                            "description": "Optional target node ID, symbol, or concept.",
                        },
                        "system_id": {
                            "type": "string",
                            "description": "Optional system scope.",
                        },
                        "include_source_excerpts": {
                            "type": "boolean",
                            "description": "Include code and design excerpts at every hop (default: true).",
                            "default": True,
                        },
                    },
                },
            },
            {
                "name": "fs_crud",
                "description": (
                    "Perform FSBase-backed File System CRUD ('list', 'read', 'create', 'update', "
                    "'upload', 'delete', 'mkdir', 'sync') on the Promoted Model & Staging Qdrant workspace "
                    "(/Users/jarek/IdeaProjects/neural_networks/promoted and "
                    "/Users/jarek/IdeaProjects/qdrant_staging/promoted_workspace), supporting text, code, "
                    "markdown, and bidirectional multimedia (base64 images, audio, video, PDF) backed up via "
                    "FSBase and indexed in staging Qdrant ('promoted_model_artifacts')."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": [
                                "list",
                                "tree",
                                "read",
                                "create",
                                "update",
                                "write",
                                "upload",
                                "delete",
                                "mkdir",
                                "sync",
                            ],
                            "description": "CRUD operation to perform on the FSBase workspace.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Target relative or absolute file path inside promoted or staging workspace.",
                        },
                        "root_id": {
                            "type": "string",
                            "enum": ["promoted_repo", "promoted_staging"],
                            "description": "Target FSBase root ('promoted_repo' or 'promoted_staging'). Defaults to 'promoted_repo'.",
                            "default": "promoted_repo",
                        },
                        "folder": {
                            "type": "string",
                            "description": "Default subfolder when creating/uploading ('artifacts' or 'multimedia').",
                            "default": "artifacts",
                        },
                        "content": {
                            "type": "string",
                            "description": "UTF-8 text/markdown/code content for create/update.",
                        },
                        "content_base64": {
                            "type": "string",
                            "description": "Base64 or data:...;base64,... payload for binary/multimedia uploads.",
                        },
                        "media_type": {
                            "type": "string",
                            "description": "Optional MIME type (e.g. 'image/png', 'audio/wav', 'video/mp4', 'text/markdown').",
                        },
                        "query": {
                            "type": "string",
                            "description": "Optional filter query when action='list'.",
                        },
                    },
                    "required": ["action"],
                },
            },
        ]

    @staticmethod
    def get_resources_manifest() -> List[Dict[str, Any]]:
        """Returns the MCP `resources/list` manifest."""
        return [
            {
                "uri": "qdrant://systems/catalog",
                "name": "Workspace Multi-System & Qdrant Catalog",
                "description": "Complete catalog of all 7 systems, Qdrant collections, entrypoints, and keep_in_place invariants.",
                "mimeType": "application/json",
            },
            {
                "uri": "qdrant://staging/manifest",
                "name": "Unified Qdrant Staging Manifest",
                "description": "Live unified Qdrant staging manifest from /Users/jarek/IdeaProjects/qdrant_staging/staging_manifest.json.",
                "mimeType": "application/json",
            },
            {
                "uri": "qdrant://geodesic/design-compiler-code",
                "name": "Start-to-Finish Design -> Compiler -> Code Geodesic Pipeline",
                "description": "Complete end-to-end Poincaré hyperbolic geodesic code path from Design Spec through Stage-0/Stage-1 Compiler to Runtime vCPU, Neural Harness, FastAPI Service, and React UI.",
                "mimeType": "application/json",
            },
        ]

    @staticmethod
    def get_prompts_manifest() -> List[Dict[str, Any]]:
        """Returns the MCP `prompts/list` manifest."""
        return [
            {
                "name": "trace_geodesic_code_path",
                "description": "Trace the full start-to-finish geodesic code path with juxtaposed Design, Compiler, and Code context.",
                "arguments": [
                    {
                        "name": "query",
                        "description": "Symbol, module, or subsystem to trace from start to finish.",
                        "required": False,
                    }
                ],
            },
            {
                "name": "audit_system_lifecycle",
                "description": "Verify SHA-256 code integrity, Qdrant collection unification, and keep_in_place invariants across all systems.",
                "arguments": [
                    {
                        "name": "system_id",
                        "description": "Target system ID or 'all'.",
                        "required": False,
                    }
                ],
            },
        ]

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Executes one of the 5 MCP tools and returns the structured dictionary payload."""
        idx = self._ensure_index()
        args = dict(arguments or {})

        if name == "qdrant_search":
            return idx.search_qdrant_unified(
                query=str(args.get("query", "")),
                collection=args.get("collection"),
                system_id=args.get("system_id"),
                layer=args.get("layer"),
                node_type=args.get("node_type"),
                top_k=int(args.get("top_k", 15)),
                rejoin_chunks=bool(args.get("rejoin_chunks", True)),
            )

        if name == "list_systems":
            return idx.list_systems(
                include_collections=bool(args.get("include_collections", True)),
                include_lifecycle=bool(args.get("include_lifecycle", True)),
            )

        if name == "manage_system_lifecycle":
            return idx.manage_system_lifecycle(
                action=str(args.get("action", "status")),
                system_id=str(args.get("system_id", "all")),
                target_environment=args.get("target_environment"),
                direction=str(args.get("direction", "bidirectional")),
                options=args.get("options"),
            )

        if name == "get_design_compiler_code_juxtaposition":
            return idx.get_design_compiler_code_juxtaposition(
                query=str(args.get("query", "")),
                system_id=args.get("system_id"),
                include_geodesic_trace=bool(args.get("include_geodesic_trace", True)),
            )

        if name == "trace_full_geodesic_code_path":
            return idx.trace_full_geodesic_code_path(
                start_query=str(args.get("start_query", "")),
                target_query=str(args.get("target_query", "")),
                system_id=args.get("system_id"),
                include_source_excerpts=bool(args.get("include_source_excerpts", True)),
            )

        if name == "fs_crud":
            fs_svc = self._ensure_fs_crud()
            return fs_svc.execute_crud(
                action=str(args.get("action", "list")),
                path=args.get("path") or args.get("file_path") or args.get("name"),
                root_id=args.get("root_id") or "promoted_repo",
                folder=args.get("folder") or "artifacts",
                content=args.get("content"),
                content_base64=args.get("content_base64"),
                media_type=args.get("media_type"),
                query=args.get("query") or "",
            )

        raise ValueError(f"Unknown MCP tool: {name!r}")

    def read_resource(self, uri: str) -> Dict[str, Any]:
        """Reads an MCP resource URI (`qdrant://...`)."""
        idx = self._ensure_index()
        if uri == "qdrant://systems/catalog":
            data = idx.list_systems(include_collections=True, include_lifecycle=True)
        elif uri == "qdrant://staging/manifest":
            data = idx.get_staging_overview()
        elif uri == "qdrant://geodesic/design-compiler-code":
            data = idx.trace_full_geodesic_code_path(include_source_excerpts=True)
        else:
            raise ValueError(f"Unknown MCP resource URI: {uri!r}")

        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": json.dumps(data, indent=2),
                }
            ]
        }

    def get_prompt(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Resolves an MCP prompt template with live juxtaposed context."""
        idx = self._ensure_index()
        args = dict(arguments or {})
        if name == "trace_geodesic_code_path":
            q = str(args.get("query") or "PolyphonicStageCompilerCascadeModel")
            jux = idx.get_design_compiler_code_juxtaposition(query=q, include_geodesic_trace=True)
            return {
                "description": f"Start-to-finish Design -> Compiler -> Code geodesic trace for {q}",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                f"Trace the complete Design -> Compiler -> Runtime Code geodesic path for '{q}':\n\n"
                                f"{json.dumps(jux, indent=2)}"
                            ),
                        },
                    }
                ],
            }
        if name == "audit_system_lifecycle":
            sys_id = str(args.get("system_id") or "all")
            ver = idx.manage_system_lifecycle(action="verify", system_id=sys_id)
            return {
                "description": f"Full SHA-256 and Qdrant lifecycle verification for {sys_id}",
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": (
                                f"Verify full system lifecycle and Qdrant integrity for '{sys_id}':\n\n"
                                f"{json.dumps(ver, indent=2)}"
                            ),
                        },
                    }
                ],
            }
        raise ValueError(f"Unknown MCP prompt: {name!r}")

    def _handle_single_rpc(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes a single JSON-RPC 2.0 message and returns a response dict (or None for notifications)."""
        method = str(request.get("method", ""))
        req_id = request.get("id")
        params = request.get("params") or {}

        # Notifications have no `id` and expect no response
        if req_id is None and method.startswith("notifications/"):
            return None

        try:
            if method == "initialize":
                client_proto = str(params.get("protocolVersion") or DEFAULT_MCP_PROTOCOL_VERSION)
                result = {
                    "protocolVersion": client_proto,
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"subscribe": False, "listChanged": False},
                        "prompts": {"listChanged": False},
                    },
                    "serverInfo": {
                        "name": MCP_SERVER_NAME,
                        "version": MCP_SERVER_VERSION,
                    },
                }
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": self.get_tools_manifest()}
            elif method == "tools/call":
                tool_name = str(params.get("name", ""))
                tool_args = params.get("arguments") or {}
                payload = self.call_tool(tool_name, tool_args)
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(payload, indent=2),
                        }
                    ],
                    "structuredContent": payload,
                    "isError": False,
                }
            elif method == "resources/list":
                result = {"resources": self.get_resources_manifest()}
            elif method == "resources/read":
                uri = str(params.get("uri", ""))
                result = self.read_resource(uri)
            elif method == "prompts/list":
                result = {"prompts": self.get_prompts_manifest()}
            elif method == "prompts/get":
                prompt_name = str(params.get("name", ""))
                prompt_args = params.get("arguments") or {}
                result = self.get_prompt(prompt_name, prompt_args)
            else:
                if req_id is None:
                    return None
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}",
                    },
                }

            if req_id is None:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": result,
            }
        except Exception as exc:
            if req_id is None:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32602 if isinstance(exc, (ValueError, KeyError)) else -32603,
                    "message": str(exc),
                    "data": {"traceback": traceback.format_exc()[-800:]},
                },
            }

    def handle_jsonrpc_message(
        self, message: Dict[str, Any] | List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any] | List[Dict[str, Any]]]:
        """Handles a single JSON-RPC 2.0 request or a batch of requests."""
        if isinstance(message, list):
            responses = []
            for item in message:
                if isinstance(item, dict):
                    resp = self._handle_single_rpc(item)
                    if resp is not None:
                        responses.append(resp)
            return responses if responses else None
        if isinstance(message, dict):
            return self._handle_single_rpc(message)
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32600, "message": "Invalid JSON-RPC request"},
        }

    def run_stdio(self) -> None:
        """
        Runs the MCP server over standard input / standard output (newline-delimited JSON-RPC 2.0
        with automatic support for Content-Length headers if sent by the host).
        """
        stdin = sys.stdin.buffer
        stdout = sys.stdout.buffer

        while True:
            line = stdin.readline()
            if not line:
                break
            stripped = line.strip()
            if not stripped:
                continue

            # Support optional Content-Length header framing
            if stripped.lower().startswith(b"content-length:"):
                try:
                    length = int(stripped.split(b":", 1)[1].strip())
                except ValueError:
                    continue
                # Consume blank header separator lines
                while True:
                    hdr = stdin.readline()
                    if not hdr or hdr.strip() == b"":
                        break
                body = stdin.read(length)
                if not body:
                    break
                raw_bytes = body
            else:
                raw_bytes = stripped

            try:
                parsed = json.loads(raw_bytes.decode("utf-8"))
            except Exception as exc:
                err_resp = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": f"Parse error: {exc}"},
                }
                stdout.write((json.dumps(err_resp) + "\n").encode("utf-8"))
                stdout.flush()
                continue

            resp = self.handle_jsonrpc_message(parsed)
            if resp is not None:
                stdout.write((json.dumps(resp) + "\n").encode("utf-8"))
                stdout.flush()


def main() -> None:
    server = QdrantGeodesicLifecycleMCPServer(lazy_init=True)
    server.run_stdio()


if __name__ == "__main__":
    main()
