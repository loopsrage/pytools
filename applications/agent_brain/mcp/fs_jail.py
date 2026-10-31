"""
Jailed Filesystem MCP Server for Agent Brain.

Provides isolated per-GUID filesystem jails under `data/brain/fs_jails/<guid>/`.
All operations are confined to the jail root — path traversal (`..`) is blocked.

Tools:
- fs_jail_create(guid) -> create a new isolated jail directory
- fs_jail_list() -> list all existing jail GUIDs
- fs_jail_write(guid, path, content) -> write text to a file within a jail
- fs_jail_read(guid, path) -> read text from a file within a jail
- fs_jail_delete(guid, path) -> delete a file within a jail
- fs_jail_list_files(guid, path) -> list files/dirs within a jail directory
- fs_jail_mkdir(guid, path) -> create a subdirectory within a jail

Resources:
- fs_jail://jails -> list all jails
- fs_jail://jail/{guid} -> info about a specific jail
"""

from __future__ import annotations

import io
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.core.settings import get

MCP_SERVER_NAME = "agent-brain-fs-jail"
MCP_SERVER_VERSION = "1.0.0"
DEFAULT_MCP_PROTOCOL_VERSION = "2024-11-05"


class PathEscapeError(Exception):
    """Raised when a path would escape the jail root."""


class FsJail:
    """A single isolated filesystem jail confined to a jail root directory."""

    def __init__(self, guid: str, base_dir: str | Path):
        import os

        self.guid = guid
        self.root = Path(os.path.abspath(os.path.join(str(base_dir), guid)))
        self.root.mkdir(parents=True, exist_ok=True)
        self._fs = FSBase(filesystem="file")

    def _confine(self, rel_path: str) -> Path:
        """Resolve `rel_path` relative to jail root and ensure it stays inside."""
        import os

        # Reject absolute paths and any attempt to escape via ..
        target = Path(os.path.abspath(os.path.join(str(self.root), rel_path)))
        root_resolved = Path(os.path.abspath(str(self.root)))
        # Ensure target is within root (both absolute)
        try:
            target.relative_to(root_resolved)
        except ValueError:
            raise PathEscapeError(
                f"Path '{rel_path}' escapes jail root '{self.guid}'"
            )
        return target

    def exists(self, rel_path: str) -> bool:
        try:
            target = self._confine(rel_path)
        except PathEscapeError:
            return False
        return self._fs.client.exists(str(target))

    def is_dir(self, rel_path: str) -> bool:
        try:
            target = self._confine(rel_path)
        except PathEscapeError:
            return False
        return self._fs.client.isdir(str(target)) if hasattr(self._fs.client, "isdir") else target.is_dir()

    def write_text(self, rel_path: str, content: str) -> None:
        target = self._confine(rel_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    def read_text(self, rel_path: str) -> str:
        target = self._confine(rel_path)
        return target.read_text(encoding="utf-8")

    def delete(self, rel_path: str) -> None:
        target = self._confine(rel_path)
        if target.is_dir():
            import shutil
            shutil.rmtree(target)
        else:
            target.unlink()

    def mkdir(self, rel_path: str) -> None:
        target = self._confine(rel_path)
        target.mkdir(parents=True, exist_ok=True)

    def list_dir(self, rel_path: str = "") -> List[Dict[str, Any]]:
        target = self._confine(rel_path) if rel_path else self.root
        if not target.exists() or not target.is_dir():
            return []
        entries = []
        for child in sorted(target.iterdir()):
            entries.append({
                "name": child.name,
                "path": str(child.relative_to(self.root)),
                "is_dir": child.is_dir(),
                "size": child.stat().st_size if child.is_file() else None,
            })
        return entries


class FsJailManager:
    """Manages all filesystem jails by GUID."""

    def __init__(self, base_dir: str | Path):
        import os

        self.base_dir = Path(os.path.abspath(str(base_dir)))
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._jails: Dict[str, FsJail] = {}
        self._load_existing()

    def _load_existing(self) -> None:
        if self.base_dir.exists():
            for child in self.base_dir.iterdir():
                if child.is_dir():
                    self._jails[child.name] = FsJail(child.name, self.base_dir)

    def create(self, guid: Optional[str] = None) -> FsJail:
        if guid is None:
            guid = uuid.uuid4().hex
        if guid in self._jails:
            return self._jails[guid]
        jail = FsJail(guid, self.base_dir)
        self._jails[guid] = jail
        return jail

    def get(self, guid: str) -> Optional[FsJail]:
        if guid not in self._jails:
            if (self.base_dir / guid).is_dir():
                self._jails[guid] = FsJail(guid, self.base_dir)
            else:
                return None
        return self._jails[guid]

    def list_guids(self) -> List[str]:
        return sorted(self._jails.keys())

    def jail_info(self, guid: str) -> Optional[Dict[str, Any]]:
        jail = self.get(guid)
        if jail is None:
            return None
        return {
            "guid": jail.guid,
            "root": str(jail.root),
            "file_count": len([f for f in jail.root.rglob("*") if f.is_file()]),
        }


class AgentBrainFsJailMCPServer:
    """MCP server exposing jailed filesystem operations."""

    def __init__(self, base_dir: str | Path):
        self.manager = FsJailManager(base_dir)

    # ------------------------------------------------------------------
    # MCP manifest
    # ------------------------------------------------------------------
    @staticmethod
    def get_tools_manifest() -> List[Dict[str, Any]]:
        return [
            {
                "name": "fs_jail_create",
                "description": "Create a new isolated filesystem jail directory.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Optional custom GUID for the jail. Auto-generated if omitted.",
                        }
                    },
                },
            },
            {
                "name": "fs_jail_list",
                "description": "List all existing filesystem jail GUIDs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                },
            },
            {
                "name": "fs_jail_write",
                "description": "Write text content to a file within a jail (path confined to jail root).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Relative path within the jail.",
                        },
                        "content": {
                            "type": "string",
                            "description": "Text content to write.",
                        },
                    },
                    "required": ["guid", "path", "content"],
                },
            },
            {
                "name": "fs_jail_read",
                "description": "Read text content from a file within a jail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Relative path within the jail.",
                        },
                    },
                    "required": ["guid", "path"],
                },
            },
            {
                "name": "fs_jail_delete",
                "description": "Delete a file or directory within a jail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Relative path within the jail.",
                        },
                    },
                    "required": ["guid", "path"],
                },
            },
            {
                "name": "fs_jail_list_files",
                "description": "List files and directories within a jail directory.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Relative directory path within the jail (default: jail root).",
                            "default": "",
                        },
                    },
                    "required": ["guid"],
                },
            },
            {
                "name": "fs_jail_mkdir",
                "description": "Create a subdirectory within a jail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        },
                        "path": {
                            "type": "string",
                            "description": "Relative directory path within the jail.",
                        },
                    },
                    "required": ["guid", "path"],
                },
            },
            {
                "name": "fs_jail_info",
                "description": "Get info about a specific jail.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "guid": {
                            "type": "string",
                            "description": "Jail GUID.",
                        }
                    },
                    "required": ["guid"],
                },
            },
        ]

    @staticmethod
    def get_resources_manifest() -> List[Dict[str, Any]]:
        return [
            {
                "uri": "fs_jail://jails",
                "name": "All Jails",
                "description": "List of all filesystem jail GUIDs.",
                "mimeType": "application/json",
            },
        ]

    @staticmethod
    def get_prompts_manifest() -> List[Dict[str, Any]]:
        return []

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------
    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(arguments or {})

        if name == "fs_jail_create":
            return self._jail_create(args)
        if name == "fs_jail_list":
            return self._jail_list()
        if name == "fs_jail_write":
            return self._jail_write(args)
        if name == "fs_jail_read":
            return self._jail_read(args)
        if name == "fs_jail_delete":
            return self._jail_delete(args)
        if name == "fs_jail_list_files":
            return self._jail_list_files(args)
        if name == "fs_jail_mkdir":
            return self._jail_mkdir(args)
        if name == "fs_jail_info":
            return self._jail_info(args)

        raise ValueError(f"Unknown MCP tool: {name!r}")

    # ------------------------------------------------------------------
    # Resource reading
    # ------------------------------------------------------------------
    def read_resource(self, uri: str) -> Dict[str, Any]:
        import json
        if uri == "fs_jail://jails":
            data = {"jails": self.manager.list_guids()}
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
        raise ValueError(f"Unknown MCP prompt: {name!r}")

    # ------------------------------------------------------------------
    # Internal implementations
    # ------------------------------------------------------------------
    def _jail_create(self, args: Dict[str, Any]) -> Dict[str, Any]:
        guid = args.get("guid")
        jail = self.manager.create(guid)
        return {"guid": jail.guid, "root": str(jail.root), "created": True}

    def _jail_list(self) -> Dict[str, Any]:
        return {"jails": self.manager.list_guids(), "count": len(self.manager.list_guids())}

    def _jail_write(self, args: Dict[str, Any]) -> Dict[str, Any]:
        jail = self.manager.get(str(args["guid"]))
        if jail is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        try:
            jail.write_text(str(args["path"]), str(args["content"]))
            return {"guid": jail.guid, "path": args["path"], "written": True}
        except PathEscapeError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": str(exc)}

    def _jail_read(self, args: Dict[str, Any]) -> Dict[str, Any]:
        jail = self.manager.get(str(args["guid"]))
        if jail is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        try:
            content = jail.read_text(str(args["path"]))
            return {"guid": jail.guid, "path": args["path"], "content": content}
        except PathEscapeError as exc:
            return {"error": str(exc)}
        except FileNotFoundError:
            return {"error": f"File not found: {args['path']}"}
        except Exception as exc:
            return {"error": str(exc)}

    def _jail_delete(self, args: Dict[str, Any]) -> Dict[str, Any]:
        jail = self.manager.get(str(args["guid"]))
        if jail is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        try:
            jail.delete(str(args["path"]))
            return {"guid": jail.guid, "path": args["path"], "deleted": True}
        except PathEscapeError as exc:
            return {"error": str(exc)}
        except FileNotFoundError:
            return {"error": f"File not found: {args['path']}"}
        except Exception as exc:
            return {"error": str(exc)}

    def _jail_list_files(self, args: Dict[str, Any]) -> Dict[str, Any]:
        jail = self.manager.get(str(args["guid"]))
        if jail is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        path = args.get("path", "")
        try:
            entries = jail.list_dir(path)
            return {"guid": jail.guid, "path": path, "entries": entries, "count": len(entries)}
        except PathEscapeError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": str(exc)}

    def _jail_mkdir(self, args: Dict[str, Any]) -> Dict[str, Any]:
        jail = self.manager.get(str(args["guid"]))
        if jail is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        try:
            jail.mkdir(str(args["path"]))
            return {"guid": jail.guid, "path": args["path"], "created": True}
        except PathEscapeError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": str(exc)}

    def _jail_info(self, args: Dict[str, Any]) -> Dict[str, Any]:
        info = self.manager.jail_info(str(args["guid"]))
        if info is None:
            return {"error": f"Jail '{args['guid']}' not found"}
        return info

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 handler
    # ------------------------------------------------------------------
    def _handle_single_rpc(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        import json
        import traceback

        method = str(request.get("method", ""))
        req_id = request.get("id")
        params = request.get("params") or {}

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
                            "text": json.dumps(payload, indent=2, default=str),
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
            traceback.print_exc()
            if req_id is None:
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {exc}",
                },
            }

    def handle_rpc(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle a JSON-RPC request (single or batch)."""
        if isinstance(request, list):
            responses = []
            for req in request:
                resp = self._handle_single_rpc(req)
                if resp is not None:
                    responses.append(resp)
            return responses if responses else None
        return self._handle_single_rpc(request)
