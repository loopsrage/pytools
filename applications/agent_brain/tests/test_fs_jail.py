import os
import tempfile

from applications.agent_brain.mcp.fs_jail import (
    AgentBrainFsJailMCPServer,
    FsJail,
    FsJailManager,
    PathEscapeError,
)


def test_jail_create_and_confinement(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    result = server.call_tool("fs_jail_create", {"guid": "test-jail"})
    assert result["created"] is True
    assert result["guid"] == "test-jail"


def test_jail_write_and_read(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    server.call_tool("fs_jail_create", {"guid": "rw-jail"})
    server.call_tool("fs_jail_write", {
        "guid": "rw-jail", "path": "hello.txt", "content": "hello world"
    })
    result = server.call_tool("fs_jail_read", {"guid": "rw-jail", "path": "hello.txt"})
    assert result["content"] == "hello world"


def test_jail_path_traversal_blocked(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    server.call_tool("fs_jail_create", {"guid": "trap-jail"})
    result = server.call_tool("fs_jail_read", {
        "guid": "trap-jail", "path": "../../../etc/passwd"
    })
    assert "error" in result
    assert "escapes" in result["error"]


def test_jail_list_files(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    server.call_tool("fs_jail_create", {"guid": "list-jail"})
    server.call_tool("fs_jail_write", {
        "guid": "list-jail", "path": "a.txt", "content": "a"
    })
    server.call_tool("fs_jail_write", {
        "guid": "list-jail", "path": "sub/b.txt", "content": "b"
    })
    result = server.call_tool("fs_jail_list_files", {"guid": "list-jail"})
    names = {e["name"] for e in result["entries"]}
    assert names == {"a.txt", "sub"}


def test_jail_delete(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    server.call_tool("fs_jail_create", {"guid": "del-jail"})
    server.call_tool("fs_jail_write", {
        "guid": "del-jail", "path": "tmp.txt", "content": "tmp"
    })
    result = server.call_tool("fs_jail_delete", {"guid": "del-jail", "path": "tmp.txt"})
    assert result["deleted"] is True
    result = server.call_tool("fs_jail_list_files", {"guid": "del-jail"})
    assert result["count"] == 0


def test_jail_mkdir(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    server.call_tool("fs_jail_create", {"guid": "mkdir-jail"})
    result = server.call_tool("fs_jail_mkdir", {"guid": "mkdir-jail", "path": "newdir"})
    assert result["created"] is True


def test_jail_not_found(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    server = AgentBrainFsJailMCPServer(base_dir=str(jail_dir))
    result = server.call_tool("fs_jail_read", {"guid": "nonexistent", "path": "x.txt"})
    assert "error" in result


def test_fsjail_direct_class(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    jail = FsJail("direct-jail", str(jail_dir))
    jail.write_text("test.txt", "direct access")
    assert jail.read_text("test.txt") == "direct access"
    assert jail.exists("test.txt") is True
    jail.delete("test.txt")
    assert jail.exists("test.txt") is False


def test_fsjail_path_escape_direct(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    jail = FsJail("escape-jail", str(jail_dir))
    try:
        jail.read_text("../../../etc/passwd")
    except PathEscapeError:
        pass
    else:
        raise AssertionError("expected PathEscapeError")


def test_fsjail_manager_list(tmp_path):
    jail_dir = tmp_path / "jails"
    jail_dir.mkdir()
    manager = FsJailManager(str(jail_dir))
    manager.create("jail-a")
    manager.create("jail-b")
    guids = manager.list_guids()
    assert "jail-a" in guids
    assert "jail-b" in guids
