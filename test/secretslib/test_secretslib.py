import os
import tempfile
from pathlib import Path

from secretslib.secretslib import Secrets, get, required, clear, load_env_file


def test_get_from_env(monkeypatch):
    monkeypatch.setenv("TEST_SECRET_ENV", "supersecret")
    s = Secrets()
    assert s.get("TEST_SECRET_ENV") == "supersecret"


def test_get_from_file(tmp_path, monkeypatch):
    secret_file = tmp_path / "my_secret.txt"
    secret_file.write_text("filebasedsecret")
    monkeypatch.setenv("MY_SECRET_FILE", str(secret_file))
    s = Secrets()
    assert s.get("MY_SECRET") == "filebasedsecret"


def test_required_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    s = Secrets()
    try:
        s.required("DOES_NOT_EXIST")
    except EnvironmentError:
        pass
    else:
        raise AssertionError("expected EnvironmentError")


def test_default_returned(monkeypatch):
    monkeypatch.delenv("NO_SUCH_SECRET", raising=False)
    s = Secrets()
    assert s.get("NO_SUCH_SECRET", default="fallback") == "fallback"


def test_clear(monkeypatch):
    monkeypatch.setenv("CLEAR_ME", "value1")
    s = Secrets()
    assert s.get("CLEAR_ME") == "value1"
    s.clear("CLEAR_ME")
    assert s.get("CLEAR_ME") == "value1"  # re-reads from env


def test_load_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LOADED_ONE=one\nLOADED_TWO=two\n# comment\nBADLINE\n")
    s = Secrets()
    loaded = s.load_env_file(env_file)
    assert loaded == 2
    assert os.getenv("LOADED_ONE") == "one"
    assert os.getenv("LOADED_TWO") == "two"


def test_module_level_functions(monkeypatch):
    monkeypatch.setenv("MODULE_FUNC_SECRET", "modvalue")
    assert get("MODULE_FUNC_SECRET") == "modvalue"
    assert required("MODULE_FUNC_SECRET") == "modvalue"
