"""Settings accessor for agent_brain.

Loads settings.yaml using the repo's settings module (settings/helper.py)
so all configuration lives in one YAML file and can be read via
settings.load_app_settings / settings.setting / settings.all_settings.
"""

import os
import sys
from pathlib import Path

# Ensure the repo src is importable for the settings module
_REPO_SRC = str(Path(__file__).resolve().parents[3] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from settings.helper import (
    all_settings,
    apps,
    enabled,
    load_app_settings,
    restore,
    setting,
    to_json,
    to_yaml,
)
from settings.fssettings import FsSettings

_APP_NAME = "agent_brain"
_SETTINGS_FILE = Path(__file__).resolve().parent / "settings.yaml"

_loaded = False


def load() -> None:
    """Load settings.yaml into the app settings index."""
    global _loaded
    if _loaded:
        return
    restore(str(_SETTINGS_FILE))
    _loaded = True


def get(path: str, default=None):
    """Read a single setting by dotted path, e.g. get('scraper.headless')."""
    load()
    return setting(_APP_NAME, path) or default


def all() -> dict:
    """Return all agent_brain settings as a dict."""
    load()
    return load_app_settings(_APP_NAME)


def raw_all() -> dict:
    """Return every app's settings currently in the index."""
    load()
    return all_settings()


def to_yaml_bytes() -> bytes:
    """Export agent_brain settings as YAML bytes."""
    load()
    return to_yaml(_APP_NAME)


def to_json_bytes() -> bytes:
    """Export agent_brain settings as JSON bytes."""
    load()
    return to_json(_APP_NAME)


def fs_settings():
    """Return an FsSettings instance backed by settings.yaml (repo pattern)."""
    load()
    return FsSettings("file", str(_SETTINGS_FILE))


def enabled(feature: str) -> bool:
    """Check if a feature flag is enabled."""
    load()
    return enabled(feature) is not None and enabled(feature)
