import datetime
import functools
import json
import pathlib
import re
import time

import yaml
from typing import Any, Iterator, IO, Callable, TypeVar, Type, Union

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from thread_safe.containers.containers.container import build_container_tree, Container
from thread_safe.index import Index

# Initialize the provided Index class
APP_INDEX = Index()
APPS_NAMESPACE = "apps"
APPS_CONTAINER = "container"
ENV_PATTERN = re.compile(r"\.env$", re.IGNORECASE)

def read_settings(settings, path_delim=None) -> Container:
    """
    Attempts to read settings as json, if TypeError is raised attempt yaml.safe_load
    raise Exception if neither succeeds
    :param path_delim:
    :param settings:
    :return:
    """
    data = None
    try:
        data = json.loads(settings)
    except (json.decoder.JSONDecodeError, TypeError):
        pass

    if data is None:
        try:
            data = yaml.safe_load(settings)
        except (yaml.YAMLError, TypeError):
            raise Exception('Invalid YAML/JSON')

    if data is None:
        raise Exception('Invalid YAML/JSON')

    return build_container_tree(start=data, path_delim=path_delim or ".")

def to_pascal_case(text):
    if not text:
        return ""

    # Only capitalize the first character; leave the rest alone
    text = text.lower()
    return text[0].upper() + text[1:]

def deep_merge(dict1, dict2):
    """Recursively merges dict2 into dict1."""
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            deep_merge(dict1[key], value)
        else:
            dict1[key] = value
    return dict1

@functools.cache
def enabled(feature_name: str = None) -> bool:
    """Returns true if Feature.Enabled"""
    is_enabled: bool = load_app_setting(feature_name, "Enabled")
    return is_enabled is not None and is_enabled

def path_delim():
    cont: Container = APP_INDEX.load_from_index(APPS_CONTAINER, "root")
    if cont is None:
        raise KeyError("settings failed to get root")
    return cont.path_delim

def setting(feature_name: str, path: str):
    cont: Container = APP_INDEX.load_from_index(APPS_CONTAINER, "root")
    if cont is None:
        raise KeyError("settings failed to get root")
    return cont.read_primitive_value(f"{feature_name}{cont.path_delim}{path}")

def app_settings(feature_name: str):
    return functools.partial(setting, feature_name)

def all_settings():
    idx_apps =  APP_INDEX.load_index(APPS_CONTAINER)
    return idx_apps["root"].value

def parse_env_to_nested_dict(env_data, split_key=None):
    root = {}
    if split_key is None:
        split_key = "_"

    for key, value in env_data.items():
        # 1. Split the key and convert to PascalCase
        parts = [to_pascal_case(p) for p in key.split(split_key)]

        # 2. Parse Value Types
        if isinstance(value, str):
            if value.lower() == "true": value = True
            elif value.lower() == "false": value = False
            elif value.lower() == "none": value = None
            elif value.isdigit(): value = int(value)
            elif "," in value: value = [int(x) if x.isdigit() else x for x in value.split(",")]

        # 3. Traverse the actual root directly to ensure we don't overwrite siblings
        current = root
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                # Assign the final value
                current[part] = value
            else:
                # ONLY create a new dict if it doesn't already exist
                if part not in current or not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]

    return root

def restore(path: Union[str, pathlib.Path, bytes], path_delim = None) -> None:
    """
    Restores app settings from a file.
    Uses provided read_settings logic for JSON/YAML and adds .env support.
    """
    raw_content: bytes
    is_env_format = ENV_PATTERN.search(str(path))

    if isinstance(path, bytes):
        raw_content = path

    if isinstance(path, str):
        if path.strip().startswith(("{", "[")):
            raw_content = path.encode("utf-8")
        else:
            path = pathlib.Path(path)
            if not path.exists():
                raise FileNotFoundError(f"Configuration file not found: {path}")
            raw_content = path.read_bytes()
    elif isinstance(path, pathlib.Path):
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        raw_content = path.read_bytes()

    if is_env_format:
        out = {}
        for line in raw_content.decode("utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                key, value = line.split("=", 1)
                out[key.strip()] = value.strip()
        app_data = parse_env_to_nested_dict(out)
        raw_content = json.dumps(app_data).encode("utf-8")

    decoded_str = raw_content.decode("utf-8")

    cont = read_settings(decoded_str, path_delim)
    APP_INDEX.store_in_index(APPS_CONTAINER, "root", cont)
    for app_name, settings in cont.range_values:
        if app_name == "root":
            for key, value in settings.items():
                add(key, {key: value})
            continue

        app_config = {app_name: settings}
        add(app_name, app_config)

def add(app_name: str, config: Any) -> None:
    """Stores config in the 'apps' index."""
    APP_INDEX.store_in_index(APPS_NAMESPACE, app_name, config)

def apps() -> list[str]:
    """Returns all registered app names from the index."""
    try:
        return [key for key, _ in APP_INDEX.range_index(APPS_NAMESPACE)
                if key.count(path_delim()) == 0]
    except KeyError:
        return []

def load_app_settings(app_name: str) -> dict[str, Any]:
    """Loads app settings. Mimics Go's type switch behavior."""
    try:
        data = APP_INDEX.load_from_index(APPS_NAMESPACE, app_name)
        return data if isinstance(data, dict) else {}
    except KeyError:
        return {}

def load_app_setting(app_name: str, setting: str) -> Any:
    """Retrieves a specific nested setting."""
    # Logic matches Go: LoadAppSettings(appName)[appName].(map)[setting]
    return load_app_settings(app_name).get(app_name, {}).get(setting)

def to_yaml(app_name: str) -> bytes:
    """Returns YAML bytes for the app settings."""
    settings = load_app_settings(app_name)
    return yaml.dump(settings, sort_keys=False).encode("utf-8")

def to_env(app_name: str) -> bytes:
    """Flattens settings into ENV format (bytes) using recursion."""
    def flatten(data: Any, prefix: str = "") -> Iterator[str]:
        if isinstance(data, dict):
            for k, v in data.items():
                yield from flatten(v, f"{prefix}_{k}" if prefix else k)
        elif isinstance(data, (list, tuple)):
            value = ",".join(map(str, data))
            yield f"{prefix.upper()}={value}"
        else:
            yield f"{prefix.upper()}={data}"

    settings = load_app_settings(app_name)
    content = "\n".join(flatten(settings)) + "\n"
    return content.encode("utf-8")

def to_json(app_name: str) -> bytes:
    """Returns JSON bytes for the app settings."""
    settings = load_app_settings(app_name)
    return json.dumps(settings, sort_keys=False).encode("utf-8")

def unmarshal_app_settings_dict(app_name: str, app_config: dict) -> None:
    """Updates the provided app_config dict with the app's settings."""
    settings = load_app_settings(app_name)
    if not settings:
        raise ValueError(f"settings not loaded for {app_name}")

    # Go logic: set[appName] is used as the source
    app_data = settings.get(app_name)
    if isinstance(app_data, dict):
        app_config.update(app_data)


T = TypeVar("T", bound=Union[BaseModel, BaseSettings])
def unmarshal(app: dict, settings_cls: Type[T]) -> T:
    return settings_cls.model_validate(app)

def unmarshal_app_settings(app_name: str, settings_cls: Type[T]) -> T:
    """Loads and unmarshals app settings into a Pydantic BaseSettings class."""
    settings_data = load_app_settings(app_name)
    if not settings_data:
        raise ValueError(f"settings not loaded for {app_name}")

    app_data = settings_data.get(app_name)
    if not isinstance(app_data, dict):
        raise ValueError(f"Expected dict for {app_name}, got {type(app_data)}")

    return settings_cls.model_validate(app_data)

def unmarshal_settings(settings_cls: Type[T]) -> T:
    """Loads and unmarshals app settings into a Pydantic BaseSettings class."""
    settings_data = all_settings()
    if not settings_data:
        raise ValueError(f"settings not loaded for")

    return settings_cls.model_validate(settings_data)

def write_all(writer: IO[bytes], formatter: Callable[[str], bytes]) -> bytes:
    """
    Iterates over all apps, formats their data, writes to the writer,
    and returns the combined bytes of all formatted data.
    """
    combined_bytes = b"".join(formatter(name) for name in apps())

    writer.seek(0)
    writer.write(combined_bytes)
    writer.truncate()
    return combined_bytes

def write_all_yaml(writer: IO[bytes]) -> bytes:
    """Writes all app configurations in YAML format and returns the bytes."""
    return write_all(writer, to_yaml)

def write_all_env(writer: IO[bytes]) -> bytes:
    """Writes all app configurations in ENV format and returns the bytes."""
    return write_all(writer, to_env)

def write_all_json(writer: IO[bytes]) -> bytes:
    """Writes all app configurations in JSON format and returns the bytes."""
    return write_all(writer, to_json)

def write_all_types(directory, name):
    write_time = time.time()
    file_timestamp = datetime.datetime.fromtimestamp(write_time).strftime('%Y%m%d_%H%M%S')
    with open(f"{directory}/{name}_{file_timestamp}-apps.env", "wb") as f:
        write_all_env(f)

    with open(f"{directory}/{name}_{file_timestamp}-apps.yaml", "wb") as f:
        write_all_yaml(f)

    with open(f"{directory}/{name}_{file_timestamp}-apps.json", "wb") as f:
        write_all_json(f)

def print_all_types():
    with open(f"/tmp/settings.txt", "wb") as f:
        env_content = write_all_env(f)
        print("-- ENV FORMAT --:")
        print(env_content.decode())

        yml_content = write_all_yaml(f)
        print("-- YML FORMAT --:")
        print(yml_content.decode())

        jsn_content = write_all_json(f)
        print("-- JSN FORMAT --:")
        print(jsn_content.decode())


def settings_for_namespace(namespace):
    return functools.partial(setting, namespace)

def enabled_flag(feature_name: str):
    """
    Returns None if not Feature.Enabled, or returns unmodified if Feature.Enabled
    :param feature_name:
    :return:
    """
    def decorator(func):

        if not enabled(feature_name=feature_name):

            @functools.wraps(func)
            def disabled_wrapper(*args, **kwargs):
                print(f"Skipping {func.__name__}: {feature_name} is disabled.")
                return None
            return disabled_wrapper

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

