import json
import pathlib
import tempfile
import unittest

import yaml

from settings.fssettings import FsSettings
from settings.helper import (
    all_settings,
    app_settings,
    apps,
    deep_merge,
    enabled,
    enabled_flag,
    print_all_types,
    restore,
    setting,
    settings_for_namespace,
    to_env,
    to_json,
    to_yaml,
    unmarshal_app_settings,
    unmarshal_app_settings_dict,
    unmarshal_settings,
    write_all_types,
)
from settings.schemas.schemas import (
    AppSettings,
    Example,
    Example2,
    Example3,
    Example4,
    NestedSettings,
)


class TestSettings(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.settings_dir = pathlib.Path(self._tmpdir.name)
        self.yaml_file = pathlib.Path(__file__).parent / "test_env.yaml"

        # Generate JSON and .env equivalents in temp dir from test_env.yaml
        yaml_data = yaml.safe_load(self.yaml_file.read_text())
        self.json_file = self.settings_dir / "test_env.json"
        self.json_file.write_text(json.dumps(yaml_data))

        self.env_file = self.settings_dir / "test.env"
        self.env_file.write_text(
            "TESTFEATURE_ENABLED=true\n"
            "TESTFEATURE2_ENABLED=true\n"
            "GLOBAL_SHALLOWINT=1\n"
            "GLOBAL_DATA=1,2,3\n"
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_model_dump_and_write_all_types(self):
        settings_json = AppSettings(
            ex1=Example(key="k1", value="v1"),
            ex2=Example2(key="k2", value="v2"),
            ex3=Example3(key="k3", value="v3"),
            ex4=Example4(key="k4", value="v4"),
            nested=NestedSettings(),
        ).model_dump_json()
        restore(settings_json)
        write_all_types(str(self.settings_dir), "app_settings")
        written = list(self.settings_dir.glob("app_settings_*-apps.*"))
        assert len(written) == 3

    def test_restore_from_yml(self):
        restore(self.yaml_file)
        assert enabled("TestFeature")
        assert not enabled("Global")
        assert enabled("TestFeature2")
        assert "TestFeature" in apps()

    def test_restore_from_json(self):
        restore(self.json_file)
        assert enabled("TestFeature")
        assert not enabled("Global")
        assert enabled("TestFeature2")

    def test_restore_from_env_and_enabled_flag(self):
        restore(self.env_file)
        assert enabled("Testfeature")
        assert not enabled("Global")
        assert enabled("Testfeature2")
        assert setting("Global", "Shallowint") == 1
        assert setting("Global", "Data") == [1, 2, 3]

        @enabled_flag("Testfeature")
        def active_fn(x: int) -> int:
            return x * 2

        @enabled_flag("Global")
        def inactive_fn(x: int) -> int:
            return x * 2

        assert active_fn(5) == 10
        assert inactive_fn(5) is None

    def test_app_settings_and_unmarshal(self):
        settings_json = AppSettings(
            ex1=Example(key="hello", value="world"),
            nested=NestedSettings(),
        ).model_dump_json()
        restore(settings_json)
        assert setting("nested", "test") == "not nested"
        assert setting("nested", "nest.test") == "nested_layer2"
        assert setting("nested", "nest.nest.test") == "nested_layer1"
        assert setting("nested", "nest.nest.nest.test") == "nested_layer"

        aps_config = settings_for_namespace("nested")
        assert aps_config("test") == "not nested"

        aps_config2 = app_settings("nested.nest")
        assert aps_config2("test") == "nested_layer2"

        ex1 = unmarshal_app_settings("ex1", Example)
        assert ex1.key == "hello" and ex1.value == "world"

        target_dict = {}
        unmarshal_app_settings_dict("ex1", target_dict)
        assert target_dict == {"key": "hello", "value": "world"}

        full = unmarshal_settings(AppSettings)
        assert full.ex1.key == "hello"
        assert isinstance(all_settings(), dict)

        assert b"hello" in to_yaml("ex1")
        assert b"hello" in to_json("ex1")
        assert b"EX1_KEY=hello" in to_env("ex1")

        merged = deep_merge({"a": {"x": 1}}, {"a": {"y": 2}})
        assert merged == {"a": {"x": 1, "y": 2}}
        print_all_types()

        fs_settings = FsSettings("file", str(self.yaml_file))
        assert enabled("TestFeature")
        fs_settings.close()


if __name__ == "__main__":
    unittest.main()
