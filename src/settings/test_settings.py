import unittest


from settings.helper import restore, enabled, setting, print_all_types, write_all_types, settings_for_namespace
from settings.schemas.schemas import AppSettings, Example, NestedSettings, Example2, Example3, Example4


class Test(unittest.TestCase):

    def setUp(self):
        self.settings_dir = "/settings"

    def test_model_dump(self):
        settings = AppSettings(
            ex1=Example.model_construct(),
            ex2=Example2.model_construct(),
            ex3=Example3.model_construct(),
            ex4=Example4.model_construct(),
            nested=NestedSettings.model_construct()
        ).model_dump_json()
        restore(settings)
        write_all_types(self.settings_dir, "app_settings")

    def test_restore_from_yml(self):
        restore("./test_env.yaml")
        assert enabled("Testfeature")
        assert not enabled("Global")
        assert enabled("Testfeature2")

    def test_restore_from_json(self):
        restore("./test_env.json")
        assert enabled("Testfeature")
        assert not enabled("Global")
        assert enabled("Testfeature2")


    def test_app_settings(self):
        settings = AppSettings(
            nested=NestedSettings()
        ).model_dump_json()
        restore(settings)
        assert setting("nested", "test") == "not nested"
        assert setting("nested", "nest.test") == "nested_layer2"
        assert setting("nested", "nest.nest.test") == "nested_layer1"
        assert setting("nested", "nest.nest.nest.test") == "nested_layer"


        aps_config = settings_for_namespace("nested")
        assert aps_config("test") == "not nested"

        aps_config = settings_for_namespace("nested.nest")
        assert aps_config("test") == "nested_layer2"

        aps_config = settings_for_namespace("nested.nest.nest")
        assert aps_config("test") == "nested_layer1"

        aps_config = settings_for_namespace("nested.nest.nest.nest")
        assert aps_config("test") == "nested_layer"

        print_all_types()