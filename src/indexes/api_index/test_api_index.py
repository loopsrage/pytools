import unittest

from indexes.api_index.api_index import ApiIndex
from jdelib.jdelib import AisSettings, JdeApi
from otmlib.otmlib import OtmApi, OtmSettings


class TestApiIndex(unittest.TestCase):

    def test_api_index(self):
        idx = ApiIndex()
        jde = JdeApi(AisSettings(server="jde.example.com", port=443))
        otm = OtmApi(OtmSettings(user="u", password="p", server="otm"))
        idx.register_apis({
            "JDE": jde,
            "OTM": otm,
        })
        loaded_jde, was_loaded_jde = idx.api("JDE")
        loaded_otm, was_loaded_otm = idx.api("OTM")
        assert loaded_jde is jde and was_loaded_jde is True
        assert loaded_otm is otm and was_loaded_otm is True
        assert dict(idx.list_api()) == {"JDE": jde, "OTM": otm}


if __name__ == "__main__":
    unittest.main()
