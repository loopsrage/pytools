import unittest

from indexes.api_index.api_index import ApiIndex
from jdelib.jdelib import JdeApi
from otmlib.otmlib import OtmApi


class Test(unittest.TestCase):

    idx = ApiIndex()

    _ais_server = None
    _otm_server = None
    _ais_port = None

    def test_api_index(self):
        self.idx.register_api({
            "JDE": JdeApi(self._ais_server, self._ais_port),
            "OTM": OtmApi(self._otm_server, auth=None),
        })
