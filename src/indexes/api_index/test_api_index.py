import unittest

from src.indexes.api_index.api_index import ApiIndex
from src.jdelib.jdelib import JdeApi


class Test(unittest.TestCase):

    idx = ApiIndex()

    _ais_server = None
    _otm_server = None
    _ais_port = None

    def test_api_index(self):
        self.idx.register_api({
            "JDE": JdeApi(self._ais_server, self._ais_port),
        })
