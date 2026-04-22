import os
import unittest

from dotenv import load_dotenv

from azurelib.blob import AzureBlobConfig, AzureBlob
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.fsindex.fsindex import FilesystemIndex
from settings.helper import unmarshal_app_settings, restore

load_dotenv()
restore(os.getenv("ENV_FILE"))


class Test(unittest.TestCase):

    def setUp(self):
        config = unmarshal_app_settings("Azure.Storage", AzureBlobConfig)
        self.local_path = "/Users/jarek/IdeaProjects/non_threaded/lib/hslib/test_files"
        self._fss = FilesystemIndex()
        self._fss.register_filesystems({
            "local": FSBase(filesystem="local", path=self.local_path),
            "azure": AzureBlob(config)
        })

    def test_blob_list(self):
        az, _ =self._fss.filesystem("azure")
        lc, _ = self._fss.filesystem("local")
        print(list(az.list("tariffs/*")))
        print(list(lc.list_files()))

    def test_transfer(self):
        az, _ = self._fss.filesystem("azure")
        lc, _ = self._fss.filesystem("local")
        lc.transfer(az, f"{self.local_path}/*", "tariffs/")
        az.transfer(lc, "tariffs/*", self.local_path)
