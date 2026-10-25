import base64
import hashlib
import io
import pathlib
import tempfile
import unittest

from azurelib.blob import (
    AzureBlob,
    AzureBlobConfig,
    AzureIndexAgentSearchConfig,
    AzureSearchVectorStoreConfig,
    calculate_local_md5,
    normalize_md5,
)
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.fsindex.fsindex import FilesystemIndex


class LocalBackedAzureBlob(AzureBlob):
    def __init__(self):
        FSBase.__init__(self, "memory")


class TestAzureBlob(unittest.TestCase):

    def test_md5_helpers_and_configs(self):
        data = b"tariff_payload_123"
        expected_hex = hashlib.md5(data).hexdigest().lower()
        assert calculate_local_md5(data) == expected_hex
        assert normalize_md5(None) is None
        assert normalize_md5(hashlib.md5(data).digest()) == expected_hex
        assert normalize_md5(f'"{expected_hex.upper()}"') == expected_hex

        b64_md5 = base64.b64encode(hashlib.md5(data).digest()).decode("ascii")
        assert normalize_md5(b64_md5) == expected_hex

        cfg = AzureIndexAgentSearchConfig(
            Storage=AzureBlobConfig(account_name="acct", account_key="key"),
            Vector=AzureSearchVectorStoreConfig(index_key="idx", embedding_dimensionality=1536),
        )
        assert cfg.Storage.account_name == "acct"
        assert cfg.Vector.embedding_dimensionality == 1536

    def test_azure_blob_methods_and_transfer(self):
        with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
            az = LocalBackedAzureBlob()
            lc = FSBase(filesystem="file")

            src_file = pathlib.Path(tmpdir1) / "item.txt"
            src_file.write_bytes(b"customs_data")

            fss = FilesystemIndex()
            fss.register_filesystems({"local": lc, "azure": az})
            az_fs, _ = fss.filesystem("azure")
            lc_fs, _ = fss.filesystem("local")

            # load_or_store on AzureBlob
            stored, was_loaded = az_fs.load_or_store("/tariffs/item.txt", lambda: b"customs_data")
            assert stored == b"customs_data" and was_loaded is False

            # Transfer from AzureBlob to local drive
            az_fs.transfer(lc_fs, "/tariffs/*", tmpdir2)
            assert (pathlib.Path(tmpdir2) / "item.txt").read_bytes() == b"customs_data"


if __name__ == "__main__":
    unittest.main()
