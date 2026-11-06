import base64
import hashlib
import pathlib
import tempfile
import unittest
from unittest import mock

from azurelib.blob import (
    AzureBlob,
    AzureBlobConfig,
    calculate_local_md5,
    normalize_md5,
)
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class MemoryAzureBlob(AzureBlob):
    def __init__(self):
        FSBase.__init__(self, "memory")


def _blob_with_client():
    blob = AzureBlob.__new__(AzureBlob)
    blob._filesystem = "abfs"
    blob._storage_options = {}
    blob._index = mock.MagicMock()
    blob._key = "directories"
    blob._fs = mock.MagicMock()
    return blob


class TestAzureBlobConfig(unittest.TestCase):

    def test_fields(self):
        cfg = AzureBlobConfig(account_name="acct", account_key="key", ssl=None)
        assert cfg.account_name == "acct"
        assert cfg.account_key == "key"
        assert cfg.ssl is None

    def test_extra_allowed(self):
        cfg = AzureBlobConfig(account_name="acct", custom_field="x")
        assert cfg.custom_field == "x"


class TestAzureMd5Helpers(unittest.TestCase):

    def test_calculate_local_md5(self):
        data = b"tariff_payload_123"
        assert calculate_local_md5(data) == hashlib.md5(data).hexdigest().lower()

    def test_normalize_md5_none(self):
        assert normalize_md5(None) is None

    def test_normalize_md5_bytes(self):
        data = b"hello"
        assert normalize_md5(hashlib.md5(data).digest()) == hashlib.md5(data).hexdigest().lower()

    def test_normalize_md5_quoted_hex(self):
        hexd = hashlib.md5(b"x").hexdigest()
        assert normalize_md5(f'"{hexd.upper()}"') == hexd

    def test_normalize_md5_plain_hex(self):
        hexd = hashlib.md5(b"x").hexdigest()
        assert normalize_md5(hexd.upper()) == hexd

    def test_normalize_md5_base64(self):
        data = b"hello"
        b64 = base64.b64encode(hashlib.md5(data).digest()).decode("ascii")
        assert normalize_md5(b64) == hashlib.md5(data).hexdigest().lower()

    def test_normalize_md5_unknown(self):
        assert normalize_md5("not-a-md5") == "not-a-md5"


class TestAzureBlobMethods(unittest.TestCase):

    def test_get_remote_md5_success(self):
        blob = _blob_with_client()
        blob.client.info.return_value = {"content_settings": {"content_md5": "aaaa"}}
        assert blob.get_remote_md5("/x.txt") == "aaaa"

    def test_get_remote_md5_exception(self):
        blob = _blob_with_client()
        blob.client.info.side_effect = Exception("boom")
        assert blob.get_remote_md5("/x.txt") is None

    def test_write_if_different_match(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=hashlib.md5(b"data").hexdigest().lower())
        assert blob.write_if_different("/x.txt", b"data") is False
        blob.client.pipe_file.assert_not_called()

    def test_write_if_different_mismatch(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value="different")
        assert blob.write_if_different("/x.txt", b"data") is True
        blob.client.pipe_file.assert_called_once_with("/x.txt", b"data")

    def test_load_or_store_exists_match(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=hashlib.md5(b"data").hexdigest().lower())
        data, loaded = blob.load_or_store("/x.txt", lambda: b"data")
        assert data == b"data" and loaded is True
        blob.client.pipe_file.assert_not_called()

    def test_load_or_store_exists_mismatch(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value="different")
        data, loaded = blob.load_or_store("/x.txt", lambda: b"data")
        assert data == b"data" and loaded is False
        blob.client.pipe_file.assert_called_once()

    def test_load_or_store_not_exists(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=None)
        data, loaded = blob.load_or_store("/x.txt", lambda: b"data")
        assert data == b"data" and loaded is False
        blob.client.pipe_file.assert_called_once()

    def test_transfer_missing_target(self):
        blob = _blob_with_client()
        blob.list = mock.MagicMock(return_value=[])
        dest = mock.MagicMock()
        with self.assertRaises(AttributeError):
            blob.transfer(dest, "/x/*", None)

    def test_transfer(self):
        blob = _blob_with_client()
        blob.client.open.return_value.__enter__.return_value.read.return_value = b"remote"
        blob.list = mock.MagicMock(return_value=["abfs://container/dir/a.txt"])
        dest = mock.MagicMock()
        dest.load_or_store = mock.MagicMock(return_value=(b"remote", False))
        blob.transfer(dest, "/dir/*", "/tmp/dest")
        dest.load_or_store.assert_called_once()

    def test_sync(self):
        blob = _blob_with_client()
        blob.transfer = mock.MagicMock()
        dest = mock.MagicMock()
        dest.transfer = mock.MagicMock()
        blob.sync(dest, "/local", "/remote")
        blob.transfer.assert_any_call(dest, "/local/*", "/remote/")
        dest.transfer.assert_any_call(blob, "/remote/*", "/local")

    def test_put(self):
        blob = _blob_with_client()
        blob.put("/local.txt", "/remote.txt", overwrite=True)
        blob.client.put.assert_called_once_with("/local.txt", "/remote.txt", overwrite=True)


class TestAzureBlobFsIndex(unittest.TestCase):

    def test_load_or_store_and_transfer_via_index(self):
        with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
            az = MemoryAzureBlob()
            lc = FSBase(filesystem="file")

            src_file = pathlib.Path(tmpdir1) / "item.txt"
            src_file.write_bytes(b"customs_data")

            from indexes.fsindex.fsindex import FilesystemIndex
            fss = FilesystemIndex()
            fss.register_filesystems({"local": lc, "azure": az})
            az_fs, _ = fss.filesystem("azure")
            lc_fs, _ = fss.filesystem("local")

            stored, was_loaded = az_fs.load_or_store("/tariffs/item.txt", lambda: b"customs_data")
            assert stored == b"customs_data" and was_loaded is False

            az_fs.transfer(lc_fs, "/tariffs/*", tmpdir2)
            assert (pathlib.Path(tmpdir2) / "item.txt").read_bytes() == b"customs_data"


if __name__ == "__main__":
    unittest.main()
