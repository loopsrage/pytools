import hashlib
import pathlib
import tempfile
import unittest
from unittest import mock

from gclib.blob import (
    GcsBlob,
    GcsBlobConfig,
)


class MemoryGcsBlob(GcsBlob):
    def __init__(self):
        from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
        FSBase.__init__(self, "memory")
        self.bucket_name = "my-bucket"


def _blob_with_client():
    blob = GcsBlob.__new__(GcsBlob)
    blob._filesystem = "gcs"
    blob._storage_options = {}
    blob._index = mock.MagicMock()
    blob._key = "directories"
    blob._fs = mock.MagicMock()
    blob.bucket_name = "my-bucket"
    return blob


class TestGcsBlobConfig(unittest.TestCase):

    def test_fields(self):
        cfg = GcsBlobConfig(bucket_name="my-bucket", project="my-project")
        assert cfg.bucket_name == "my-bucket"
        assert cfg.project == "my-project"

    def test_extra_allowed(self):
        cfg = GcsBlobConfig(bucket_name="b", custom="x")
        assert cfg.custom == "x"


class TestGcsBlobMethods(unittest.TestCase):

    def test_full_key_prefixes_bucket(self):
        blob = _blob_with_client()
        assert blob._full_key("dir/x.txt") == "my-bucket/dir/x.txt"
        assert blob._full_key("my-bucket/dir/x.txt") == "my-bucket/dir/x.txt"

    def test_get_remote_md5_success(self):
        blob = _blob_with_client()
        blob.client.info.return_value = {"etag": '"abcd1234"'}
        assert blob.get_remote_md5("dir/x.txt") == "abcd1234"

    def test_get_remote_md5_exception(self):
        blob = _blob_with_client()
        blob.client.info.side_effect = Exception("boom")
        assert blob.get_remote_md5("dir/x.txt") is None

    def test_write_if_different_match(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=hashlib.md5(b"data").hexdigest().lower())
        assert blob.write_if_different("dir/x.txt", b"data") is False
        blob.client.pipe_file.assert_not_called()

    def test_write_if_different_mismatch(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value="different")
        assert blob.write_if_different("dir/x.txt", b"data") is True
        blob.client.pipe_file.assert_called_once_with("my-bucket/dir/x.txt", b"data")

    def test_load_or_store_exists_match(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=hashlib.md5(b"data").hexdigest().lower())
        data, loaded = blob.load_or_store("dir/x.txt", lambda: b"data")
        assert data == b"data" and loaded is True

    def test_load_or_store_exists_mismatch(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value="different")
        data, loaded = blob.load_or_store("dir/x.txt", lambda: b"data")
        assert data == b"data" and loaded is False

    def test_load_or_store_not_exists(self):
        blob = _blob_with_client()
        blob.get_remote_md5 = mock.MagicMock(return_value=None)
        data, loaded = blob.load_or_store("dir/x.txt", lambda: b"data")
        assert data == b"data" and loaded is False

    def test_transfer_missing_target(self):
        blob = _blob_with_client()
        blob.list = mock.MagicMock(return_value=[])
        dest = mock.MagicMock()
        with self.assertRaises(AttributeError):
            blob.transfer(dest, "dir/*", None)

    def test_transfer(self):
        blob = _blob_with_client()
        blob.client.open.return_value.__enter__.return_value.read.return_value = b"remote"
        blob.list = mock.MagicMock(return_value=["gs://my-bucket/dir/a.txt"])
        dest = mock.MagicMock()
        dest.load_or_store = mock.MagicMock(return_value=(b"remote", False))
        blob.transfer(dest, "dir/*", "/tmp/dest")
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


class TestGcsBlobFsIndex(unittest.TestCase):

    def test_load_or_store_and_transfer_via_index(self):
        with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
            gcs = MemoryGcsBlob()
            from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
            lc = FSBase(filesystem="file")

            src_file = pathlib.Path(tmpdir1) / "item.txt"
            src_file.write_bytes(b"customs_data")

            from indexes.fsindex.fsindex import FilesystemIndex
            fss = FilesystemIndex()
            fss.register_filesystems({"local": lc, "gcs": gcs})
            gcs_fs, _ = fss.filesystem("gcs")
            lc_fs, _ = fss.filesystem("local")

            stored, was_loaded = gcs_fs.load_or_store("/tariffs/item.txt", lambda: b"customs_data")
            assert stored == b"customs_data" and was_loaded is False

            gcs_fs.transfer(lc_fs, "/tariffs/*", tmpdir2)
            assert (pathlib.Path(tmpdir2) / "item.txt").read_bytes() == b"customs_data"


if __name__ == "__main__":
    unittest.main()
