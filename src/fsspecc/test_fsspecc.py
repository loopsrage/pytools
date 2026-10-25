import io
import pathlib
import tempfile
import unittest

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from langgraph.prebuilt import ToolRuntime

from fsspecc import AtomicWriteFs, CleanFs, FSBase, ImagesFs, MemFS, get_file_path
from fsspecc.base_fsspecfs.base_toolkit import FSToolkit
from fsspecc.base_fsspecfs.validate_input_node import validate_request_id
from fsspecc.cleanfs.cleanfs_toolkit import CleanFSToolkit
from fsspecc.imagefs.imagesfs_toolkit import ImagesFsToolkit
from fsspecc.memfs.memfs_toolkit import MemFSToolkit


def make_runtime(state: dict) -> ToolRuntime:
    return ToolRuntime(
        state=state,
        context=None,
        config={},
        stream_writer=lambda _: None,
        tool_call_id="tc_1",
        store=None,
    )


class TestFsspecc(unittest.TestCase):

    def test_get_file_path(self):
        assert get_file_path("req1", "a.txt") == "/req1/a.txt"
        assert get_file_path("req1", "a.txt", "sub") == "/req1/sub/a.txt"
        assert get_file_path("req1", "a.txt", ["s1", "s2"]) == "/req1/s1/s2/a.txt"

    def test_validate_request_id(self):
        assert validate_request_id("tool", {"request_id": "r1"}) == "r1"
        with self.assertRaises(KeyError):
            validate_request_id("tool", {})

    def test_fsbase_local_and_memory(self):
        with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
            fs1 = FSBase("file")
            fs2 = FSBase("file")

            file_a = f"{tmpdir1}/sub/a.txt"
            fs1.make_dirs(f"{tmpdir1}/sub", exist_ok=True)
            fs1.write(file_a, io.BytesIO(b"hello"), use_pipe=False)
            fs1.append(file_a, io.BytesIO(b" world"))

            out = io.BytesIO()
            fs1.read(file_a, out, use_pipe=False)
            assert out.getvalue() == b"hello world"

            # Pipe mode read/write
            file_b = f"{tmpdir1}/sub/b.txt"
            fs1.write(file_b, io.BytesIO(b"pipe_data"), use_pipe=True)
            out_b = io.BytesIO()
            fs1.read(file_b, out_b, use_pipe=True)
            assert out_b.getvalue() == b"pipe_data"

            # Index, walk, list
            fs1.index(tmpdir1)
            indexed = list(fs1.list_files())
            assert any(p.endswith("a.txt") for p in indexed)
            assert len(list(fs1.walk(tmpdir1))) >= 1
            assert len(list(fs1.walk_dirs(tmpdir1))) >= 1

            # load_or_store
            file_c = f"{tmpdir1}/sub/c.txt"
            data1, loaded1 = fs1.load_or_store(file_c, lambda: b"computed")
            data2, loaded2 = fs1.load_or_store(file_c, lambda: b"ignored")
            assert data1 == b"computed" and loaded1 is False
            assert data2 == b"computed" and loaded2 is True

            # transfer_to_drive & sync
            drive_target = f"{tmpdir2}/drive"
            fs1.transfer_to_drive(pattern=f"{tmpdir1}/sub/*.txt", target_directory=drive_target)
            assert (pathlib.Path(drive_target) / "a.txt").read_bytes() == b"hello world"

            sync_target = f"{tmpdir2}/sync"
            fs1.sync(fs2, f"{tmpdir1}/sub", sync_target)
            assert (pathlib.Path(sync_target) / "b.txt").read_bytes() == b"pipe_data"
            fs1.close()
            fs2.close()

    def test_atomic_write_fs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            afs = AtomicWriteFs("file")
            target = f"{tmpdir}/atomic.txt"
            afs.write(target, io.BytesIO(b"v1"), use_pipe=False)
            buf = io.BytesIO()
            afs.read(target, buf)
            assert buf.getvalue() == b"v1"

            afs.write(target, io.BytesIO(b"v2"), use_pipe=True)
            afs.read(target, buf)
            assert buf.getvalue() == b"v2"
            # Ensure no lingering .tmp files
            assert list(pathlib.Path(tmpdir).glob("*.tmp")) == []

    def test_memfs_and_toolkit(self):
        mfs = MemFS()
        mfs.store("req1", "k1", "val1")
        buf = io.BytesIO()
        mfs.load("req1", "k1", buf)
        assert buf.getvalue() == b"val1"

        tools = {t.name: t for t in MemFSToolkit(fs=mfs).get_tools()}
        rt = make_runtime({"request_id": "req2"})
        tools["store"].func(key="k2", value="val2", runtime=rt)
        val_bytes, _ = tools["load"].func(key="k2", runtime=rt)
        assert val_bytes == b"val2"

    def test_cleanfs_and_toolkit(self):
        cfs = CleanFs("memory")
        df_raw = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df_clean = pd.DataFrame({"a": [10, 20], "b": [30, 40]})

        cfs.save_raw_file("req1", df_raw, use_pipe=True)
        cfs.save_clean_file("req1", df_clean, use_pipe=True)

        read_raw = cfs.get_raw_file("req1", use_pipe=True)
        read_clean = cfs.get_clean_file("req1", use_pipe=True)
        assert read_raw.to_dict(orient="list") == {"a": [1, 2], "b": [3, 4]}
        assert read_clean.to_dict(orient="list") == {"a": [10, 20], "b": [30, 40]}
        assert len(list(cfs.list_raw_files("req1"))) == 1
        assert len(list(cfs.list_clean_files("req1"))) == 1

        tools = {t.name: t for t in CleanFSToolkit(fs=cfs).get_tools()}
        rt = make_runtime({"request_id": "req_tk"})
        tools["save_raw_file"].func(csv_data="x,y\n1,2\n", runtime=rt)
        tools["save_clean_file"].func(csv_data="x,y\n5,6\n", runtime=rt)
        raw_str, raw_art = tools["get_raw_file"].func(runtime=rt)
        clean_str, clean_art = tools["get_clean_file"].func(runtime=rt)
        assert raw_art["row_count"] == 1
        assert clean_art["row_count"] == 1
        _, raw_list = tools["list_raw_files"].func(runtime=rt)
        _, clean_list = tools["list_clean_files"].func(runtime=rt)
        assert len(raw_list) == 1 and len(clean_list) == 1

    def test_imagesfs_and_toolkit(self):
        ifs = ImagesFs("memory")
        fig, ax = plt.subplots()
        ax.plot([0, 1], [0, 1])
        ifs.save_png_file("req_img", "plot.png", fig, use_pipe=True)
        plt.close(fig)

        imgs = list(ifs.list_images("req_img"))
        assert len(imgs) == 1
        png_bytes = ifs.get_png_bytes("req_img", "plot.png")
        assert png_bytes.startswith(b"\x89PNG\r\n\x1a\n")

        tools = {t.name: t for t in ImagesFsToolkit(fs=ifs).get_tools()}
        rt = make_runtime({"request_id": "req_img2"})
        fig2, ax2 = plt.subplots()
        ax2.plot([1, 2], [3, 4])
        tools["save_png_file"].func(file_name="tk.png", runtime=rt)
        plt.close(fig2)
        listed, ok = tools["list_images"].func(runtime=rt)
        assert ok is True and len(listed) == 1

    def test_base_fstoolkit(self):
        fs = FSBase("memory")
        tools = {t.name: t for t in FSToolkit(fs=fs).get_tools()}
        rt = make_runtime({"request_id": "req_base"})
        tools["save_file"].func(file_path="doc.txt", file_data="hello_fs", runtime=rt)
        content, ok = tools["get_file"].func(file_path="doc.txt", runtime=rt)
        assert ok is True and content == b"hello_fs"
        files, _ = tools["list_files"].func(pattern="*.txt", runtime=rt)
        assert len(files) == 1


if __name__ == "__main__":
    unittest.main()
