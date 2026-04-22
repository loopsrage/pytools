
import io

import random
import threading

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from thread_safe.index import Index
from thread_safe.tslist import TsList


class MLXDataWriter:

    _index = None
    _fs: FSBase = None
    _namespace = None

    _write_lock: threading.Lock = None

    def __init__(self, fs: FSBase, base_path, namespace=None, save_after=10, backup=True):
        self.base_path = base_path
        self.backup = backup
        self.formats = ["grpo", "rft", "dpo"]
        self._write_lock = threading.Lock()

        self._namespace = namespace
        if self._namespace is None:
            self._namespace = "mlx"

        self._save_after = save_after
        self._fs = fs
        self._index = Index()
        for f in self.formats:
            self._index.new(f)

    def add_rft(self,  value: str) -> int:
        fmt = "rft"
        rft_lines, _ = self._index.load_or_store_in_index(fmt, "", TsList())
        count = rft_lines.add(value)
        if count > self._save_after:
            self.write_rft()
        return count

    def add_grpo(self,  value: str):
        fmt = "grpo"
        grpo_lines, _ = self._index.load_or_store_in_index(fmt, "", TsList())
        count = grpo_lines.add(value)
        if count > self._save_after:
            self.write_grpo()
        return count

    def add_dpo(self,  value: str):
        fmt = "dpo"
        dpo_lines, _ = self._index.load_or_store_in_index(fmt, "", TsList())
        count = dpo_lines.add(value)
        if count > self._save_after:
            self.write_dpo()
        return count

    def write_rft(self, ratio = 0.1):
        fmt = "rft"
        self.write_train_valid_gt(fmt, ratio)
        self._index.store_in_index(fmt, "", TsList())

    def write_dpo(self, ratio = 0.1):
        fmt = "dpo"
        self.write_train_valid_gt(fmt, ratio)
        self._index.store_in_index(fmt, "", TsList())

    def write_grpo(self, ratio = 0.1):
        fmt = "grpo"
        self.write_train_valid_gt(fmt, ratio)
        self._index.store_in_index(fmt, "", TsList())


    def list_rft_lines(self):
        fmt = "rft"
        lines: TsList = self._index.load_from_index(fmt, "")
        return lines.to_list() if lines is not None else []

    def list_dpo_lines(self):
        fmt = "dpo"
        lines: TsList = self._index.load_from_index(fmt, "")
        return lines.to_list() if lines is not None else []

    def list_grpo_lines(self):
        fmt = "grpo"
        lines: TsList = self._index.load_from_index(fmt, "")
        return lines.to_list() if lines is not None else []

    def list_lines(self):
        lines = self.list_grpo_lines()+self.list_dpo_lines()+self.list_rft_lines()
        return lines

    def _file_path(self, fmt):
        return f"{self.base_path}/{self._namespace}/{fmt}"

    def write_train_valid_gt(self, fmt, ratio = 0.1):
        valid_file = io.BytesIO()
        train_file = io.BytesIO()
        ground_truth = io.BytesIO()

        for _, line in self._index.range_index(fmt):
            for x in line.all():
                ln = (x+"\n").encode("utf-8")
                if random.random() < ratio:
                    valid_file.write(ln)
                else:
                    train_file.write(ln)
                ground_truth.write(ln)

        base = self._file_path(fmt)
        self._fs.make_dirs(base)

        self._fs.append(f"{base}/train.jsonl", train_file)
        self._fs.append(f"{base}/valid.jsonl", valid_file)
        self._fs.append(f"{base}/gt.jsonl", ground_truth)
        self._index.store_in_index(fmt, "", TsList())

    def write(self):
        with self._write_lock:
            for f in self.formats:
                self.write_train_valid_gt( f)