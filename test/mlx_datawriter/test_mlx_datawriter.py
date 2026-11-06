import io
import unittest

from fsspecc import FSBase
from mlx_datawriter.data_writer import MLXDataWriter


class TestMLXDataWriter(unittest.TestCase):

    def test_mlx_data_writer_formats_and_flush(self):
        fs = FSBase("memory")
        writer = MLXDataWriter(fs=fs, base_path="/mlx_out", namespace="exp1", save_after=5)

        writer.add_rft('{"rft": 1}')
        writer.add_grpo('{"grpo": 1}')
        writer.add_dpo('{"dpo": 1}')

        assert writer.list_rft_lines() == ['{"rft": 1}']
        assert writer.list_grpo_lines() == ['{"grpo": 1}']
        assert writer.list_dpo_lines() == ['{"dpo": 1}']
        assert len(writer.list_lines()) == 3

        writer.write()
        assert writer.list_lines() == []

        for fmt in ["rft", "grpo", "dpo"]:
            gt_buf = io.BytesIO()
            fs.read(f"/mlx_out/exp1/{fmt}/gt.jsonl", gt_buf)
            assert gt_buf.getvalue().decode("utf-8").strip() == f'{{"{fmt}": 1}}'


if __name__ == "__main__":
    unittest.main()
