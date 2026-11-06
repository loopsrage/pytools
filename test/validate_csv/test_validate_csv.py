import io
import unittest
from fastapi import UploadFile
from starlette.datastructures import Headers

from fsspecc import CleanFs
from validate_csv.validate_csv import (
    read_contents_in_threadpool,
    validate_csv_header,
    validate_file_extension,
    validate_max_size,
    validate_structure,
)


class TestValidateCsv(unittest.IsolatedAsyncioTestCase):

    async def test_validate_csv_functions(self):
        csv_bytes = b"col_a,col_b\n10,20\n30,40\n50,60\n"
        validate_structure(csv_bytes)

        upload = UploadFile(
            file=io.BytesIO(csv_bytes),
            size=len(csv_bytes),
            filename="data.csv",
            headers=Headers({"content-type": "text/csv"}),
        )
        validate_file_extension(upload)
        validate_max_size(upload, 1024)
        with self.assertRaises(Exception):
            validate_max_size(upload, 5)

        header = await validate_csv_header(upload)
        assert header.startswith(b"col_a,col_b")

        cfs = CleanFs("memory")
        df = await read_contents_in_threadpool(cfs, upload, "req_csv_1")
        assert len(df) == 3
        stored_df = cfs.get_raw_file("req_csv_1", use_pipe=True)
        assert list(stored_df["col_a"]) == [10, 30, 50]

        bad_ext = UploadFile(file=io.BytesIO(b"x"), filename="data.txt")
        with self.assertRaises(Exception):
            validate_file_extension(bad_ext)


if __name__ == "__main__":
    unittest.main()
