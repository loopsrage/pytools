import unittest
import pandas as pd

from csv_as_json.parse_csv_as_json import generate_json


class TestCsvAsJson(unittest.TestCase):

    def test_generate_json(self):
        df = pd.DataFrame(
            [
                {"id": 1, "name": "item1", "meta": '{"color": "red"}', "bad_json": "{broken"},
                {"id": 2, "name": "item2", "meta": '{"color": "blue"}', "bad_json": ""},
            ]
        )
        rows = list(generate_json(df, promoted_columns=["id"], json_columns=["meta", "bad_json"]))
        assert len(rows) == 2
        assert rows[0] == {
            "id": 1,
            "attributes": {"name": "item1"},
            "meta": {"color": "red"},
        }
        assert rows[1] == {
            "id": 2,
            "attributes": {"name": "item2"},
            "meta": {"color": "blue"},
        }


if __name__ == "__main__":
    unittest.main()
