import re

import pandas as pd
from pandas import DataFrame

from src.thread_safe.index import Index


class GTIndex:
    _index = None
    def __init__(self):
        self._namespace = "ground_truth"
        self._index = Index()

    def register_gt(self, query, truth):
        self._index.store_in_index("query_to_truth", query, truth)

    def load_gt(self, query):
        return self._index.load_from_index("query_to_truth", query)

class GroundTruthIndex:
    _index = None
    _namespace = None
    _path = None
    items: DataFrame = None

    def __init__(self, path):
        self._index = Index()
        self._namespace = "ground_truth"
        self._index.new(self._namespace)
        self._path = path
        self.load_hts_data()

    def item_to_hts(self, item_id):
        return self._index.load_from_index("item_to_gt", item_id)

    def item_to_formatted(self, item_id):
        return self._index.load_from_index("item_to_formatted", item_id)

    def register_gt(self, item_id, hts_code):
        formatted_code = f"{hts_code[:4]}.{hts_code[4:6]}.{hts_code[-4:]}"
        self._index.store_in_index("item_to_gt", str(item_id), hts_code)
        self._index.store_in_index("item_to_formatted", str(item_id), formatted_code)

    def load_hts_data(self):
        df = pd.read_csv(self._path)

        self.items = pd.to_numeric(df["Item ID"], errors='coerce').dropna().astype(int).tolist()

        for item_id, hts_code in df[["Item ID", "HTS US"]].itertuples(index=False):
            try:
                clean_id = int(float(item_id))
            except (ValueError, TypeError):
                continue

            hts_code = str(hts_code)
            if not re.match(r"\d", hts_code):
                continue
            self.register_gt(clean_id, hts_code)

        print(len(self.items))

