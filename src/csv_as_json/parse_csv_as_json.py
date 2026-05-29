import json
from json import JSONDecodeError
from typing import Generator

from pandas import DataFrame


def generate_json(df: DataFrame, promoted_columns: list[str] = None, json_columns: list[str] = None) -> Generator[dict]:

    if not json_columns:
        json_columns = []

    if not promoted_columns:
        promoted_columns = []

    other_cols = [col for col in df.columns if col not in json_columns]

    targets_list = df[json_columns].to_dict(orient="records") if json_columns else [{} for _ in range(len(df))]
    attributes_list = df[other_cols].to_dict(orient="records")

    for target_row, attr_row in zip(targets_list, attributes_list):

        payload = {}
        for p in promoted_columns:
            payload[p] = attr_row.pop(p)

        payload["attributes"] = attr_row

        for c in json_columns:
            tr = target_row.get(c)
            try:
                if tr:
                    json_att = json.loads(tr)
                    payload[c] = json_att
            except JSONDecodeError:
                continue
        yield payload