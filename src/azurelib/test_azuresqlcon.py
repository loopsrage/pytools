import os
import unittest
from typing import List, Any, Union, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from azurelib.sql import AzureSqlConfig, AzureSql
from settings.helper import unmarshal_app_settings, restore
from thread_safe.index import Index

load_dotenv()
restore(os.getenv("ENV_FILE"))

class MyTestCase(unittest.TestCase):
    def test_something(self):
        config = unmarshal_app_settings("HTSDev", AzureSqlConfig)
        db = AzureSql(config)
        idx = Index()
        last_item = 0
        tables = ["windchill_items_staging", "windchill_item_attributes_staging"]
        batch_ids = []

        result = db.search(tables[0], "Item", {}, 100, last_item=last_item)
        for key, value in enumerate(result["Item"].values):
            att = {
                "description": result["Description"].values[key],
                "last_modified": result["lst_mod_dttm"].values[key]
            }
            batch_ids.append(value)
            idx.store_in_index(str(value), "attributes", att)

        att_result = db.search(tables[1], "Item", {
            "Item": batch_ids
        })
        for key, value in enumerate(att_result["Item"].values):
            att = {
                "AttributeName": att_result["AttributeName"].values[key],
                "AttributeValue": att_result["AttributeValue"].values[key],
                "AttributeLastModified": att_result["lst_mod_dttm"].values[key],
            }
            prev_att = idx.load_from_index(str(value), "attributes")
            att = {**prev_att, **att}
            idx.store_in_index(str(value), "attributes", att)

        for x in idx.list_indexes():
            for v in idx.range_index(x):
                print(x, v)



class MockSchema(BaseModel):
    parent: Optional["MockSchema"] = None
    children: List["MockSchema"] = Field(default_factory=list)
    value: dict[str, Any] | None = None
    model_config = {"arbitrary_types_allowed": True}

def generate_test_schema(depth=10):
    root: MockSchema | None = None
    last: MockSchema | None = None
    for i in range(0, depth):
        current = MockSchema(parent=last,
                          value={"Some_key": "Some_value"})

        if last is not None:
            current.children.append(MockSchema(
                parent=last,
                value={"some_child_value": "some_child"}))

        last = current
        if root is None:
            root = current

    output = DocumentIndexConverter(root)
    return output.to_search_documents()


class TestDocConverter(unittest.TestCase):

    def test_doc_index_converter(self):
        schema = generate_test_schema(10)
        print(schema.values())
