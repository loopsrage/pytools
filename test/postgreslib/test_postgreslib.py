import json
import pathlib
import tempfile
import unittest
from sqlalchemy import Column, Index, Integer, String, Text

from postgreslib.count_column import count_column
from postgreslib.datagrid_adapter import (
    is_any_of,
    mui_datagrid_select_many,
    to_binary,
    to_csv_file,
    to_data_viewer,
    to_field_value_table,
)
from postgreslib.engine import Base, _engines, named_session
from postgreslib.util import (
    extract_model_kwargs,
    get_all_unique_columns,
    get_fts_expression,
    get_pk_names,
    get_searchable_columns,
    to_dict,
    to_dict_jsonb,
)
from settings.helper import restore


class SampleItem(Base):
    __tablename__ = "sample_items"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    category = Column(String)
    description = Column(Text)

    __table_args__ = (Index("idx_sample_fts", "description"),)

    @classmethod
    def vector(cls):
        return None


class TestPostgresLib(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = pathlib.Path(self._tmpdir.name) / "test.db"
        restore(json.dumps({"TestDB": {"url": f"sqlite:///{self.db_path}", "echo": False}}))
        with named_session("TestDB") as sess:
            SampleItem.__table__.create(_engines["TestDB"])
            sess.add_all(
                [
                    SampleItem(code="A1", category="alpha", description="first item"),
                    SampleItem(code="A2", category="alpha", description="second item"),
                    SampleItem(code="B1", category="beta", description="third item"),
                ]
            )
            sess.commit()

    def tearDown(self):
        if "TestDB" in _engines:
            _engines["TestDB"].dispose()
            del _engines["TestDB"]
        self._tmpdir.cleanup()

    def test_util_functions(self):
        assert get_pk_names(SampleItem) == ["id"]
        uniq = get_all_unique_columns(SampleItem)
        assert "id" in uniq and "code" in uniq
        kw = extract_model_kwargs(SampleItem, code="C1", category="gamma", extra_ignored=123)
        assert kw == {"code": "C1", "category": "gamma"}
        searchable = [c.name for c in get_searchable_columns(SampleItem)]
        assert set(searchable) == {"code", "category", "description"}
        assert get_fts_expression(SampleItem, "sample") is not None

        item = SampleItem(id=1, code="A1", category="alpha", description="desc")
        d = to_dict(item)
        dj = to_dict_jsonb(item)
        assert d["code"] == "A1" and dj["code"] == "A1"

    def test_datagrid_helpers_and_queries(self):
        f_any = is_any_of("id", [1, 2])
        assert f_any["items"][0]["operator"] == "isAnyOf"
        assert to_binary("```json\n{\"a\": 1}\n```")["Data"] == '{"a": 1}'
        assert to_csv_file("a,b")["Subtype"] == 20
        assert to_field_value_table("k:v")["Subtype"] == 19
        assert to_data_viewer("txt", read_only=True)["Subtype"] == 0
        assert to_data_viewer("txt", read_only=False)["Subtype"] == 16

        with named_session("TestDB") as sess:
            counts = count_column(sess, SampleItem, "category")
            assert dict(counts) == {"alpha": 2, "beta": 1}

            res_all = mui_datagrid_select_many(sess, SampleItem)
            assert res_all["total"] == 3 and len(res_all["rows"]) == 3

            res_eq = mui_datagrid_select_many(
                sess,
                SampleItem,
                filter_model={
                    "items": [{"field": "category", "operator": "equals", "value": "alpha"}],
                    "logicOperator": "and",
                },
                sort_model=[{"field": "code", "sort": "desc"}],
            )
            assert res_eq["total"] == 2
            assert [r.code for r in res_eq["rows"]] == ["A2", "A1"]

            res_contains = mui_datagrid_select_many(
                sess,
                SampleItem,
                filter_model={
                    "items": [{"field": "description", "operator": "contains", "value": "third"}],
                    "logicOperator": "and",
                },
            )
            assert res_contains["total"] == 1 and res_contains["rows"][0].code == "B1"


if __name__ == "__main__":
    unittest.main()
