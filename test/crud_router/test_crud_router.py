import json
import pathlib
import tempfile
import unittest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String
from starlette.testclient import TestClient

from application_controller.app_controller import AppIndex
from crud_router.crud_router import create_crud_router, delete_by_id, select_by_id
from postgreslib.engine import Base, _engines, named_session
from settings.helper import restore


class CrudRecord(Base):
    __tablename__ = "crud_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    stage = Column(String, default="initial")

    @classmethod
    def vector(cls):
        return None


class CrudSchema(BaseModel):
    name: str = "sample"
    stage: str = "initial"


class TestCrudRouter(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = pathlib.Path(self._tmpdir.name) / "crud.db"
        restore(json.dumps({"CrudDB": {"url": f"sqlite:///{self.db_path}", "echo": False}}))
        with named_session("CrudDB") as sess:
            CrudRecord.__table__.create(_engines["CrudDB"])
            sess.add_all(
                [
                    CrudRecord(name="rec1", stage="initial"),
                    CrudRecord(name="rec2", stage="done"),
                ]
            )
            sess.commit()

        self.app = FastAPI()
        self.app.state.app_index = AppIndex()
        router = APIRouter()
        create_crud_router(router, "CrudDB", CrudRecord, CrudSchema, ["name"])
        self.app.include_router(router)

    def tearDown(self):
        if "CrudDB" in _engines:
            _engines["CrudDB"].dispose()
            del _engines["CrudDB"]
        self._tmpdir.cleanup()

    def test_crud_endpoints_and_helpers(self):
        with TestClient(self.app) as client:
            grid_resp = client.get("/grid")
            assert grid_resp.status_code == 200
            body = grid_resp.json()
            assert body["pagination"]["total"] == 2
            assert len(body["results"]) == 2

            count_resp = client.get("/count?column=stage")
            assert count_resp.status_code == 200
            counts = {r["name"]: r["count"] for r in count_resp.json()["results"]}
            assert counts == {"initial": 1, "done": 1}

            schema_resp = client.get("/create")
            assert schema_resp.status_code == 200

            get_resp = client.get("/get?id=1")
            assert get_resp.status_code == 200
            assert get_resp.json()["name"] == "rec1"

            del_resp = client.get("/delete?id=1")
            assert del_resp.status_code == 200
            assert del_resp.json() == {"success": True}

            del_missing = client.get("/delete?id=999")
            assert del_missing.json() == {"success": False}

        with named_session("CrudDB") as sess:
            assert select_by_id(sess, CrudRecord, 1) is None
            assert select_by_id(sess, CrudRecord, 2).name == "rec2"
            assert delete_by_id(sess, CrudRecord, 2) is True
            assert delete_by_id(sess, CrudRecord, 2) is False


if __name__ == "__main__":
    unittest.main()
