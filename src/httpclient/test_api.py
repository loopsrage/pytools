import asyncio
import unittest
from typing import Union

from pydantic import BaseModel
from pytest_httpserver import HTTPServer
from httpclient.api import Api


class ExampleSchema(BaseModel):
    Name: str


class ExampleApi(Api):
    async def create(self, schema: Union[ExampleSchema, list[ExampleSchema]]):
        return await self.api_request(
            to="create",
            method="POST",
            data=[i.model_dump(exclude_none=True) for i in [*schema]],
        )

    async def update(self, schema: ExampleSchema):
        return await self.api_request(
            to="update",
            method="POST",
            data=schema.model_dump(exclude_none=True),
        )

    async def delete(self, ids: list[str]):
        return await self.api_request(
            to="delete",
            method="POST",
            data={"document_ids": ids},
        )

    async def get(self, ids: list[str]):
        return await self.api_request(
            to="get",
            method="GET",
            data={"document_ids": ids},
        )


class TestExampleApi(unittest.IsolatedAsyncioTestCase):
    _api: ExampleApi
    _server: HTTPServer
    _mock_host: str

    async def asyncSetUp(self) -> None:
        self._server = HTTPServer()
        self._server.start()
        self.addCleanup(self._server.stop)
        self._mock_host = f"http://{self._server.host}:{self._server.port}"
        self._api = ExampleApi(base_url=self._mock_host)

    async def asyncTearDown(self) -> None:
        await self._api.close()

    async def test_api_crud_and_tracking(self):
        self._server.expect_request("/create").respond_with_json({"status": "created"})
        self._server.expect_request("/update").respond_with_json({"status": "updated"})
        self._server.expect_request("/delete").respond_with_json({"status": "deleted"})
        self._server.expect_request("/get").respond_with_json({"status": "fetched"})

        req_ids = await asyncio.gather(
            self._api.create([ExampleSchema(Name="item1"), ExampleSchema(Name="item2")]),
            self._api.update(ExampleSchema(Name="item1_upd")),
            self._api.delete(["id1"]),
            self._api.get(["id1"]),
        )
        assert len(req_ids) == 4
        results = await self._api.json_results()
        statuses = {r["status"] for r in results}
        assert statuses == {"created", "updated", "deleted", "fetched"}

        req_data = self._api.read_request_data(req_ids[0])
        assert req_data["request"]["method"] == "POST"
        assert req_data["response"].status == 200
        all_data = self._api.list_all_data()
        assert len(list(all_data["requests"])) == 4
        assert len(list(all_data["responses"])) == 4


if __name__ == "__main__":
    unittest.main()
