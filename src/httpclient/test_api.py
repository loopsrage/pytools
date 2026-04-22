import asyncio
import unittest
from typing import Union

from pydantic import BaseModel
from pytest_httpserver import HTTPServer
from httpclient.api import Api

DEFAULT_REQUEST = {}
DEFAULT_CLIENT = {}


class ExampleSchema(BaseModel):
    pass


class ExampleApi(Api):
    async def create(self, schema: Union[ExampleSchema, list[ExampleSchema]]):
        await self.api_request(
            to="create",
            method="POST",
            data=[i.model_dump_json(exclude_none=True) for i in [*schema]])

    async def update(self, schema: ExampleSchema):
        await self.api_request(
            to="update",
            method="POST",
            data=schema.model_dump_json(exclude_none=True))

    async def delete(self, ids: list[str]):
        await self.api_request(
            to="delete",
            method="POST",
            data={"document_ids": ids})

    async def get(self, ids: list[str]):
        await self.api_request(
            to="get",
            method="GET",
            data={"document_ids": ids})


class TestExampleApi(unittest.IsolatedAsyncioTestCase):
    _api: ExampleApi
    _server: HTTPServer
    _mock_host: str
    async def asyncSetUp(self) -> None:
        try:
            self._server = HTTPServer()
            self._server.start()
            self.addCleanup(self._server.stop)
            self._mock_host = f"http://{self._server.host}:{self._server.port}"
            self._api = ExampleApi(base_url=self._mock_host)
        except Exception as e:
            raise e

    async def asyncTearDown(self) -> None:
        try:
           await self._api.close()
        except Exception as e:
            raise e

    async def test_something(self):
        try:
            self._server.expect_request("/create").respond_with_json({"message": "Mocked Data"})
            requests = [self._api.create([ExampleSchema(Name=str(j)+str(i)) for j in range(100)]) for i in range(100)]
            await asyncio.gather(*requests)
            results = await self._api.json_results()
            for j in results:
                print(j)
        except Exception as e:
            raise e

