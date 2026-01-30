import asyncio
import threading
import uuid
from typing import Callable, Awaitable

from aiohttp import ClientSession, ClientResponse

from src.thread_safe.index import Index

type JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

class Api:

    _lock: threading.Lock
    _base_url: str
    _response_index: Index
    _client_args: dict
    _request_args: dict

    def __init__(self, base_url: str, default_client_args: dict = None, default_request_args: dict = None):
        self._base_url = base_url
        self._lock = threading.Lock()
        self._response_index = Index()
        self._client_args = default_client_args
        self._request_args = default_request_args

    @property
    def base_url(self) -> str:
        with self._lock:
            return self._base_url

    @property
    def client_args(self) -> dict:
        with self._lock:
            return (self._client_args or {}).copy()

    @property
    def request_args(self) -> dict:
        with self._lock:
            return (self._request_args or {}).copy()

    def endpoint(self, endpoint: str):
        return "/".join([self.base_url, endpoint])

    async def _client_session(self, request_handler: Callable[[ClientSession], Awaitable[None]], client_args: dict = None):
        session, _ = self._response_index.load_or_store_in_index("session", "session", ClientSession(**client_args))
        await request_handler(session)

    async def close(self):
        session: ClientSession = self._response_index.load_from_index("session", "session")
        await session.close()

    async def client_request(self, response_handler, client_args: dict = None, request_args: dict = None):
        request_args = {**(self._request_args or {}), **(request_args or {})}

        async def request_callback(session: ClientSession):
            if not request_args["url"]:
                request_args["url"] = self.base_url

            if not request_args["method"]:
                request_args["method"] = "GET"
            rd = await session.request(**(request_args or {}))
            await response_handler(rd)

        client_args = {**(self.client_args or {}), **(client_args or {})}
        await self._client_session(request_callback, client_args)

    async def gather_json_results(self):
        return await asyncio.gather(*[response.json() for _, response in self.list_responses()])

    async def gather_text_results(self):
        return await asyncio.gather(*[response.text() for _, response in self.list_responses()])

    async def json_results(self):
        return await self.gather_json_results()

    async def text_results(self):
        return await self.gather_text_results()

    def list_responses(self):
        yield from self._response_index.range_index("response")

    def list_requests(self):
        yield from self._response_index.range_index("requests")

    def read_response(self, request_id):
        return self._response_index.load_from_index("response", request_id)

    def read_request(self, request_id):
        return self._response_index.load_from_index("request", request_id)

    def _store_request(self, request_id, request_args):
        self._response_index.store_in_index(request_id, "request", request_args)

    def _store_response(self, request_id, response):
        self._response_index.store_in_index("response", request_id, response)

    def read_request_data(self, request_id):
        return {
            "response": self.read_response(request_id),
            "request": self.read_request(request_id)
        }

    def list_all_data(self):
        return {
            "responses": self.list_responses(),
            "requests": self.list_requests()
        }

    async def api_request(self, method: str, to: str, data: JSON | dict = None, request_args: dict = None, auth=None):
        request_id = uuid.uuid4().hex
        async def _handle_response(response: ClientResponse) -> None:
            self._store_response(request_id, response)

        args = {
            "url": self.endpoint(to),
            "json": data,
            "method": method,
            **(request_args or {})
        }

        await self.client_request(_handle_response, request_args=args)
        return request_id
