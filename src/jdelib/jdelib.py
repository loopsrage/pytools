import asyncio

from httpclient.api import Api
from jdelib.schemas.schemas import Auth, DataServiceRequest, FileDownload, ListMediaFiles, AppstackExecute, \
    ReportDiscover, ReportExecute, ReportStatus


class AisSettings:
    def __init__(self, server: str = "", port: int = 443):
        self.server = server
        self.port = port


class JdeApi(Api):
    _token: str = None
    _lock_token: asyncio.Lock
    def __init__(self, settings: AisSettings, default_client_args: dict = None, default_request_args: dict = None):
        if settings.server.startswith(("http://", "https://")):
            base_url = f"{settings.server.rstrip('/')}:{settings.port}/jderest/v2"
        else:
            base_url = f"https://{settings.server}:{settings.port}/jderest/v2"
        super().__init__(base_url, default_client_args, default_request_args)
        self._base_url = base_url
        self._lock_token = asyncio.Lock()

    def headers(self, **kwargs):
        return {"Authorization": f"Bearer {self._token}", **kwargs}

    async def token_request(self, auth: Auth):
        async with self._lock_token:
            req_id = await self.api_request(
                to="tokenrequest",
                method="POST",
                data=auth.model_dump(exclude_none=True))
            resp = self.read_response(req_id)
            result = await resp.json()
            self._token = result.get("userInfo", {}).get("token")

    async def dataservice(self, request: DataServiceRequest):
        return await self.api_request(
            to="dataservice",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def file_download(self, request: FileDownload):
        return await self.api_request(
            to="file/download",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def list_mediafiles(self, request: ListMediaFiles):
        return await self.api_request(
            to="file/list",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def appstack_execute(self, request: AppstackExecute):
        return await self.api_request(
            to="appstack/execute",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def report_discover(self, request: ReportDiscover):
        return await self.api_request(
            to="report/discover",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def report_execute(self, request: ReportExecute):
        return await self.api_request(
            to="report/execute",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})

    async def report_status(self, request: ReportStatus):
        return await self.api_request(
            to="report/status",
            method="POST",
            data=request.model_dump(exclude_none=True),
            request_args={"headers": self.headers()})