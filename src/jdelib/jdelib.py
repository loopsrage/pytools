import threading

from src.httpclient.api import Api
from src.jdelib.schemas.schemas import Auth, DataServiceRequest, FileDownload, ListMediaFiles, AppstackExecute, \
    ReportDiscover, ReportExecute, ReportStatus


class AisSettings:
    pass


class JdeApi(Api):
    _token: str = None
    _lock_token: threading.Lock
    def __init__(self, settings: AisSettings, default_client_args: dict = None, default_request_args: dict = None):
        base_url = f"https://{settings.server}:{settings.port}/jderest/v2/"
        super().__init__(base_url, default_client_args, default_request_args)
        self._base_url = base_url
        self._lock_token = threading.Lock()

    def headers(self, **kwargs):
        return {"Authorization": f"Bearer {self._token}", **kwargs}

    async def token_request(self, auth: Auth):
        with self._lock_token:
            await self.api_request(
                to="tokenrequest",
                method="POST",
                data=auth.model_dump_json())
            result = await self.last_response.json()
            self._token = result.get("userInfo", {}).get("token")

    async def dataservice(self, request: DataServiceRequest):
        await self.api_request(
            to="dataservice",
            method="POST",
            data=request.model_dump_json())

    async def file_download(self, request: FileDownload):
        await self.api_request(
            to="file/download",
            method="POST",
            data=request.model_dump_json())

    async def list_mediafiles(self, request: ListMediaFiles):
        await self.api_request(
            to="file/download",
            method="POST",
            data=request.model_dump_json())

    async def appstack_execute(self, request: AppstackExecute):
        await self.api_request(
            to="appstack/execute",
            method="POST",
            data=request.model_dump_json())

    async def report_discover(self, request: ReportDiscover):
        await self.api_request(
            to="report/discover",
            method="POST",
            data=request.model_dump_json())

    async def report_execute(self, request: ReportExecute):
        await self.api_request(
            to="report/execute",
            method="POST",
            data=request.model_dump_json())

    async def report_status(self, request: ReportStatus):
        await self.api_request(
            to="report/status",
            method="POST",
            data=request.model_dump_json())