import unittest
from pytest_httpserver import HTTPServer

from jdelib.jdelib import AisSettings, JdeApi
from jdelib.schemas.schemas import (
    AppstackExecute,
    Auth,
    DataServiceRequest,
    FileDownload,
    ListMediaFiles,
    ReportDiscover,
    ReportExecute,
    ReportStatus,
)


class TestJdeLib(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._server = HTTPServer()
        self._server.start()
        self.addCleanup(self._server.stop)
        settings = AisSettings(server=f"http://{self._server.host}", port=self._server.port)
        self._api = JdeApi(settings)

    async def asyncTearDown(self):
        await self._api.close()

    async def test_jde_token_and_endpoints(self):
        self._server.expect_request("/jderest/v2/tokenrequest", method="POST").respond_with_json(
            {"userInfo": {"token": "tok_abc123"}}
        )
        await self._api.token_request(Auth(username="jde_user", password="pw"))
        assert self._api._token == "tok_abc123"
        assert self._api.headers() == {"Authorization": "Bearer tok_abc123"}

        endpoints = [
            "dataservice",
            "file/download",
            "file/list",
            "appstack/execute",
            "report/discover",
            "report/execute",
            "report/status",
        ]
        for ep in endpoints:
            self._server.expect_request(f"/jderest/v2/{ep}", method="POST").respond_with_json(
                {"endpoint": ep, "ok": True}
            )

        await self._api.dataservice(DataServiceRequest(target_name="F0101", target_type="table", data_selection=[]))
        await self._api.file_download(
            FileDownload(
                mo_structure="AB",
                mo_key=["1"],
                form_name="P01012",
                _version="ZJDE0001",
                sequence=1,
                download_url="/dl",
                device_name="dev",
                height=100,
                width=100,
            )
        )
        await self._api.list_mediafiles(
            ListMediaFiles(
                mo_structure="AB",
                mo_key=["1"],
                form_name="P01012",
                _version="ZJDE0001",
                media_object_types="FILE",
                extensions="pdf",
                include_urls=True,
                include_data=False,
                device_name="dev",
            )
        )
        await self._api.appstack_execute(AppstackExecute(action="open", form_request={}, device_name="dev"))
        await self._api.report_discover(
            ReportDiscover(report_name="R0010P", report_version="XJDE0001", environment="JDV920", role="*ALL")
        )
        await self._api.report_execute(
            ReportExecute(
                report_name="R0010P",
                device_name="dev",
                report_version="XJDE0001",
                queue_name="QBATCH",
                fire_and_forget=True,
                report_interconnects=[],
            )
        )
        await self._api.report_status(
            ReportStatus(
                job_number=101,
                execution_server="ENT1",
                environment="JDV920",
                view_type="PDF",
                device_name="dev",
            )
        )

        results = await self._api.json_results()
        # 1 token_request + 7 service calls = 8 responses
        assert len(results) == 8


if __name__ == "__main__":
    unittest.main()
