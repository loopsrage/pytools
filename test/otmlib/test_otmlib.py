import unittest
from pytest_httpserver import HTTPServer

from otmlib.otmlib import OtmApi, OtmLog, OtmSettings, ShipmentsRequest, headers, resources_v2


class TestOtmLib(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._server = HTTPServer()
        self._server.start()
        self.addCleanup(self._server.stop)
        host_url = f"http://{self._server.host}:{self._server.port}"
        self._settings = OtmSettings(user="otm_user", password="secret", server=host_url)
        self._api = OtmLog(self._settings)

    async def asyncTearDown(self):
        await self._api.close()

    def test_resources_v2_and_headers(self):
        assert resources_v2("shipments") == "/logisticsRestApi/resources/v2/shipments"
        assert resources_v2("shipments", describe=True) == "/logisticsRestApi/resources/v2/shipments/describe"
        assert headers(Accept="*/*") == {"Content-Type": "application/json", "Accept": "*/*"}
        fn = self._api.build_url("orderReleases", describe=True)
        assert fn() == "/logisticsRestApi/resources/v2/orderReleases/describe"

    async def test_otm_endpoints(self):
        endpoints = [
            "shipments",
            "orderMovements",
            "orderReleases",
            "itineraries",
            "rateOfferings",
            "rateRecords",
            "servprovs",
            "tradeTransactions",
            "exportRequests",
            "exportStatuses",
            "regions",
        ]
        for ep in endpoints:
            self._server.expect_request(f"/logisticsRestApi/resources/v2/{ep}", method="POST").respond_with_json(
                {"resource": ep, "ok": True}
            )

        req = ShipmentsRequest(limit=10, filter="id eq 1", offset=0, total_results=True)
        await self._api.shipments(req)
        await self._api.order_movements(req)
        await self._api.order_releases(req)
        await self._api.itineraries(req)
        await self._api.rate_offerings(req)
        await self._api.rate_records(req)
        await self._api.servprovs(req)
        await self._api.trade_transactions(req)
        await self._api.export_requests(req)
        await self._api.export_statuses(req)
        await self._api.regions(req)

        results = await self._api.json_results()
        assert len(results) == 11
        assert {r["resource"] for r in results} == set(endpoints)


if __name__ == "__main__":
    unittest.main()
