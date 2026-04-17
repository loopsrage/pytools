from functools import partial

from pydantic import BaseModel

from lib.apis.shared_schemas.shared_schemas import OtmSettings
from lib.httpclient.api import Api

def headers(**kwargs):
    return {"Content-Type": f"application/json", **kwargs}

# OTM Core Resources (Transportation)
# f"/logisticsRestApi/resources/v2/shipments"
# f"/logisticsRestApi/resources/v2/orderReleases"
# f"/logisticsRestApi/resources/v2/locations"
# f"/logisticsRestApi/resources/v2/vouchers"
# f"/logisticsRestApi/resources/v2/trackingEvents"
# f"/logisticsRestApi/resources/v2/tradeTransactions"
# f"/logisticsRestApi/resources/v2/declarations"
# f"/logisticsRestApi/resources/v2/licenses"
# f"/logisticsRestApi/resources/v2/partyScreens"

# Supply Chain & Financials (FSCM) Resources
# f"/fscmRestApi/resources/11.13.18.05/purchaseOrders"
# f"/fscmRestApi/resources/11.13.18.05/payablesInvoices"
# f"/fscmRestApi/resources/11.13.18.05/inventoryOrganizations"
# f"/fscmRestApi/resources/11.13.18.05/salesOrdersForOrderHub"
# f"/fscmRestApi/resources/11.13.18.05/suppliers"
# f"/fscmRestApi/resources/11.13.18.05/receivablesInvoices"

# Human Capital Management (HCM) Resources
# f"/hcmRestApi/resources/11.13.18.05/emps"
# f"/hcmRestApi/resources/11.13.18.05/workers"
# f"/hcmRestApi/resources/11.13.18.05/jobs"
# f"/hcmRestApi/resources/11.13.18.05/positions"
# f"/hcmRestApi/resources/11.13.18.05/departments"
# f"/hcmRestApi/resources/11.13.18.05/payrollDefinitions"

# Customer Relationship Management (CRM) Resources
# f"/crmRestApi/resources/11.13.18.05/accounts"
# f"/crmRestApi/resources/11.13.18.05/opportunities"
# f"/crmRestApi/resources/11.13.18.05/leads"
# f"/crmRestApi/resources/11.13.18.05/serviceRequests"
# f"/crmRestApi/resources/11.13.18.05/contacts"
# f"/crmRestApi/resources/11.13.18.05/activities"

# Common Foundation (FND) Resources
# f"/fndRestApi/resources/11.13.18.05/attachments"
# f"/fndRestApi/resources/11.13.18.05/lookups"
# f"/fndRestApi/resources/11.13.18.05/managedAttachments"

# Quick Python Discovery Tip
# If you want to see the exact list of resources available on your specific server right now, you can use the Oracle Metadata Catalog:
# f"/logisticsRestApi/resources/v2/metadata-catalog/{resource}"
# "{resource}/describe"


def resources_v2(prefix: str, resource: str = None, version: str = None, describe: bool = None):
    if prefix is None:
        prefix = "logisticsRestApi"

    if describe is None:
        describe = False

    if version is None:
        version = "v2"

    return f"/{prefix}/resources/{version}/{resource}{'/describe' if describe else ''}"

class OtmApi(Api):
    _token: str = None
    _base_url: str = None

    def __init__(self, settings: OtmSettings, default_client_args: dict = None, default_request_args: dict = None):
        self._auth = OtmAuth(user=settings.user, password=settings.password)
        base_url = f"https://{settings.server}.oraclecloud.com"
        super().__init__(base_url, default_client_args, default_request_args)
        self._base_url = base_url

class OtmLog(OtmApi):

    @property
    def version(self):
        return "v2"

    @property
    def prefix(self):
        return "logisticsRestApi"

    def build_url(self, to: str, describe: bool = False):
        return partial(
            resources_v2,
            prefix=self.prefix,
            version=self.version,
            resource=to,
            describe=describe
        )

#     # f"/logisticsRestApi/resources/v2/shipments"
# # f"/logisticsRestApi/resources/v2/orderReleases"
# # f"/logisticsRestApi/resources/v2/locations"
# # f"/logisticsRestApi/resources/v2/vouchers"
# # f"/logisticsRestApi/resources/v2/trackingEvents"
# # f"/logisticsRestApi/resources/v2/tradeTransactions"
# # f"/logisticsRestApi/resources/v2/declarations"
# # f"/logisticsRestApi/resources/v2/licenses"
# # f"/logisticsRestApi/resources/v2/partyScreens"


class ShipmentsRequest(BaseModel):
    limit: int
    filter: str
    offset: int
    total_results: bool | str

class OtmAuth(BaseModel):
    user: str
    password: str

class OtmApi(Api):
    _token: str = None

    def __init__(self, settings: OtmSettings, default_client_args: dict = None, default_request_args: dict = None):
        self._auth = OtmAuth(user=settings.user, password=settings.password)
        base_url = f"https://{settings.server}.oraclecloud.com"
        super().__init__(base_url, default_client_args, default_request_args)
        self._base_url = base_url

    async def shipments(self, request: ShipmentsRequest):
        await self.api_request(
            to=resources_v2("shipments"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def order_movements(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("orderMovements"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def order_releases(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("orderReleases"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def itineraries(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("itineraries"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def rate_offerings(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("rateOfferings"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def rate_records(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("rateRecords"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def servprovs(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("servprovs"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def trade_transactions(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("tradeTransactions"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def export_requests(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("tradeTransactions"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def export_statuses(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("tradeTransactions"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)

    async def regions(self, request: BaseModel):
        await self.api_request(
            to=resources_v2("regions"),
            method="POST",
            data=request.model_dump_json(), auth=self._auth)