from typing import Any, Literal

from pydantic import BaseModel

class Auth(BaseModel):
    username: str
    password: str

class DataServiceRequest(BaseModel):
    target_name: str = None
    target_type: str = None
    data_selection: list[dict[str, Any]]

class ReportDiscover(BaseModel):
    report_name: str
    report_version: str
    environment: str
    role: str

class ReportStatus(BaseModel):
    job_number: int
    execution_server: str
    environment: str
    view_type: str
    device_name: str

class ListMediaFiles(BaseModel):
    mo_structure: str
    mo_key: list[str]
    form_name: str
    _version: str
    media_object_types: Literal["TEXT", "FILE", "URL"]
    extensions: Literal["pdf", "jpg"]
    include_urls: bool
    include_data: bool
    device_name: str

class FileDownload(BaseModel):
    mo_structure: str
    mo_key: list[str]
    form_name: str
    _version: str
    sequence: int
    download_url: str
    device_name: str
    height: int
    width: int

class ReportExecute(BaseModel):
    report_name: str
    device_name: str
    report_version: str
    queue_name: str
    fire_and_forget: bool
    report_interconnects: list[dict[str, Any]]

class AppstackExecute(BaseModel):
    action: str
    form_request: dict[str, Any]
    device_name: str
