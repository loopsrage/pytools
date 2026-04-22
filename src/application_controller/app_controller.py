import logging
import os
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from typing import Literal

import uvicorn
from aiohttp.web_request import Request
from fastapi import FastAPI
from pydantic_settings import BaseSettings
from starlette.datastructures import MutableHeaders
from starlette.staticfiles import StaticFiles

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from indexes.api_index.api_index import ApiIndex
from indexes.connection_index.connection_index import ConnectionIndex
from indexes.fsindex.fsindex import FilesystemIndex
from indexes.specialist_index.specialist_index import SpecialistIndex
from settings.helper import unmarshal_app_settings, setting


class LogConfig(BaseSettings):
    filename: str | os.PathLike[str] | None = None
    filemode: str | None = None
    format: str | None = None
    datefmt: str | None = None
    style: Literal["%", "{", "$"] = "$"
    level: int | str | None = None
    force: bool | None = False
    encoding: str | None = None
    errors: str | None = None

class UvicornSettings(BaseSettings):
    host: str = None
    port: int = None
    ssl_keyfile: str = None
    ssl_certfile: str = None
    timeout_keep_alive: int = None
    log_level: int = None
    access_log: bool = None
    forwarded_allow_ips: str = None
    limit_concurrency: int = None
    workers: int = None
    proxy_headers: str = None
    loop: str = None

class AppIndex(FilesystemIndex, ConnectionIndex, ApiIndex, SpecialistIndex):
    pass

class App:
    _settings: UvicornSettings = None
    _fast_api = None
    _logger = None
    _controllers = None
    _app_index = None

    def __init__(self, uvcorn_settings: UvicornSettings):
        self._closers = None
        self._settings = uvcorn_settings

    @property
    def app_index(self):
        return self._app_index

    def async_lifespan(self):

        @asynccontextmanager
        async def lifespan(api: FastAPI):
            # --- STARTUP LOGIC ---
            self._app_index = AppIndex()
            cfg = unmarshal_app_settings("Logging", LogConfig)
            logging.basicConfig(**cfg.model_dump())
            self._logger = logging.getLogger("default_logger")
            yield  # --- The app is now running and handling requests ---

            for c in self._closers:
                if hasattr(c, "close"):
                    c.close()
                self._logger.info(f"Shutdown complete.")

        self._fast_api = FastAPI(lifespan=lifespan)
        self._fast_api.state.app_index = self._app_index

        @self._fast_api.middleware("http")
        async def add_process_time_header(request: Request, call_next):
            start_time = time.perf_counter()

            headers = MutableHeaders(scope=request.scope)
            request_id = uuid.uuid4().hex
            headers.append("X-Request-Id", request_id)

            response = await call_next(request)
            process_time = time.perf_counter() - start_time

            response.headers["X-Process-Time"] = f"{process_time:.4f}s"
            response.headers["X-Request-Id"] = str(request_id)

            self._logger.info(
                f"Method: {request.method}, RequestId: {request_id}, Path: {request.url.path} Time: {process_time:.4f}s")

            return response

    def register_router(self, router):
        self._fast_api.include_router(router)

    def serve_with_static_files(self):
        self._fast_api.state.storage = FSBase(filesystem="memory")
        static_dir = setting("FastApi", "static_files")
        try:

            if os.path.exists(static_dir):
                self._fast_api.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
            else:
                self._logger.warning(f"Static directory '{static_dir}' not found. Skipping mount.")
            uvicorn.run(self._fast_api, **self._settings.model_dump())
        except Exception as e:
            traceback.print_exception(e)
            self._logger.error(str(e))
