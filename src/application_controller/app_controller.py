import asyncio
import io
import logging
import os
import time
import traceback
import uuid
from asyncio import TaskGroup
from contextlib import asynccontextmanager
from typing import Literal, List

import uvicorn
from aiohttp.web_request import Request
from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.routing import APIRoute
from pydantic_settings import BaseSettings
from starlette.datastructures import MutableHeaders
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from azurelib.blob import AzureIndexAgentSearchConfig, AzureBlob
from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from index_queue.index_queue import IndexQueue, ActionConfig
from indexes.api_index.api_index import ApiIndex
from indexes.app_ctrl_index.appctrl import ApplicationIndex
from indexes.connection_index.connection_index import ConnectionIndex
from indexes.fsindex.fsindex import FilesystemIndex
from indexes.specialist_index.specialist_index import SpecialistIndex
from indexes.worker_service_index.worker_index import WorkerServiceIndex
from queue_controller.queueController import QueueController
from service_controller.service_controller import ServiceController
from settings.helper import unmarshal_app_settings, setting, restore
from thread_safe.controller.controller import Controller
from thread_safe.index import Index
from thread_safe.onceler import Onceler


class ThreadSafeTG(TaskGroup):
    def __init__(self, tg, loop):
        self._tg = tg
        self._loop = loop

    def create_task(self, coro):
        return self._loop.call_soon_threadsafe(self._tg.create_task, coro)


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
    host: str | None = None
    port: int | None= None
    ssl_keyfile: str| None = None
    ssl_certfile: str | None= None
    reload: bool | None = False
    timeout_keep_alive: int | None = 5
    log_level: int | None = 1
    access_log: bool | None= None
    forwarded_allow_ips: str| None = None
    limit_concurrency: int | None= None
    workers: int | None= 1
    proxy_headers: str| None = None
    loop: str | None= "uvloop"

class AppIndex(ApplicationIndex, WorkerServiceIndex, FilesystemIndex, ConnectionIndex, ApiIndex, SpecialistIndex):
    pass

class AppBase(ServiceController):
    _app_index = AppIndex = None
    _logger = None

    @property
    def app_index(self):
        if not self._app_index:
            self._app_index = AppIndex()

        return self._app_index

def load_azure_app_settings(remote_path: str, azure_settings_key=None, ):
    if azure_settings_key is None:
        azure_settings_key = "Azure"

    load_dotenv()
    restore(os.getenv("ENV_FILE"))
    if os.path.exists("/proc/1/cgroup") or os.path.exists("/.dockerenv"):
        azure_config = unmarshal_app_settings(azure_settings_key, AzureIndexAgentSearchConfig)
        azure_config.Storage.ssl = False
        azure_config.Storage.connection_verify = False

        azure_fs = AzureBlob(storage_options=azure_config.Storage)

        buff = io.BytesIO()
        azure_fs.read(remote_path, buff, use_pipe=False)
        restore(buff.getvalue())


def logger_from_settings(remote_path: str):
    load_azure_app_settings(remote_path)
    cfg = unmarshal_app_settings("Logging", LogConfig)
    logging.basicConfig(**cfg.model_dump())
    logger = logging.getLogger("default_logger")
    return logger


_APP_REGISTRY = ApplicationIndex()

def app_registry() -> ApplicationIndex:
    return _APP_REGISTRY

async def run_app(app_name: str, app, settings_path: str, logger=None, register=True):
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    if logger is None:
        logger = logger_from_settings(settings_path)

    if register:
        app_registry().register_applications({app_name: app})

    try:
        async with TaskGroup() as tg:
            tg_proxy = ThreadSafeTG(tg, loop)

            async def wrap_init():
                try:
                    app.init(stop_event, tg_proxy, logger)
                except Exception as e:
                    traceback.print_exception(e)
                    raise

            tg.create_task(wrap_init())
    except* (Exception, SystemExit) as eg:
        stop_event.set()
        raise eg
    finally:
        await app.close()

class AlembicApp(AppBase):
    config: Config
    def __init__(self, path, script_location: str, sqlalchemy_url: str):
        self.config = Config(path)
        self.config.set_main_option("script_location", script_location)
        self.config.set_main_option("sqlalchemy.url", sqlalchemy_url)
        command.upgrade(self.config, "head")

class WorkerApp(AppBase):
    def init(self, stop_event, tg, logger=None):
        super().init(stop_event, tg, logger)
        self.app_index.start_workers(stop_event, tg)

class SimpleApp(AppBase):
    action_queues: IndexQueue = None
    actions: dict[str, ActionConfig]

    def __init__(self, actions: dict[str, ActionConfig]):
        self.queues.extend([controller.queue for _, controller in actions.items()])
        self.action_queues = IndexQueue({name: controller for name, controller in actions.items()})

class WebApp(AppBase):
    _settings: UvicornSettings = None
    _fast_api = None

    def __init__(self, uvcorn_settings: UvicornSettings, controllers: List[Controller] = None):
        self._settings = uvcorn_settings
        self._controllers = controllers
        self._app_index = AppIndex()

    def async_lifespan(self):

        @asynccontextmanager
        async def lifespan(api: FastAPI):
            yield
            await self.close()

        self._fast_api = FastAPI(lifespan=lifespan)
        self._fast_api.state.app_index = self._app_index
        self._fast_api.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

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

            self.logger.info(f"Method: {request.method}, RequestId: {request_id}, Path: {request.url.path} Time: {process_time:.4f}s")
            return response

    def api_routes(self):
        return [r for r in self._fast_api.routes
            if isinstance(r, APIRoute)]

    def register_router(self, router):
        self._fast_api.include_router(router)

    def serve_with_static_files(self):
        self._fast_api.state.storage = FSBase(filesystem="memory")
        static_dir = setting("FastApi", "static_files")
        try:
            if os.path.exists(static_dir):
                self._fast_api.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
            else:
                self.logger.warning(f"Static directory '{static_dir}' not found. Skipping mount.")
            uvicorn.run(self._fast_api, **self._settings.model_dump())
        except BaseException as e:
            traceback.print_exception(e)
            self.logger.error(str(e))

    async def aserve(self):
        try:
            await asyncio.to_thread(self.serve_with_static_files)
        except BaseException as e:
            print(e)
            raise

    def init(self, stop_event, tg, logger=None):
        super().init(stop_event, tg, logger)
        tg.create_task(self.aserve())

class WebAppWithWorkers(WebApp, WorkerApp):
    def init(self, stop_event, tg, logger=None):
        WebApp.init(self, stop_event, tg, logger)
        WorkerApp.init(self, stop_event, tg, logger)

class AlembicWebAppWithWorkers(WebAppWithWorkers, AlembicApp):
    def __init__(self, uvicorn_settings, path, script_location, sql_alchemy_url):
        WebAppWithWorkers.__init__(self, uvicorn_settings)
        AlembicApp.__init__(self, path, script_location, sql_alchemy_url)

    def init(self, stop_event, tg, logger=None):
        WebAppWithWorkers.init(self, stop_event, tg, logger)

class SimpleAppWithWorkers(SimpleApp, WorkerApp):
    pass