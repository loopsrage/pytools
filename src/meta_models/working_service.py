import threading
from typing import Any

from pydantic_settings import BaseSettings

from meta_models.file import UploadedFiles
from meta_models.working import select_work
from periodic_producer.periodic_producer import PeriodicProducer
from postgreslib.engine import named_session
from queue_controller.helpers import new_controller
from service_controller.service_controller import ServiceController

class WorkerServiceConfig(BaseSettings):
    identity: str
    session_name: str
    enabled: bool = True
    stage: str = "initial"
    limit: int = 10
    retries: int = 3
    set_stage: str | None = None
    action: Any
    worker_count: int = 5
    worker_interval: int = 1
    start_now: bool = True

class WorkerService(ServiceController):
    identity: str = None
    config: WorkerServiceConfig
    _lock = threading.RLock

    def __init__(self, session_name: str, config: WorkerServiceConfig):
        self.session_name = session_name
        self._lock = threading.RLock()
        self.config = config
        self.start()

    def start(self):
        if self.config.enabled:
            self.either = True
            self.initial()

    def initial(self):

        def cba(*args, **kwargs):
            async def action():
                with self._lock:
                    sn = self.session_name
                    stage = self.config.stage
                    set_stage = self.config.set_stage
                    limit = self.config.limit
                    retries = self.config.retries

                with named_session(sn) as session:
                    res = select_work(
                        session=session,
                        model=UploadedFiles,
                        stage=stage,
                        limit=limit,
                        retries=retries,
                        set_stage=set_stage
                    )
                return res
            return action

        initq = new_controller(
            action=self.config.action,
            worker_count=self.config.worker_count)
        result = PeriodicProducer(
            queue=initq,
            action=cba(),
            interval=self.config.worker_interval,
            start_now=self.config.start_now)
        self.queues.append(initq)
        self.controllers.append(result)