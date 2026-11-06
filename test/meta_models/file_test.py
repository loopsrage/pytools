import datetime
import unittest

from meta_models.file import UploadedFiles, file_data_index
from meta_models.working import Working, work_stage_index
from meta_models.working_service import WorkerActionService, WorkerServiceConfig


class TestMetaModels(unittest.IsolatedAsyncioTestCase):

    def test_working_and_uploaded_files_schema(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        w = Working(stage="initial", scheduled_for=now, retries=1)
        dumped = w.model_dump(mode="json")
        assert dumped["stage"] == "initial"
        assert dumped["retries"] == 1

        assert UploadedFiles.__tablename__ == "files"
        idx_names = {idx.name for idx in UploadedFiles.__table__.indexes}
        assert "idx_unique_hash" in idx_names
        assert "idx_initial_stage" in idx_names
        assert "idx_pending_stage" in idx_names

        class DummyNoHash:
            pass

        assert file_data_index(DummyNoHash) is None
        assert work_stage_index(DummyNoHash, "initial") is None

    async def test_worker_action_service(self):
        calls = []

        async def my_action():
            calls.append(1)

        cfg = WorkerServiceConfig(identity="test_worker", worker_interval=1, start_now=True)
        svc = WorkerActionService(config=cfg, action=my_action)
        try:
            assert len(svc.controllers) == 1
        finally:
            svc.close_controllers()


if __name__ == "__main__":
    unittest.main()
