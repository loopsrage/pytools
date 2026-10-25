import asyncio
import pathlib
import tempfile
import unittest

import pypdfium2 as pdfium

from application_controller.app_controller import AppIndex
from fsspecc import FSBase
from httpclient.api import Api
from indexes.app_ctrl_index.appctrl import ApplicationIndex
from indexes.datasource_index.datasource_index import Datasource, DatasourceIndex
from indexes.fsindex.fsindex import FilesystemIndex
from indexes.ground_truth_index.gt_index import GroundTruthIndex, GTIndex
from indexes.pdf_index.pdf_index import PDFIndex
from indexes.worker_service_index.worker_index import WorkerServiceIndex
from pfds.pdfm import PDFM
from service_controller.service_controller import ServiceController


class MemoryDatasource(Datasource):
    def query_datasource(self):
        return ["row1", "row2"]


class TestAllIndexes(unittest.IsolatedAsyncioTestCase):

    def test_application_index(self):
        idx = ApplicationIndex()
        idx.register_applications({"app1": "svc1"})
        val, loaded = idx.application("app1")
        assert val == "svc1" and loaded is True

    def test_datasource_index(self):
        idx = DatasourceIndex()
        ds = MemoryDatasource()
        idx.register_datasources({"mem": ds})
        loaded_ds, was_loaded = idx.datasource("mem", ds)
        assert loaded_ds is ds and was_loaded is True
        assert loaded_ds.query_datasource() == ["row1", "row2"]
        assert dict(idx.list_datasource()) == {"mem": ds}

    def test_filesystem_index(self):
        idx = FilesystemIndex()
        fs = FSBase("memory")
        idx.register_filesystems({"mem": fs})
        loaded_fs, was_loaded = idx.filesystem("mem")
        assert loaded_fs is fs and was_loaded is True

    def test_gt_index_and_ground_truth_index(self):
        gt = GTIndex()
        gt.register_gt("q1", "a1")
        assert gt.load_gt("q1") == "a1"

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = pathlib.Path(tmpdir) / "hts.csv"
            csv_path.write_text("Item ID,HTS US\n1001,8471300100\n1002,invalid\nbad_id,1234567890\n")
            gti = GroundTruthIndex(str(csv_path))
            assert gti.item_to_hts(1001) == "8471300100"
            assert gti.item_to_hts("1001") == "8471300100"
            assert gti.item_to_formatted(1001) == "8471.30.0100"

    def test_pdf_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p1 = pathlib.Path(tmpdir) / "one.pdf"
            p2 = pathlib.Path(tmpdir) / "two.pdf"

            doc1 = pdfium.PdfDocument.new()
            doc1.new_page(200, 200)
            doc1.save(str(p1))
            doc1.close()

            doc2 = pdfium.PdfDocument.new()
            doc2.new_page(200, 200)
            doc2.new_page(200, 200)
            doc2.save(str(p2))
            doc2.close()

            pidx = PDFIndex()
            pidx.store_from_file(str(p1))
            pidx.store_pdf("two", PDFM(pdf_path=str(p2)))

            assert pidx.load_pdf("two").page_count == 2
            assert len(pidx.list_pdfs()) == 2
            sorted_asc = list(pidx.sort_by_page_count(reverse=False).keys())
            sorted_desc = list(pidx.sort_by_page_count(reverse=True).keys())
            assert sorted_asc == [str(p1), "two"]
            assert sorted_desc == ["two", str(p1)]

    async def test_worker_service_index_and_app_index_isolation(self):
        widx = WorkerServiceIndex()
        svc = ServiceController()
        widx.register_workers({"w1": svc})
        loaded_w, was_loaded = widx.worker("w1")
        assert loaded_w is svc and was_loaded is True

        stop_event = asyncio.Event()
        async with asyncio.TaskGroup() as tg:
            widx.start_workers(stop_event, tg)
            stop_event.set()
            await widx.close_workers()

        # Verify AppIndex multi-inheritance namespace isolation
        app_idx = AppIndex()
        fs = FSBase("memory")
        api = Api("http://localhost")
        app_idx.application("shared_name", "app_obj")
        app_idx.worker("shared_name", svc)
        app_idx.filesystem("shared_name", fs)
        app_idx.connection("shared_name", "conn_obj")
        app_idx.api("shared_name", api)

        assert app_idx.application("shared_name")[0] == "app_obj"
        assert app_idx.worker("shared_name")[0] is svc
        assert app_idx.filesystem("shared_name")[0] is fs
        assert app_idx.connection("shared_name")[0] == "conn_obj"
        assert app_idx.api("shared_name")[0] is api


if __name__ == "__main__":
    unittest.main()
