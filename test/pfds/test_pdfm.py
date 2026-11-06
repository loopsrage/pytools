import pathlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
import pypdfium2 as pdfium

from pfds.pdfm import PDFM, extract_images, extract_text, page_path


class TestPDFM(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.pdf_path = pathlib.Path(self._tmpdir.name) / "sample.pdf"
        doc = pdfium.PdfDocument.new()
        doc.new_page(200, 200)
        doc.new_page(200, 200)
        doc.save(str(self.pdf_path))
        doc.close()

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_pdfm_lifecycle_and_helpers(self):
        assert page_path(0, ".txt") == "0_.txt"
        pdfm = PDFM(str(self.pdf_path))
        try:
            assert pdfm.page_count == 2
            txt, img = pdfm.load_page(0)
            assert isinstance(txt, bytes)
            assert img.size[0] > 0 and img.size[1] > 0

            loaded_txt, loaded_img = pdfm.load_page_data(0)
            assert loaded_txt == txt
            assert loaded_img is img

            bounded_txt = pdfm.read_page_text(0, rect={"left": 0, "bottom": 0, "right": 100, "top": 100})
            assert isinstance(bounded_txt, bytes)

            assert len(list(pdfm.range_data())) == 2
            pdfm.delete_page(0)
            assert pdfm.load_page_text(0) is None
            assert pdfm.load_page_png(0) is None

            with ThreadPoolExecutor(max_workers=2) as ex:
                extract_text(pdfm, ex)
                extract_images(pdfm, ex)

            assert pdfm.load_page_text(1) is not None
            assert pdfm.load_page_png(1) is not None
        finally:
            pdfm.close()


if __name__ == "__main__":
    unittest.main()
