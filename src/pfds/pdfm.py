import threading
import pypdfium2
from pypdfium2 import PdfDocument

from src.thread_safe.index import Index


class PDFM:

    _index: Index = None
    _raw_data: bytes = None
    _lock: threading.Lock
    _render_dpi: int = 300

    document: PdfDocument
    path: str = None
    page_count: int = 0
    page_references: dict

    def __init__(self, pdf_path: str):
        self.path = pdf_path
        self._index = Index().new(pdf_path)
        self._lock = threading.Lock()
        with open(self.path, "rb") as f:
            self._raw_data = f.read()

        self.document = pypdfium2.PdfDocument(self._raw_data)
        self.page_count = len(self.document)

    def page(self, page_number):
        with self._lock:
            page = self.document[page_number]
            return page.get_textpage()

    def render(self, page_number):
        scale = self._render_dpi / 72
        with self._lock:
            page = self.document[page_number]
            bitmap = page.render(scale=scale)
        return bitmap.to_pil()

    def extract_text(self, page_number):
        self._index.store_in_index(
            self.path,
            page_path(page_number, ".txt"),
            self.page(page_number))

    def render_page(self, page_number):
        self._index.store_in_index(
            self.path,
            page_path(page_number, ".png"),
            self.render(page_number),
        )

    def range_data(self):
        for key, value in self._index.range_index(self.path):
            yield key, value

    def close(self):
        with self._lock:
            self.document.close()


def page_path(page_number: int, page_derivative: str):
    return f"{str(page_number)}_{page_derivative}"

def extract_text(pdfm: PDFM, executor):
    executor.map(pdfm.extract_text, range(0, pdfm.page_count))

def extract_images(pdfm: PDFM, executor):
    executor.map(pdfm.render_page, range(0, pdfm.page_count))