import threading
import pypdfium2
from pypdfium2 import PdfDocument

from thread_safe.index import Index


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

    def delete_page(self, page_number):
        self.delete_page_png(page_number)
        self.delete_page_png(page_number)

    def extract_text(self, page_number, rect: dict=None):
        text_page = self.page(page_number)
        payload = None
        if rect:
            payload = {
                "left":   rect.get("left") or None,
                "bottom": rect.get("bottom") or None,
                "right": rect.get("right") or None,
                "top": rect.get("top")
            }
        data = text_page.get_text_bounded(**(payload or {})).encode("utf8")
        self._index.store_in_index(
            self.path,
            page_path(page_number, ".txt"),
            data)

    def load_page_text(self, page_number):
        page = page_path(page_number, ".txt")
        return self._index.load_from_index(self.path, page)

    def load_page_png(self, page_number):
        page = page_path(page_number, ".png")
        return self._index.load_from_index(self.path, page)

    def read_page_text(self, page_number, rect: dict=None):
        self.extract_text(page_number, rect)
        return self.load_page_text(page_number)

    def read_page_png(self, page_number):
        self.render_page(page_number)
        return self.load_page_png(page_number)

    def delete_page_text(self, page_number):
        page = page_path(page_number, ".txt")
        self._index.delete_from_index(self.path, page)

    def delete_page_png(self, page_number):
        page = page_path(page_number, ".png")
        self._index.delete_from_index(self.path, page)

    def load_page(self, page_number):
        return (
            self.read_page_text(page_number),
            self.read_page_png(page_number)
        )

    def load_page_data(self, page_number):
        return (
            self.load_page_text(page_number),
            self.load_page_png(page_number)
        )

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
    list(executor.map(pdfm.extract_text, range(0, pdfm.page_count)))

def extract_images(pdfm: PDFM, executor):
    list(executor.map(pdfm.render_page, range(0, pdfm.page_count)))