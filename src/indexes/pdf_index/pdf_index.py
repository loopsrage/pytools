from pfds.pdfm import PDFM
from thread_safe.index import Index


class PDFIndex:
    _index: Index = None
    _namespace: str = None

    def __init__(self):
        self._namespace = "PDFS"
        self._index = Index().new(self._namespace)

    def store_from_file(self, file: str):
        self.store_pdf(file, PDFM(pdf_path=file))

    def store_pdf(self, name: str, pdf: PDFM):
        self._index.store_in_index(self._namespace, name, pdf)

    def load_pdf(self, name) -> PDFM:
        return self._index.load_from_index(self._namespace, name)

    def list_pdfs(self):
        return list(self._index.range_index(self._namespace))

    def range_pdfs(self):
        yield from self._index.range_index(self._namespace)

    def sort_by_page_count(self, reverse = False):
        values = list(self.range_pdfs())
        sorted_values = sorted(values, key=lambda item: item[1].page_count, reverse=reverse)
        return {name: pdf for name, pdf in sorted_values}
