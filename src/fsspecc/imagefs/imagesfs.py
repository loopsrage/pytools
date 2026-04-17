import io

from lib.fsspecc.base_fsspecfs.base_fsspecfs import FSBase, get_file_path


class ImagesFs(FSBase):

    def __init__(self, filesystem: str = None):
        super().__init__(filesystem)

    def _write_png(self, file_path, figure, use_pipe=None):
        img_buffer = io.BytesIO()
        figure.savefig(img_buffer, format='png', bbox_inches='tight')
        self.write(file_path, img_buffer, use_pipe)

    def list_images(self, request_id: str):
        file_path = f"{get_file_path(request_id, "images/*.png")}"
        for i in self.client.glob(file_path):
            yield i

    def save_png_file(self, request_id, file_name, figure, use_pipe=None):
        file_path = f"{get_file_path(request_id, file_name, sub_dir="images")}"
        self._write_png(file_path, figure, use_pipe)

    def get_png_bytes(self, request_id: str, file_name: str) -> bytes:
        file_path = get_file_path(request_id, file_name, sub_dir="images")
        with self.client.open(file_path, "rb") as f:
            return f.read()