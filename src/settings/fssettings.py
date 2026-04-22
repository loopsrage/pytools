import io

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from src.settings.helper import restore


class FsSettings(FSBase):

    def __init__(self, filesystem: str, path: str, use_pipe=None):
        super().__init__(filesystem=filesystem)
        buffer = io.BytesIO()
        self.read(path, buffer, use_pipe)

        buffer.seek(0)
        restore(buffer.getvalue())
