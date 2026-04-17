import io
import uuid

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class AtomicWriteFs(FSBase):
    def write(self, file_path: str, file_buffer: io.BytesIO, use_pipe=None):
        file_buffer.seek(0)
        errors = []
        temp_path = f"{file_path}.{uuid.uuid4()}.tmp"

        if use_pipe is None:
            use_pipe = False

        try:
            if use_pipe:
                try:
                    self.client.pipe_file(temp_path, file_buffer.getvalue())
                except Exception as pipe_err:
                    errors.append(pipe_err)
            else:
                with self.client.open(temp_path, "wb") as fs:
                    fs.write(file_buffer.getbuffer())

            self.client.rename(temp_path, file_path)
        except Exception as write_err:
            if self.client.exists(temp_path):
                self.client.rm(temp_path)
            raise ExceptionGroup("Atomic write failed", [*errors, write_err])
        finally:
            file_buffer.truncate(0)
            file_buffer.seek(0)



