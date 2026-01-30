import asyncio
import csv
import io

import pandas as pd
import puremagic
from starlette.concurrency import run_in_threadpool

def validate_structure(header):
    # Read a small sample of the text
    sample = header.decode("utf-8")
    has_header = csv.Sniffer().has_header(sample)
    if not has_header:
        raise ValueError("Headers required")

    dialect = csv.Sniffer().sniff(sample)
    if dialect.delimiter not in [",", ";", "\t"]:
        raise ValueError("No common delimiter found")

def validate_max_size(file, max_file_size: int):
    if file.size > max_file_size:
        raise Exception("File too large")

async def validate_csv_header(file):
    header = await file.read(2048)
    await file.seek(0)

    try:
        mime = await asyncio.to_thread(puremagic.from_stream, file.file)
        await file.seek(0)

        if mime not in ["text/csv", "text/plain"]:
            raise Exception("Invalid file type")
    except Exception:
        mime = "text/plain"

    is_csv_content = b"," in header or b";" in header
    is_valid_mime = mime in ["text/csv", "text/plain", "application/csv"]
    is_valid_header = file.content_type in ["text/csv", "application/vnd.ms-excel"]
    if not (is_csv_content and (is_valid_mime or is_valid_header)):
        raise Exception(f"Invalid file type: {mime}")
    return header

async def read_contents_in_threadpool(storage, file, request_id):
    contents = await file.read()

    def encase(ct):
        df = pd.read_csv(io.BytesIO(ct))
        storage.save_raw_file(df, request_id, use_pipe=True)
        return df

    return await run_in_threadpool(encase, contents)

def validate_file_extension(file):
    if not file.filename.lower().endswith(".csv"):
        raise Exception("File must have a .csv extension")