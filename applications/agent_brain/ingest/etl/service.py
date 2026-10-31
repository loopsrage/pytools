import io
import uuid

import pandas as pd
from puremagic import from_stream

from validate_csv.validate_csv import (
    validate_file_extension,
    validate_max_size,
    validate_csv_header,
)
from csv_as_json.parse_csv_as_json import generate_json
from dynamic_model.dynamic_model import dynamic_model
from rapidfuzzlib.rapidfuzzlib import fuzz_one_against_choices

from .storage import LocalStorage
from .jobs import JobStore
from .models import JobStatus, TransformRequest, DedupeRequest, SchemaResponse

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


class ETLService:
    def __init__(self, storage: LocalStorage, jobs: JobStore):
        self.storage = storage
        self.jobs = jobs

    async def upload(self, upload_file) -> str:
        validate_file_extension(upload_file)
        await validate_csv_header(upload_file)
        await upload_file.seek(0)
        if upload_file.size is not None:
            validate_max_size(upload_file, MAX_FILE_SIZE)

        contents = await upload_file.read()
        df = pd.read_csv(io.BytesIO(contents))

        job_id = str(uuid.uuid4())
        self.storage.save_df(f"{job_id}/raw.csv", df)

        self.jobs.create(job_id, upload_file.filename or "upload.csv")
        self.jobs.update(
            job_id,
            status=JobStatus.VALIDATED,
            result={"rows": int(len(df)), "columns": list(df.columns)},
        )
        return job_id

    def transform(self, job_id: str, req: TransformRequest) -> dict:
        if self.jobs.get(job_id) is None:
            raise KeyError(f"Job {job_id} not found")

        df = self.storage.load_df(f"{job_id}/raw.csv")
        for col in req.json_columns:
            if col in df.columns:
                df[col] = df[col].fillna("")
        payloads = list(
            generate_json(
                df,
                promoted_columns=req.promoted_columns,
                json_columns=req.json_columns,
            )
        )

        self.storage.save_json(f"{job_id}/transformed.json", payloads)

        self.jobs.update(
            job_id,
            status=JobStatus.TRANSFORMED,
            result={"count": len(payloads), "sample": payloads[:5]},
        )
        return {"job_id": job_id, "count": len(payloads), "sample": payloads[:5]}

    def schema(self, job_id: str) -> SchemaResponse:
        if self.jobs.get(job_id) is None:
            raise KeyError(f"Job {job_id} not found")

        df = self.storage.load_df(f"{job_id}/raw.csv")
        fields: dict[str, type] = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype in ("int8", "int16", "int32", "int64", "UInt8", "UInt16", "UInt32", "UInt64"):
                fields[col] = int
            elif dtype in ("float16", "float32", "float64"):
                fields[col] = float
            elif dtype == "bool":
                fields[col] = bool
            else:
                fields[col] = str

        model = dynamic_model("Row", **fields)
        type_names = {k: v.__name__ for k, v in fields.items()}
        return SchemaResponse(
            model_name="Row",
            fields=type_names,
            json_schema=model.model_json_schema(),
        )

    def dedupe(self, job_id: str, req: DedupeRequest) -> dict:
        if self.jobs.get(job_id) is None:
            raise KeyError(f"Job {job_id} not found")

        df = self.storage.load_df(f"{job_id}/raw.csv")
        if req.column not in df.columns:
            raise ValueError(f"Column '{req.column}' not found")

        values = df[req.column].dropna().astype(str).tolist()
        unique_vals = list(dict.fromkeys(values))

        remaining = set(range(len(unique_vals)))
        clusters: list[list[str]] = []

        while remaining:
            i = min(remaining)
            remaining.discard(i)
            cluster = [unique_vals[i]]
            target = unique_vals[i]
            choices = [unique_vals[j] for j in sorted(remaining)]
            if choices:
                scores = fuzz_one_against_choices(target, choices, req.threshold)
                to_remove = []
                for j in sorted(remaining):
                    if scores.get(unique_vals[j], 0) >= req.threshold:
                        cluster.append(unique_vals[j])
                        to_remove.append(j)
                for j in to_remove:
                    remaining.discard(j)
            clusters.append(cluster)

        dup_groups = [c for c in clusters if len(c) > 1]
        self.jobs.update(
            job_id,
            status=JobStatus.DEDUPED,
            result={
                "column": req.column,
                "threshold": req.threshold,
                "unique_values": len(unique_vals),
                "duplicate_groups": dup_groups,
                "duplicate_group_count": len(dup_groups),
            },
        )
        return {
            "job_id": job_id,
            "column": req.column,
            "unique_values": len(unique_vals),
            "duplicate_groups": dup_groups,
        }

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def list_jobs(self):
        return self.jobs.list()
