
from __future__ import annotations
import logging
import uuid
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ..services.pipeline import ETLPipeline
from .schemas import RunRequest, RunResponse, StatusResponse

logger = logging.getLogger(__name__)

app = FastAPI(title="ETL Pipeline API", version="0.1.0")

# Allow local dev UIs (optional)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job registry (simple demo; swap for Redis/DB in production)
JOBS: Dict[str, Dict[str, Any]] = {}

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

def _run_pipeline(job_id: str, payload: RunRequest) -> None:
    logger.info("Job %s started", job_id)
    job = JOBS[job_id]
    job["status"] = "running"
    try:
        # Instrumentation: count rows in/out per chunk via a thin wrapper
        rows_in_total = 0
        rows_out_total = 0

        # Monkey-patch: wrap ETLPipeline.run to update metrics per chunk
        from ..core.extract import read_csv
        from ..core.transform import transform
        from ..core.load import make_engine, to_sql

        engine = make_engine(payload.db_url or None or ETLPipeline().db_url)
        source_path = payload.source_path or ETLPipeline().source_path
        table_name = payload.table_name or ETLPipeline().table_name
        chunk_size = payload.chunk_size or ETLPipeline().chunk_size

        for i, chunk in enumerate(read_csv(source_path, chunksize=chunk_size), start=1):
            rows_in_total += len(chunk)
            tidy = transform(chunk)
            rows_out_total += len(tidy)
            to_sql(tidy, engine, table_name, if_exists="append")

        job["status"] = "succeeded"
        job["rows_in"] = rows_in_total
        job["rows_out"] = rows_out_total
        logger.info("Job %s succeeded: rows_in=%s rows_out=%s", job_id, rows_in_total, rows_out_total)
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        logger.exception("Job %s failed: %s", job_id, e)
    finally:
        job["finished_at"] = datetime.utcnow()

@app.post("/run", response_model=RunResponse, summary="Start an ETL run in the background")
def run_pipeline(payload: RunRequest, bg: BackgroundTasks):
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "status": "queued",
        "started_at": datetime.utcnow(),
        "finished_at": None,
        "error": None,
        "rows_in": None,
        "rows_out": None,
        "params": payload.model_dump(),
    }
    bg.add_task(_run_pipeline, job_id, payload)
    return RunResponse(job_id=job_id, status="queued", started_at=JOBS[job_id]["started_at"])

@app.get("/status/{job_id}", response_model=StatusResponse, summary="Get job status/metrics")
def get_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_id not found")
    return StatusResponse(**{
        "status": job["status"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
        "error": job["error"],
        "rows_in": job["rows_in"],
        "rows_out": job["rows_out"],
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("my_project.api.app:app", host="0.0.0.0", port=8000, reload=True)
