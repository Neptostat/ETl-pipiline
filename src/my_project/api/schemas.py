
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, HttpUrl

class RunRequest(BaseModel):
    source_path: Optional[str] = Field(None, description="Path or URL to source data (e.g., CSV URL)")
    db_url: Optional[str] = Field(None, description="SQLAlchemy database URL")
    table_name: Optional[str] = Field(None, description="Destination table name")
    chunk_size: Optional[int] = Field(None, description="CSV chunk size")

class RunResponse(BaseModel):
    job_id: str
    status: str
    started_at: datetime

class StatusResponse(BaseModel):
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
