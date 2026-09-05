from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class LogEntry(BaseModel):
    timestamp: datetime
    service: str
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"]
    message: str
    context: dict | None = None


class IngestRequest(BaseModel):
    logs: list[LogEntry]


class IngestResponse(BaseModel):
    accepted: int
