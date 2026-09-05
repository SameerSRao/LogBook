from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

import asyncpg
from fastapi import FastAPI, Query, Request

from .config import settings
from .db import search_logs


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/logs")
async def query_logs(
    request: Request,
    service: str | None = Query(None),
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] | None = Query(None),
    q: str | None = Query(None),
    before: datetime | None = Query(None),
    after: datetime | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    logs, total = await search_logs(
        request.app.state.pool, service, level, q, before, after, limit, offset
    )
    return {"logs": logs, "total": total}
