from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request

from .config import settings
from .db import insert_logs
from .models import IngestRequest, IngestResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/logs", response_model=IngestResponse)
async def ingest_logs(body: IngestRequest, request: Request):
    async with request.app.state.pool.acquire() as conn:
        await insert_logs(conn, body.logs)
    return IngestResponse(accepted=len(body.logs))
