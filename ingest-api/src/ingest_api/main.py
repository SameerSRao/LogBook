from contextlib import asynccontextmanager

import asyncpg
from redis.asyncio import Redis
from fastapi import FastAPI, Request

from .config import settings
from .queue import ingest_logs_to_queue
from .models import IngestRequest, IngestResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    yield
    await app.state.pool.close()
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/logs", response_model=IngestResponse)
async def ingest_logs(body: IngestRequest, request: Request):
    await ingest_logs_to_queue(request.app.state.redis, body.logs)
    return IngestResponse(accepted=len(body.logs))
