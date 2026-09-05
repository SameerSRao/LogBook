import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

import asyncpg
from fastapi import FastAPI, Query, Request
from redis.asyncio import Redis
from sse_starlette.sse import EventSourceResponse

from .config import settings
from .db import search_logs

PUBSUB_CHANNEL = "logbook:new-log"


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


@app.get("/logs/stream")
async def live_tail(request: Request, service: str | None = Query(None)):
    async def event_generator():
        pubsub = request.app.state.redis.pubsub()
        await pubsub.subscribe(PUBSUB_CHANNEL)
        try:
            async for message in pubsub.listen():
                if await request.is_disconnected():
                    break
                if message["type"] != "message":
                    continue
                data = json.loads(message["data"])
                if service and data.get("service") != service:
                    continue
                yield {"data": json.dumps(data)}
        finally:
            await pubsub.unsubscribe(PUBSUB_CHANNEL)
            await pubsub.aclose()

    return EventSourceResponse(event_generator())
