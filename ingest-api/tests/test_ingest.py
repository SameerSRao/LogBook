import json
import os

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport
from redis.asyncio import Redis

from ingest_api.main import app
from ingest_api.queue import STREAM_NAME

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@pytest.fixture
async def redis():
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    await r.delete(STREAM_NAME)
    yield r
    await r.delete(STREAM_NAME)
    await r.aclose()


@pytest.fixture
async def client(redis):
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    app.state.pool = pool
    app.state.redis = redis
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    await pool.close()


async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ingest_pushes_to_stream(client, redis):
    payload = {
        "logs": [{
            "timestamp": "2026-09-05T10:00:00Z",
            "service": "gymlog",
            "level": "INFO",
            "message": "User logged in",
            "context": {"user_id": 42}
        }]
    }
    resp = await client.post("/logs", json=payload)
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 1}

    messages = await redis.xrange(STREAM_NAME)
    assert len(messages) == 1
    _, fields = messages[0]
    assert fields["service"] == "gymlog"
    assert fields["level"] == "INFO"
    assert fields["message"] == "User logged in"
    assert json.loads(fields["context"]) == {"user_id": 42}


async def test_ingest_batch_pushes_all(client, redis):
    payload = {
        "logs": [
            {"timestamp": "2026-09-05T10:00:00Z", "service": "svc", "level": "DEBUG", "message": "a"},
            {"timestamp": "2026-09-05T10:00:01Z", "service": "svc", "level": "ERROR", "message": "b"},
        ]
    }
    resp = await client.post("/logs", json=payload)
    assert resp.json()["accepted"] == 2

    messages = await redis.xrange(STREAM_NAME)
    assert len(messages) == 2


async def test_invalid_level_rejected(client):
    payload = {
        "logs": [{"timestamp": "2026-09-05T10:00:00Z", "service": "svc", "level": "TRACE", "message": "x"}]
    }
    resp = await client.post("/logs", json=payload)
    assert resp.status_code == 422
