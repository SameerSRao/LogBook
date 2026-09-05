import os
import pytest
import asyncpg
from httpx import AsyncClient, ASGITransport
from ingest_api.main import app

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")


@pytest.fixture
async def client():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    app.state.pool = pool
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    await pool.close()


async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ingest_single_log(client):
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

    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow("SELECT * FROM logs WHERE service = 'gymlog'")
    await conn.close()
    assert row["message"] == "User logged in"
    assert row["level"] == "INFO"
    assert row["context"] == '{"user_id": 42}'


async def test_ingest_batch(client):
    payload = {
        "logs": [
            {"timestamp": "2026-09-05T10:00:00Z", "service": "svc", "level": "DEBUG", "message": "a"},
            {"timestamp": "2026-09-05T10:00:01Z", "service": "svc", "level": "ERROR", "message": "b"},
        ]
    }
    resp = await client.post("/logs", json=payload)
    assert resp.json()["accepted"] == 2

    conn = await asyncpg.connect(DATABASE_URL)
    count = await conn.fetchval("SELECT COUNT(*) FROM logs WHERE service = 'svc'")
    await conn.close()
    assert count == 2


async def test_invalid_level_rejected(client):
    payload = {
        "logs": [{"timestamp": "2026-09-05T10:00:00Z", "service": "svc", "level": "TRACE", "message": "x"}]
    }
    resp = await client.post("/logs", json=payload)
    assert resp.status_code == 422
