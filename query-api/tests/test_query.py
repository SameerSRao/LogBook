import os

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport

from query_api.main import app

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


async def test_returns_all_logs(client):
    resp = await client.get("/logs")
    data = resp.json()
    assert data["total"] == 3
    assert len(data["logs"]) == 3


async def test_filter_by_service(client):
    resp = await client.get("/logs?service=gymlog")
    data = resp.json()
    assert data["total"] == 2
    assert all(l["service"] == "gymlog" for l in data["logs"])


async def test_filter_by_level(client):
    resp = await client.get("/logs?level=ERROR")
    data = resp.json()
    assert data["total"] == 1
    assert data["logs"][0]["message"] == "DB timeout"


async def test_search_message(client):
    resp = await client.get("/logs?q=retry")
    data = resp.json()
    assert data["total"] == 1
    assert "Retry" in data["logs"][0]["message"]


async def test_search_is_case_insensitive(client):
    resp = await client.get("/logs?q=USER")
    data = resp.json()
    assert data["total"] == 1
    assert data["logs"][0]["message"] == "User logged in"


async def test_pagination_limit(client):
    resp = await client.get("/logs?limit=2&offset=0")
    data = resp.json()
    assert data["total"] == 3
    assert len(data["logs"]) == 2


async def test_pagination_offset(client):
    resp = await client.get("/logs?limit=2&offset=2")
    data = resp.json()
    assert data["total"] == 3
    assert len(data["logs"]) == 1


async def test_results_ordered_newest_first(client):
    resp = await client.get("/logs")
    logs = resp.json()["logs"]
    timestamps = [l["timestamp"] for l in logs]
    assert timestamps == sorted(timestamps, reverse=True)


async def test_context_returned_as_object(client):
    resp = await client.get("/logs?service=gymlog&level=INFO")
    data = resp.json()
    assert data["logs"][0]["context"] == {"user_id": 1}


async def test_null_context_returned_as_none(client):
    resp = await client.get("/logs?level=ERROR")
    data = resp.json()
    assert data["logs"][0]["context"] is None


async def test_invalid_level_rejected(client):
    resp = await client.get("/logs?level=TRACE")
    assert resp.status_code == 422
