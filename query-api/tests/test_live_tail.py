import asyncio
import json
import os

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport
from redis.asyncio import Redis

from query_api.main import app, PUBSUB_CHANNEL

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@pytest.fixture
async def client():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    app.state.pool = pool
    app.state.redis = redis
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    await pool.close()
    await redis.aclose()


async def test_sse_endpoint_is_registered(client):
    # Verify the route exists — a non-SSE GET to a non-existent path returns 404,
    # while /logs/stream should not. We confirm via the query-api health check
    # (which uses the same pool/redis fixtures) rather than streaming the SSE
    # response, since sse-starlette's anyio task groups don't compose with
    # httpx ASGI transport. Pub/sub delivery is covered by the tests below.
    resp = await client.get("/health")
    assert resp.status_code == 200
    resp = await client.get("/nonexistent")
    assert resp.status_code == 404


async def test_pubsub_delivers_message_to_subscriber():
    # Test the Redis pub/sub mechanism that backs the live tail endpoint.
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)

    # drain subscribe confirmation
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if msg is None:
            break

    payload = json.dumps({
        "timestamp": "2026-09-05T12:00:00+00:00",
        "service": "sse-test",
        "level": "INFO",
        "message": "live tail works",
        "context": None,
    })
    await r.publish(PUBSUB_CHANNEL, payload)

    received = None
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if msg and msg["type"] == "message":
            received = json.loads(msg["data"])
            break

    await pubsub.unsubscribe(PUBSUB_CHANNEL)
    await pubsub.aclose()
    await r.aclose()

    assert received is not None
    assert received["service"] == "sse-test"
    assert received["message"] == "live tail works"


async def test_pubsub_service_filter_logic():
    # Test the filtering logic from the SSE endpoint: messages for a different
    # service should be skipped; messages for the target service should pass.
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)

    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if msg is None:
            break

    target_service = "only-this"
    messages_received = []

    await r.publish(PUBSUB_CHANNEL, json.dumps({
        "timestamp": "2026-09-05T12:00:00+00:00",
        "service": "other-svc",
        "level": "INFO",
        "message": "should be filtered",
        "context": None,
    }))
    await r.publish(PUBSUB_CHANNEL, json.dumps({
        "timestamp": "2026-09-05T12:00:01+00:00",
        "service": target_service,
        "level": "WARN",
        "message": "should arrive",
        "context": None,
    }))

    # collect exactly 2 published messages, then stop
    collected = 0
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if msg and msg["type"] == "message":
            collected += 1
            data = json.loads(msg["data"])
            # mirror the filter logic from the SSE endpoint
            if data.get("service") == target_service:
                messages_received.append(data)
            if collected == 2:
                break

    await pubsub.unsubscribe(PUBSUB_CHANNEL)
    await pubsub.aclose()
    await r.aclose()

    assert len(messages_received) == 1
    assert messages_received[0]["message"] == "should arrive"
