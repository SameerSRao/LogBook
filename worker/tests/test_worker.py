import json
import os

import asyncpg
import pytest
from redis.asyncio import Redis

from worker.consumer import STREAM_NAME, GROUP_NAME, ensure_consumer_group
from worker.db import insert_log_row

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


async def test_worker_reads_stream_and_writes_to_postgres():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    try:
        await redis.xgroup_destroy(STREAM_NAME, GROUP_NAME)
    except Exception:
        pass
    await ensure_consumer_group(redis)

    await redis.xadd(STREAM_NAME, {
        "timestamp": "2026-09-05T12:00:00+00:00",
        "service": "test-svc",
        "level": "WARN",
        "message": "something happened",
        "context": json.dumps({"req_id": "abc"}),
    })

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)

    messages = await redis.xreadgroup(
        GROUP_NAME, "worker-1", {STREAM_NAME: ">"}, count=10, block=500
    )
    assert messages, "no messages found in stream"
    _, entries = messages[0]
    async with pool.acquire() as conn:
        for msg_id, fields in entries:
            await insert_log_row(conn, fields)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow("SELECT * FROM logs WHERE service = 'test-svc'")
    await conn.close()
    await pool.close()
    await redis.aclose()

    assert row is not None
    assert row["message"] == "something happened"
    assert row["level"] == "WARN"
    assert row["context"] == '{"req_id": "abc"}'


async def test_log_without_context_inserts_null():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    try:
        await redis.xgroup_destroy(STREAM_NAME, GROUP_NAME)
    except Exception:
        pass
    await ensure_consumer_group(redis)

    await redis.xadd(STREAM_NAME, {
        "timestamp": "2026-09-05T12:00:00+00:00",
        "service": "no-ctx-svc",
        "level": "INFO",
        "message": "no context here",
        "context": "",
    })

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    messages = await redis.xreadgroup(
        GROUP_NAME, "worker-1", {STREAM_NAME: ">"}, count=10, block=500
    )
    _, entries = messages[0]
    async with pool.acquire() as conn:
        for msg_id, fields in entries:
            await insert_log_row(conn, fields)
            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow("SELECT * FROM logs WHERE service = 'no-ctx-svc'")
    await conn.close()
    await pool.close()
    await redis.aclose()

    assert row["context"] is None
