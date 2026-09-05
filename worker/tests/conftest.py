import os

import asyncpg
import pytest
from redis.asyncio import Redis

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


@pytest.fixture(autouse=True)
async def clean_state():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE logs RESTART IDENTITY")
    await conn.close()
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    await r.delete("logbook:logs")
    await r.aclose()
    yield
