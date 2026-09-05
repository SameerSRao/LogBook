import os
import pytest
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")


@pytest.fixture(autouse=True)
async def clean_logs():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE logs RESTART IDENTITY")
    await conn.close()
    yield
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE logs RESTART IDENTITY")
    await conn.close()
