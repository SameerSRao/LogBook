import os

import asyncpg
import pytest

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logbook:logbook@localhost:5433/logbook")


@pytest.fixture(autouse=True)
async def seed_logs():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE logs RESTART IDENTITY")
    await conn.execute("""
        INSERT INTO logs (timestamp, service, level, message, context) VALUES
        ('2026-09-05 10:00:00+00', 'gymlog', 'INFO',  'User logged in',  '{"user_id": 1}'),
        ('2026-09-05 10:01:00+00', 'gymlog', 'ERROR', 'DB timeout',       NULL),
        ('2026-09-05 10:02:00+00', 'billing', 'WARN', 'Retry attempt',   '{"attempt": 2}')
    """)
    await conn.close()
    yield
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("TRUNCATE logs RESTART IDENTITY")
    await conn.close()
