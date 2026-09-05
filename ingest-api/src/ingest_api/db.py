import json
import asyncpg
from .models import LogEntry


async def insert_logs(conn: asyncpg.Connection, entries: list[LogEntry]) -> None:
    rows = [
        (
            e.timestamp,
            e.service,
            e.level,
            e.message,
            json.dumps(e.context) if e.context else None,
        )
        for e in entries
    ]
    await conn.executemany(
        """
        INSERT INTO logs (timestamp, service, level, message, context)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        """,
        rows,
    )
