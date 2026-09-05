from datetime import datetime

import asyncpg


async def insert_log_row(conn: asyncpg.Connection, fields: dict) -> None:
    context = fields.get("context", "")
    await conn.execute(
        """
        INSERT INTO logs (timestamp, service, level, message, context)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        """,
        datetime.fromisoformat(fields["timestamp"]),
        fields["service"],
        fields["level"],
        fields["message"],
        context if context else None,
    )
