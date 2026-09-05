import json
from datetime import datetime

import asyncpg


async def search_logs(
    pool: asyncpg.Pool,
    service: str | None,
    level: str | None,
    q: str | None,
    before: datetime | None,
    after: datetime | None,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    conditions = []
    params: list = []
    i = 1

    if service:
        conditions.append(f"service = ${i}")
        params.append(service)
        i += 1
    if level:
        conditions.append(f"level = ${i}")
        params.append(level)
        i += 1
    if q:
        conditions.append(f"message ILIKE ${i}")
        params.append(f"%{q}%")
        i += 1
    if after:
        conditions.append(f"timestamp >= ${i}")
        params.append(after)
        i += 1
    if before:
        conditions.append(f"timestamp <= ${i}")
        params.append(before)
        i += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM logs {where}", *params)
        rows = await conn.fetch(
            f"""
            SELECT id, timestamp, service, level, message, context, ingested_at
            FROM logs {where}
            ORDER BY timestamp DESC
            LIMIT ${i} OFFSET ${i + 1}
            """,
            *params, limit, offset,
        )

    def row_to_dict(r: asyncpg.Record) -> dict:
        return {
            "id": r["id"],
            "timestamp": r["timestamp"].isoformat(),
            "service": r["service"],
            "level": r["level"],
            "message": r["message"],
            "context": json.loads(r["context"]) if r["context"] else None,
            "ingested_at": r["ingested_at"].isoformat(),
        }

    return [row_to_dict(r) for r in rows], total
