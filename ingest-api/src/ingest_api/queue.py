import json
from redis.asyncio import Redis
from .models import LogEntry

STREAM_NAME = "logbook:logs"


async def ingest_logs_to_queue(redis: Redis, entries: list[LogEntry]) -> None:
    pipe = redis.pipeline()
    for e in entries:
        fields = {
            "timestamp": e.timestamp.isoformat(),
            "service": e.service,
            "level": e.level,
            "message": e.message,
            "context": json.dumps(e.context) if e.context else "",
        }
        pipe.xadd(STREAM_NAME, fields)
    await pipe.execute()
