import asyncpg
import json
from redis.asyncio import Redis

from .db import insert_log_row

STREAM_NAME = "logbook:logs"
GROUP_NAME = "logbook-workers"
CONSUMER_NAME = "worker-1"
BATCH_SIZE = 100
BLOCK_MS = 2000
PUBSUB_CHANNEL = "logbook:new-log"


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


async def run_consumer(redis: Redis, pool: asyncpg.Pool) -> None:
    await ensure_consumer_group(redis)
    while True:
        messages = await redis.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=BATCH_SIZE,
            block=BLOCK_MS,
        )
        if not messages:
            continue
        _, entries = messages[0]
        async with pool.acquire() as conn:
            for msg_id, fields in entries:
                await insert_log_row(conn, fields)
                await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                await redis.publish(PUBSUB_CHANNEL, json.dumps({
                    "timestamp": fields["timestamp"],
                    "service": fields["service"],
                    "level": fields["level"],
                    "message": fields["message"],
                    "context": json.loads(fields["context"]) if fields.get("context") else None,
                }))
