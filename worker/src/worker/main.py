import asyncio

import asyncpg
from redis.asyncio import Redis

from .config import settings
from .consumer import run_consumer


async def main():
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await run_consumer(redis, pool)
    finally:
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
