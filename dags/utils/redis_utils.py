import redis.asyncio as redis

async def get_redis_client(redis_url: str):
    return await redis.from_url(redis_url)

async def flush_queue(redis_url: str, q_name: str):
    client = await get_redis_client(redis_url)
    try:
        await client.delete(q_name)
    finally:
        await client.close()

async def push_data(redis_url: str, data, q_name: str):
    try:
        redis_client = await get_redis_client(redis_url)
        await redis_client.lpush(q_name, data)
    finally:
        await redis_client.close()

async def pop_data(redis_url: str, q_name: str):
    try:
        redis_client = await get_redis_client(redis_url)
        data =  await redis_client.rpop(q_name)
    finally:
        await redis_client.close()
    return data

    




