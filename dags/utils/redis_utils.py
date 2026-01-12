import redis.asyncio as redis
from .encode_utils import Serialize, Deserialize
from typing import Optional, Callable, Any

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


# will add context manager next
class CustomRedis:
    def __init__(self, redis_url: str, serializer: Callable = Serialize, deserializer: Callable = Deserialize):
        self._client = None
        self.redis_url = redis_url
        self._serializer = serializer
        self._deserializer = deserializer


    async def close_client(self):
        if self._client is not None:
            try:
                await self._client.close()
            finally:
                self._client = None

    # context manager
    # when entering create a client 
    async def __aenter__(self):
        if self._client is None:
            self._client = redis.from_url(self.redis_url)
            try:
                await self._client.ping()
            except Exception:
                await self.close_client()
                raise
        return self
    # when exiting close the client
    async def __aexit__(self):
        await self.close_client()
    
    # Delete pre-exsisting queue to remove any leftover elements
    async def flush_data(self, q_name: str) -> None:
        await self._client.delete(q_name)

    # push data into the queue
    async def push_data(self, q_name: str, data: Any) -> None:
        payload = self._serializer(data)
        await self._client.lpush(q_name, payload)

    # pop data from the queue
    async def pop_data(self, q_name: str) -> Any:
        raw: bytes = await self._client.rpop(q_name)
        return self._deserializer(raw)

    




