import asyncpg
import asyncio
import time
from .get_logger import make_logger

task_logger = make_logger()

async def make_table(uri, query)-> None:
    try:
        conn = await asyncpg.connect(uri)
        await conn.execute(query)
        print("Table created successfully!")
    except Exception as e:
        print("An error occured!!", e)
    finally:
        await conn.close()

async def Writer(id_:int, data: tuple[tuple], pool: asyncpg.pool) -> None:
    """Each producer inserts multiple rows asynchronously."""
    start_time = time.perf_counter()
    async with pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO gmail_payload(Id, Recieved_date, Subject, Payload) VALUES($1, $2, $3, $4);""", data)
    task_logger.info(f"[Writer - {id_}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")

async def main(uri: str, data: list[tuple[tuple]])-> None:
    try:
        pool = await asyncpg.create_pool(uri, min_size=2, max_size=10)
        try:
            producers = [asyncio.create_task(Writer(idx, tup, pool)) for idx, tup in enumerate(data)]
            await asyncio.gather(*producers)
        finally:
            await pool.close()
    except Exception as e:
        task_logger.error(f"Error occured >> {e}")

