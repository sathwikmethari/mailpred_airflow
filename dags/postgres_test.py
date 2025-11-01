import asyncpg, asyncio
from airflow.sdk import dag, task, Variable
from utils.get_logger import make_logger

task_logger = make_logger() 

# async def producer(name: str, pool) -> None:
#     """Each producer inserts multiple rows asynchronously.""" 
#     async with pool.acquire() as conn:
#         tup = [(f"{name}-{random.randint(100, 10000000)}", f'Hello there-{id}', b'testing', datetime.date(1984, 3, 1))
#                    for id in range(5)]
#         await conn.executemany('''
#             INSERT INTO payload(mail_id, subject, payload, created_at) VALUES($1, $2, $3, $4)''', tup)
        
#         print(f"✅ [Producer-{name}]-inserted: {name}")
#         await asyncio.sleep(random.uniform(0.1, 0.5))  # simulate work

# async def main(db_uri)-> None:
#     pool = await asyncpg.create_pool(db_uri, min_size=2, max_size=10)
#     try:
#         producers = [asyncio.create_task(producer(f"{i}", pool)) for i in range(3)]
#         await asyncio.gather(*producers)
#     finally:
#         await pool.close()

async def printrows(uri)-> None:
    try:
        conn = await asyncpg.connect(uri)           
        rows = await conn.fetch('SELECT * FROM gmail_payload')
        # await conn.execute("DELETE FROM payload;")
        # for ele in rows[:10]:
        #     task_logger.info(f"Row: {dict(ele)}")
        task_logger.info(f"Row: {len(rows)}")
    except asyncpg.PostgresError as e:
        task_logger.error(f"Database error: {e}")
        # await conn.close
    finally:
        await conn.close()

@dag
def postgres_test():

    @task
    def post_test_task():
        uri = Variable.get("PG_URI")
        asyncio.run(printrows(uri))
    post_test_task()

postgres_test()
       