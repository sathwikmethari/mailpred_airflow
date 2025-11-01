import asyncio, os
from dags.utils.postgres_utils import make_table
from dotenv import load_dotenv
load_dotenv()

query = """CREATE TABLE IF NOT EXISTS gmail_payload (
        Id VARCHAR(16) PRIMARY KEY,
        Recieved_date TIMESTAMPTZ,
        Subject TEXT,
        Payload BYTEA );"""

# query2 = "DROP TABLE gmail_payload;"
if __name__ == "__main__":
    uri = os.getenv("DB_URI")
    asyncio.run(make_table(uri, query=query))