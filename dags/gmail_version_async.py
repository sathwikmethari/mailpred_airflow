from airflow.sdk import dag, task, chain
import os, collections, gzip, time, asyncio
from datetime import  datetime, timedelta
from functools import partial

@dag
def gmail_dag_async():
    @task
    def get_dates() -> list[tuple]:
        today = datetime.now().date()
        ranges = []
        for i in range(1, 20):
            after_date = today - timedelta(days=i)
            before_date = today - timedelta(days=i - 1)
            ranges.append((after_date.strftime("%Y/%m/%d"), before_date.strftime("%Y/%m/%d")))
        return ranges
    
    _my_task_1 = get_dates()
    
    @task
    def get_ids(dates: list[tuple]) -> list[dict]:
        """Importing libraries/functions."""
        from utils import wrapper_for_ids
        
        token_path = os.environ.get("token_path_airflow")
       
        x = wrapper_for_ids(dates, token_path)
        return x
       
    _my_task_2 = get_ids(_my_task_1)
    
        
        
    chain(
    _my_task_1,
    _my_task_2)
    
gmail_dag_async()