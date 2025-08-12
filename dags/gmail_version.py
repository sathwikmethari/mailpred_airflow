from airflow.sdk import dag, task, chain
import os, time, threading
from queue import Queue
from datetime import  datetime, timedelta

@dag
def gmail_dag():
    @task
    def get_dates() -> list[tuple]:
        today = datetime.now().date()
        ranges = []
        for i in range(1, 8):
            after_date = today - timedelta(days=i)
            before_date = today - timedelta(days=i - 1)
            ranges.append((after_date.strftime("%Y/%m/%d"), before_date.strftime("%Y/%m/%d")))
        return ranges
    
    _my_task_1 = get_dates()
    
    @task
    def threaded_get_ids(dates: list[tuple]) -> list[dict]:
        """Importing libraries."""
        from utils import get_ids_gmail
        token_path = os.environ.get("token_path_airflow")

        
        threads=[]
        q = Queue()   
        for i, (after, before) in enumerate(dates, start=1):
                t = threading.Thread(target=get_ids_gmail, args=(i, q, token_path, after, before))
                threads.append(t)
                t.start()
                time.sleep(0.1)
                
        for t in threads:
            t.join()
        print("All threads finished.")

        return [q.get() for _ in range(q.qsize())]
    
    _my_task_2 = threaded_get_ids(_my_task_1)
    
    chain(
    _my_task_1,
    _my_task_2,
    )
    
gmail_dag()