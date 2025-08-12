from airflow.sdk import dag, task, chain
import os, time, threading, collections, json, gzip
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from datetime import  datetime, timedelta
from functools import partial

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
        """Importing libraries/functions."""
        from utils import get_ids_gmail
        token_path = os.environ.get("token_path_airflow")

        ids_queue = Queue()   
        partial_function_1 = partial(get_ids_gmail, token_path, ids_queue)

        with ThreadPoolExecutor(max_workers=7) as executor:   #multiple workers for faster calling/extracting
            executor.map(partial_function_1, dates)

        return [ids_queue.get() for _ in range(ids_queue.qsize())]
    
    _my_task_2 = threaded_get_ids(_my_task_1)
    
    
    @task
    def threaded_get_payload(messages_ids) -> list[tuple]:
        """Importing libraries/functions."""
        from utils import get_payload
        
        token_path = os.environ.get("token_path_airflow")
        
        payload_queue = Queue()
        partial_function_2 = partial(get_payload, token_path, payload_queue)

        with ThreadPoolExecutor(max_workers=7) as executor:   #multiple workers for faster calling/extracting
            executor.map(partial_function_2, messages_ids)
                
        return [payload_queue.get() for _ in range(payload_queue.qsize())]
    _my_task_3 = threaded_get_payload(_my_task_2)
    
    @task
    def worker(payload_list) -> None:
        out_dict = collections.defaultdict(list)
        for id, payload in payload_list:
            out_dict['Id'].append(id)
            out_dict['Payload'].append(payload)
        
        # Convert JSON to bytes and compress
        json_bytes = json.dumps(out_dict).encode('utf-8')
        with gzip.open(f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M')}.json.gz", 'wb') as f:
            f.write(json_bytes)
        return
        
    _my_task_4 = worker(_my_task_3)
    
    
    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4
    )
    
gmail_dag()