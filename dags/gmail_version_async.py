import os, gzip, msgspec
from airflow.sdk import dag, task, chain
from datetime import  datetime, timedelta

@dag
def get_gmail_data_async():
    @task
    def get_dates() -> list[tuple]:
        today = datetime.now().date()
        ranges = []
        for i in range(1, 10):
            after_date = today - timedelta(days=i)
            before_date = today - timedelta(days=i - 1)
            ranges.append((after_date.strftime("%Y/%m/%d"), before_date.strftime("%Y/%m/%d")))
        return ranges
    
    _my_task_1 = get_dates()
    
    @task
    def get_ids(dates: list[tuple]) -> list[str]:
        """Importing libraries/functions/paths."""
        from utils import wrapper_for_ids
        token_path = os.environ.get("token_path_airflow")
       
        x = wrapper_for_ids(dates, token_path)
        return x
       
    _my_task_2 = get_ids(_my_task_1)
    
    @task
    def get_payload(ids_list: list[str]) -> str:
        """Importing libraries/functions/paths."""
        from utils import wrapper_for_payload        
        token_path = os.environ.get("token_path_airflow")
               
        out_dict = wrapper_for_payload(ids_list, token_path)

        print("Starting encoding")
        # Compressing
        bytes = msgspec.msgpack.encode(out_dict)        
        zip_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M')}.json.gz"
        with gzip.open(zip_path, 'wb') as f:
            f.write(bytes)
            
        return zip_path
    
    _my_task_3 = get_payload(_my_task_2)           
    
    @task
    def parse_payload(zip_path: str) -> None:
        """Importing libraries/functions."""
        pass       
                               
    _my_task_4 = parse_payload(_my_task_3)
    
    
    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4)
    
get_gmail_data_async()