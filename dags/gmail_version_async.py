import os, gzip, msgspec
from airflow.sdk import dag, task, chain
from datetime import  datetime, timedelta

@dag
def get_gmail_data_async() -> None:
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
       
        x = wrapper_for_ids(dates, token_path, 10)
        return x
       
    _my_task_2 = get_ids(_my_task_1)
    
    @task
    def get_payload(ids_list: list[str]) -> str:
        """Importing libraries/functions/paths."""
        from utils import wrapper_for_payload        
        token_path = os.environ.get("token_path_airflow")
               
        out_dict = wrapper_for_payload(ids_list, token_path, 15)

        print("Starting encoding")
        # Compressing
        bytes = msgspec.msgpack.encode(out_dict)        
        zip_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.json.gz"
        with gzip.open(zip_path, 'wb') as f:
            f.write(bytes)
            
        return zip_path
    
    _my_task_3 = get_payload(_my_task_2)           
    
    @task
    def decode_payload(zip_path: str) -> str:
        """Importing libraries/functions."""
        import pandas as pd
        from utils import decode_zip, extract_headers, decode_body

        unzipped_data = decode_zip(zip_path)
        df = pd.DataFrame(unzipped_data)
        df["Subject"] = df["Payload"].apply(extract_headers)
        df["Body"] = df["Payload"].apply(decode_body)
        df = df.drop(["Id", "Payload"], axis=1)

        parquet_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.parquet.gzip"

        df.to_parquet(parquet_path)
        return parquet_path       
                               
    _my_task_4 = decode_payload(_my_task_3)
    
    
    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4)
    
get_gmail_data_async()