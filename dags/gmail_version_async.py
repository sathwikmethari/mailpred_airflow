from tempfile import NamedTemporaryFile
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
        
    @task
    def get_ids(dates: list[tuple]) -> list[str]:
        """Importing libraries/functions/paths."""
        import os
        from utils.gm_main_utils import wrapper_for_ids
        token_path = os.environ.get("token_path_airflow")
       
        x = wrapper_for_ids(dates, token_path, 10)
        return x
       
    @task(multiple_outputs=True)
    def get_payload(ids_list: list[str]) -> str:
        """Importing libraries/functions/paths."""
        import os, gzip, msgspec
        from utils.gm_main_utils import wrapper_for_payload        
        token_path = os.environ.get("token_path_airflow")
               
        out_dict = wrapper_for_payload(ids_list, token_path, 20)

        print("Starting encoding")
        # Compressing
        bytes = msgspec.msgpack.encode(out_dict)        
        zip_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.json.gz"
        with gzip.open(zip_path, 'wb') as f:
            f.write(bytes)
            
        return {"path":zip_path, "ids":out_dict["Id"]}   
    
    @task
    def decode_payload(zip_path: str) -> str:
        """Importing libraries/functions."""
        import pandas as pd
        from utils.gm_data_utils import decode_zip, extract_headers, decode_body

        unzipped_data = decode_zip(zip_path)
        df = pd.DataFrame(unzipped_data)[["Payload"]] #only takes payload key data from dict
        df["Subject"] = df["Payload"].apply(extract_headers)
        df["Body"] = df["Payload"].apply(decode_body)
        df = df.drop(["Payload"], axis=1)
        #parquet_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.parquet.gzip"
        
        with NamedTemporaryFile(delete=False, suffix=".parquet.gzip") as f:
            df.to_parquet(f)
            return f.name        
                                  
    @task
    def get_embeds(parquet_path: str):
        """Importing libraries/functions."""
        import os
        import numpy as np
        import pandas as pd
        from utils.gm_main_utils import get_embeddings
        try:
            df = pd.read_parquet(parquet_path)
            os.remove(parquet_path)
            print(f"Temporary file deleted.")
        except Exception as e:
            print(f"Error deleting file--{e}")

        model_name = "distilbert-base-uncased"
        embd = get_embeddings(df, model_name)

        with NamedTemporaryFile(delete=False, suffix=".npy") as f:
            np.save(f, embd)
            return f.name

    @task
    def test(x,y):
        import os
        import numpy as np
        try:
            embd = np.load(x)
            os.remove(x)
            print(f"Temporary file deleted.")
        except Exception as e:
            print(f"Error deleting file--{e}")
        
        print(f"EMBDS---{embd[0][0]}")
        print(f"IDS-----{y[0]}")

    _my_task_1 = get_dates()
    _my_task_2 = get_ids(_my_task_1)
    _my_task_3 = get_payload(_my_task_2)           
    _my_task_4 = decode_payload(zip_path = _my_task_3["path"])
    _my_task_5 = get_embeds(_my_task_4)
    _my_task_6 = test(x = _my_task_5, y = _my_task_3["ids"])


    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4,
    _my_task_5,
    _my_task_6)
    
get_gmail_data_async()

# Yesterday's run took 41 sec now 25!!