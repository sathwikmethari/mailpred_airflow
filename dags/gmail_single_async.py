import os
from tempfile import NamedTemporaryFile
from airflow.sdk import dag, task, chain

@dag
def gmail_etl_single_async() -> None:        
    @task
    def get_ids() -> list[str]:
        """Importing libraries/functions/paths."""
        from datetime import date
        from utils.gm_main_utils import get_dates, wrapper_for_ids        
        token_path = os.environ.get("token_path_airflow")
        
        from_date, num_of_days = date.today(), 10
        dates = get_dates(from_date, num_of_days)
        x = wrapper_for_ids(dates, token_path, num_of_days) # 1 coro per day
        return x
       
    @task(multiple_outputs=True)
    def get_payload(ids_list: list[str]) -> str:
        """Importing libraries/functions/paths."""
        import gzip, msgspec
        from datetime import  datetime
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
        
        with NamedTemporaryFile(delete=False, suffix=".parquet.gzip") as f:
            df.to_parquet(f)
            return f.name        
                                  
    @task
    def get_embeds(parquet_path: str):
        """Importing libraries/functions."""
        import torch
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

        with NamedTemporaryFile(delete=False, suffix=".npt") as f:
            torch.save(embd, f)
            return f.name

    @task
    def predict(embd_path: str, ids: list[str]) -> None:
        """Importing libraries/functions."""
        import torch
        import numpy as np
        import xgboost as xgb

        try:
            embd = torch.load(embd_path)
            os.remove(embd_path)
            print(f"Temporary file deleted.")
        except Exception as e:
            print(f"Error deleting file--{e}")
        
        model = xgb.Booster()
        model.load_model("/opt/airflow/data/XGBmodel.json")
        dpred = xgb.DMatrix(embd)
        pred_proba = model.predict(dpred)
        pred_binary = (pred_proba > 0.35)

        ids = np.array(ids)
        ids = np.dstack((ids, pred_binary)).squeeze()
        
        del_ids = ids[ids[:,1]=="True"][:,0]
        print(f"example array {del_ids[:5][:]}")
        return del_ids.tolist()

    _my_task_1 = get_ids()
    _my_task_2 = get_payload(_my_task_1)           
    _my_task_3 = decode_payload(zip_path = _my_task_2["path"])
    _my_task_4 = get_embeds(_my_task_3)
    _my_task_5 = predict(embd_path = _my_task_4, ids = _my_task_2["ids"])


    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4,
    _my_task_5,)
    
gmail_etl_single_async()

# Yesterday's(23/08) run took 41 sec now 25!!