import os
from tempfile import NamedTemporaryFile
from airflow.sdk import dag, task, chain

@dag
def gmail_etl_single_async() -> None:        
    @task
    def get_ids() -> list[str]:
        """Importing libraries/functions/paths."""
        from datetime import date
        from utils.gm_single_utils import get_dates, async_get_ids, wrapper_get_single_main        
        token_path = os.environ.get("token_path_airflow")
        
        from_date, num_of_days = date.today(), 10
        dates = get_dates(from_date, num_of_days)
        ids_list = wrapper_get_single_main(func = async_get_ids,
                                    a_list = dates,
                                    token_path = token_path,
                                    coro_num = 10,
                                    for_ids = True) 
        return ids_list
       
    @task(multiple_outputs=True)
    def get_payload(ids_list: list[str]) -> str:
        """Importing libraries/functions/paths."""
        import gzip, msgspec
        from datetime import  datetime
        from utils.gm_single_utils import async_get_payload, wrapper_get_single_main        
        token_path = os.environ.get("token_path_airflow")

        out_dict = wrapper_get_single_main(func = async_get_payload,
                                           a_list = ids_list,
                                           token_path = token_path,
                                           coro_num = 40,
                                           for_ids = False)

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
        df = pd.DataFrame(unzipped_data)[["Payload"]]           #only takes Payload key data from dict
        df["Subject"] = df["Payload"].apply(extract_headers)    # Gets subject data from Payload
        df["Body"] = df["Payload"].apply(decode_body)           # Gets Body data from Payload
        df = df.drop(["Payload"], axis=1)
        
        with NamedTemporaryFile(delete=False, suffix=".parquet.gzip") as f:  # Saving as temp file
            df.to_parquet(f)
            return f.name        
                                  
    @task
    def get_embeds(parquet_path: str) -> str:
        """Importing libraries/functions."""
        import torch
        import pandas as pd
        from utils.gm_single_utils import get_embeddings
        try:
            df = pd.read_parquet(parquet_path)       # Reading and deleting temp file.
            os.remove(parquet_path)
            print(f"Temporary file deleted.")
        except Exception as e:
            print(f"Error deleting file--{e}")

        model_name = "distilbert-base-uncased"
        embd = get_embeddings(df, model_name)     # Generates Embeddings of Subject and Body text data.

        with NamedTemporaryFile(delete=False, suffix=".npt") as f: # Saving as tempfile
            torch.save(embd, f)
            return f.name

    @task
    def predict(embd_path: str, ids: list[str]) -> list[str]:
        """Importing libraries/functions."""
        import torch, gc
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
        thresh = 0.35
        pred_binary = (pred_proba > thresh)  # If above threshold it is considered as Imp email!
                                             # Decrease thresh value to minimize False Negatives.
        ids = np.array(ids)
        ids = np.dstack((ids, pred_binary)).squeeze()
        
        del_ids = ids[ids[:,1]=="False"][:,0]
        num = len(del_ids)
        print(f"Number of Ids to be trashed/deleted >>> {num}")
        rem = num%25                                  
        if rem == 0:
            id_chunks = del_ids.reshape(-1, 25).tolist() # if len%25!= 0 raises error, So..
        else:
            id_chunks = del_ids[:-rem].reshape(-1, 25).tolist()
            id_chunks.extend([del_ids[-rem:].tolist()]) 

        del embd
        del dpred
        del model
        gc.collect()
        torch.cuda.empty_cache()
        
        return id_chunks
    
    @task
    def trash_ids(id_chunks: list[list[str]]) -> None :
        """Importing libraries/functions."""
        from utils.gm_trash_utils import wrapper_for_batch_trash_main
        token_path = os.environ.get("token_path_airflow")
        
        wrapper_for_batch_trash_main(id_chunks, token_path,)

    _my_task_1 = get_ids()
    _my_task_2 = get_payload(_my_task_1)           
    _my_task_3 = decode_payload(zip_path = _my_task_2["path"])
    _my_task_4 = get_embeds(_my_task_3)
    _my_task_5 = predict(embd_path = _my_task_4, ids = _my_task_2["ids"])
    _my_task_6 = trash_ids(_my_task_5)


    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4,
    _my_task_5,
    _my_task_6)
    
gmail_etl_single_async()
