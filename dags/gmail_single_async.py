import os, logging
from datetime import datetime, date
from tempfile import NamedTemporaryFile
from airflow.sdk import dag, task, chain, Variable
from airflow.exceptions import AirflowSkipException

# Logger for logging the details
task_logger = logging.getLogger("airflow.task")


@dag(start_date=datetime(2025,8,26), schedule="@weekly", catchup=True)
def gmail_etl_single_async() -> None:        
    @task
    def get_ids() -> list[str]:
        """ 
            Gets the Email ids of last 7 days. 
        """
        from utils.gm_single_utils import get_dates, async_get_ids, wrapper_get_single_main        
        token_path = Variable.get("TOKEN_PATH")
        
        from_date, num_of_days = date.today(), 7
        dates = get_dates(from_date, num_of_days)
        ids_list = wrapper_get_single_main(func = async_get_ids,
                                           a_list = dates,
                                           token_path = token_path,
                                           coro_num = 7,
                                           for_ids = True) 
        return ids_list
       
    @task(multiple_outputs=True)
    def get_payload(ids_list: list[str]) -> str:
        """
            Gets the Email Payload of last 7 days.
        """
        import gzip, msgspec
        from utils.gm_single_utils import async_get_payload, wrapper_get_single_main        
        token_path = Variable.get("TOKEN_PATH")

        out_dict = wrapper_get_single_main(func = async_get_payload,
                                           a_list = ids_list,
                                           token_path = token_path,
                                           coro_num = 25,
                                           for_ids = False)

        task_logger.info("Starting encoding")
        # Compressing
        bytes = msgspec.msgpack.encode(out_dict)        
        zip_path = f"/opt/airflow/data/{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.json.gz"
        with gzip.open(zip_path, 'wb') as f:
            f.write(bytes)            
        return {"path":zip_path, "ids":out_dict["Id"]}   
    
    @task
    def decode_payload(zip_path: str) -> str:
        """
            Decodes the payload to text.
        """
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
        """
            Generates the embeddings of the cleaned text.
        """
        import torch
        import pandas as pd
        from utils.gm_single_utils import get_embeddings
        try:
            df = pd.read_parquet(parquet_path)       # Reading and deleting temp file.
            os.remove(parquet_path)
            task_logger.info(f"Temporary file deleted.")
        except Exception as e:
            task_logger.info(f"Error deleting file--{e}")

        model_name = "distilbert-base-uncased"
        embd = get_embeddings(df, model_name)     # Generates Embeddings of Subject and Body text data.

        with NamedTemporaryFile(delete=False, suffix=".npt") as f: # Saving as tempfile
            torch.save(embd, f)
            return f.name

    @task
    def predict(embd_path: str, ids: list[str]) -> list[str]:
        """
            Predict the unimportant email ids.
        """
        import torch, gc
        import numpy as np
        import xgboost as xgb

        try:
            embd = torch.load(embd_path)
            os.remove(embd_path)
            task_logger.info(f"Temporary file deleted.")
        except Exception as e:
            task_logger.error(f"Error deleting file--{e}")
        
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
        task_logger.info(f"Number of Ids to be trashed >>> {num}")
        chunk_length = 50
        rem = num%chunk_length                                  
        if rem == 0:
            id_chunks = del_ids.reshape(-1, chunk_length).tolist() # if len%100!= 0 raises error, So..
        else:
            id_chunks = del_ids[:-rem].reshape(-1, chunk_length).tolist()
            id_chunks.extend([del_ids[-rem:].tolist()]) 

        del embd
        del dpred
        del model
        gc.collect()
        torch.cuda.empty_cache()
        
        return id_chunks
    
    @task
    def trash_ids(ids_list: list[list[str]]) -> None :
        """
            Move the umimp ids to the trash.
        """
        from utils.gm_trash_utils import batch_modify
        token_path = Variable.get("TOKEN_PATH")
        
        batch_modify(ids_list, token_path)


    @task
    def get_prev_trash_ids(dag_run=None):
        """
            Get the ids the emails already in trash.
            Raise skip if run is not scheduler-triggered
        """
        run_type = dag_run.run_type  # "scheduled", "manual", "backfill"
        task_logger.info(run_type)
        if run_type != "scheduled":
            raise AirflowSkipException(f"Skipping run_type={run_type}")
            
        else:
            """Importing libraries/functions."""
            from utils.gm_single_utils import get_ids_in_trash
            token_path = Variable.get("TOKEN_PATH")

            return get_ids_in_trash(token_path)

    @task
    def perma_del_trash(ids_list: list[str]):
        """
            Permanently delete the ids in trash.
        """
        from utils.gm_single_utils import batch_del_ids_in_trash
        token_path = Variable.get("TOKEN_PATH")

        batch_del_ids_in_trash(ids_list=ids_list, token_path=token_path,)
        return 

    _my_task_1 = get_ids()
    _my_task_2 = get_payload(ids_list = _my_task_1)           
    _my_task_3 = decode_payload(zip_path = _my_task_2["path"])
    _my_task_4 = get_embeds(parquet_path = _my_task_3)
    _my_task_5 = predict(embd_path = _my_task_4, ids = _my_task_2["ids"])
    _my_task_6 = trash_ids.expand(ids_list = _my_task_5)
    _my_task_7 = get_prev_trash_ids()
    _my_task_8 = perma_del_trash.expand(ids_list = _my_task_7)

    chain(
    _my_task_1,
    _my_task_2,
    _my_task_3,
    _my_task_4,
    _my_task_5,
    _my_task_6
    )

    chain(
        _my_task_7,
        _my_task_8,
    )
    
gmail_etl_single_async()
