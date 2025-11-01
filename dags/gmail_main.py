import asyncio
from datetime import datetime, date
from airflow.sdk import dag, task, chain, Variable
from airflow.exceptions import DownstreamTasksSkipped
from airflow.utils.types import DagRunType
from utils.redis_utils import *
from utils.get_logger import make_logger
from utils.encode_utils import Serialize, Deserialize

task_logger = make_logger()
MAIN_Q = "AIRFLOW_Q"
POST_Q = "POSTGRES_Q"
ID_Q  = "ID_Q"
PREV_Q = "PREV_Q"

@dag(start_date=datetime(2025,8,26), schedule="@weekly", catchup=True)
def gmail_redis_intermediate(token_path: str = Variable.get("TOKEN_PATH"), redis_url: str = Variable.get("REDIS_URI")) -> None:        
    @task
    def get_ids(token_path: str, redis_url: str) -> None:
        """ 
            Gets the Email ids of last 7 days. 
        """
        from utils.main_utils import get_dates, async_get_ids_main       
        
        from_date, num_of_days = date.today(), 7
        dates = get_dates(from_date, num_of_days)
        ids_list = asyncio.run(async_get_ids_main(date_list = dates,
                                                  token_path = token_path,
                                                  coro_num = 7))
        ids_list_enc = Serialize(ids_list)
        # Flushes any prexisting elements.
        asyncio.run(flush_queue(redis_url, MAIN_Q))
        asyncio.run(push_data(redis_url, ids_list_enc, MAIN_Q))

        asyncio.run(flush_queue(redis_url, ID_Q))
        asyncio.run(push_data(redis_url, ids_list_enc, ID_Q))
       
    @task
    def get_payload(token_path: str, redis_url: str) -> None:
        """
            Gets the Email Payload of last 7 days.
        """
        from utils.main_utils import async_get_paylaod_main     

        ids_list: bytes = asyncio.run(pop_data(redis_url, MAIN_Q))
        ids_list: list[str] = Deserialize(ids_list)

        out_dict = asyncio.run(async_get_paylaod_main(ids_list = ids_list,
                                                      token_path = token_path,
                                                      coro_num = 25,))

        task_logger.info("Starting encoding")
        # Compressing
        out_dict: bytes = Serialize(out_dict)
        asyncio.run(push_data(redis_url, out_dict, MAIN_Q))

    @task
    def decode_payload(redis_url: str, dag_run=None) -> None:
        """
            Decodes the payload to text and sends them to be saved and to generate embeddings.
        """
        import pandas as pd
        from utils.payload_utils import decode_gmail_payload

        out_dict: bytes = asyncio.run(pop_data(redis_url, MAIN_Q))
        out_dict: dict = Deserialize(out_dict)
        
        df = pd.DataFrame(out_dict)
        # df = pd.DataFrame(unzipped_data)[["Payload"]]           #only takes Payload key data from dict
        df[["Date", "Subject", "Body"]] = df["Payload"].apply(lambda row: pd.Series(decode_gmail_payload(row)))
        
        # for embeddings
        df_for_embed = df.drop(["Id", "Date", "Payload"], axis=1)
        encoded_df: bytes = Serialize(df_for_embed.to_dict(orient="list")) # preserves {col:[values],...}
        asyncio.run(push_data(redis_url, encoded_df, MAIN_Q))
        
        # for saving
        run_type = dag_run.run_type 
        if run_type == DagRunType.MANUAL:
            tasks_to_be_skipped = ["write_to_PG"]
            task_logger.info(f"Skipping Tasks >> run_type = {run_type}")   
            task_logger.info(f"Skipped Tasks >> {tasks_to_be_skipped}")            
            raise DownstreamTasksSkipped(tasks = tasks_to_be_skipped) # skips downstream task
        else:            
            r = len(df)
            df["Payload"] =df["Payload"].apply(lambda row: Serialize(row))
            # reordering columns
            df = df[["Id", "Date", "Subject", "Body", "Payload"]]
            chunk_size = 50 if r>50 else r
            df = df.drop(["Body"], axis=1)
            chunks: list[tuple] = [tuple(df.iloc[i:i+chunk_size].itertuples(index=False, name=None))
                                        for i in range(0, r, chunk_size)]
            chunks: bytes = Serialize(chunks)
            asyncio.run(flush_queue(redis_url, POST_Q))
            asyncio.run(push_data(redis_url, chunks, POST_Q))
      
    # # Write pushing data to postgres here
    @task
    def write_to_PG(redis_url: str) -> None:
        """
            Pushes Ids, Date, Subject, Gmail payload to PostGres Database.
        """
        from utils.postgres_utils import main
    
        chunks: bytes = asyncio.run(pop_data(redis_url, POST_Q))
        chunks: list[tuple[tuple]] = Deserialize(chunks)
        
        pg_uri = Variable.get("PG_URI")
        try:
            asyncio.run(main(pg_uri, chunks))
            task_logger.info("Successfully inserted.")
        except Exception as e:
            task_logger.error(e)  
                                  
    @task
    def get_embeds(redis_url: str) -> None:
        """
            Generates the embeddings of the cleaned text.
        """
        import torch, io
        import pandas as pd
        from utils.embedding_utils import get_embeddings

        encoded_dict: bytes = asyncio.run(pop_data(redis_url, MAIN_Q))
        decoded_dict: dict = Deserialize(encoded_dict)
        df = pd.DataFrame(decoded_dict)

        model_name = "distilbert-base-uncased"
        embd = get_embeddings(df, model_name)     # Generates Embeddings of Subject and Body text data.
        
        buffer = io.BytesIO()
        torch.save(embd, buffer)
        embd: bytes = buffer.getvalue()
        asyncio.run(push_data(redis_url, embd, MAIN_Q))

    @task
    def predict(redis_url: str) -> list[str]:
        """
            Predict the unimportant email ids.
        """
        import torch, gc, io
        import numpy as np
        import xgboost as xgb

        encoded_ids: bytes = asyncio.run(pop_data(redis_url, ID_Q))
        encoded_embs: bytes = asyncio.run(pop_data(redis_url, MAIN_Q))
        
        ids: list[str] = Deserialize(encoded_ids)
        buffer = io.BytesIO(encoded_embs)
        # Deserialize tensor
        embd = torch.load(buffer)
        
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

        id_chunks: bytes = Serialize(id_chunks)
        asyncio.run(push_data(redis_url, id_chunks, MAIN_Q))
    
    @task
    def trash_ids(token_path: str, redis_url: str) -> None :
        """
            Move the umimp ids to the trash.
        """
        from utils.main_utils import modify_trash_main

        encoded_ids: bytes = asyncio.run(pop_data(redis_url, MAIN_Q))
        ids_list: list[list[str]] = Deserialize(encoded_ids)
        
        asyncio.run(modify_trash_main(token_path, ids_list))

    @task
    def get_prev_trash_ids(token_path: str, redis_url: str, dag_run=None):
        """
            Get the ids the emails already in trash.
            Raise skip if run is not scheduler-triggered
        """
        run_type = dag_run.run_type 
        if run_type == DagRunType.MANUAL:
            task_logger.info(f"Skipping Tasks >> run_type - {run_type}")            
            raise DownstreamTasksSkipped(tasks = ["perma_del_trash"]) # skips downstream task
        else:     
            from utils.main_utils import get_ids_in_trash
            id_chunks: list[list[str]] = get_ids_in_trash(token_path)
            id_chunks: bytes = Serialize(id_chunks)
            asyncio.run(push_data(redis_url, id_chunks, PREV_Q))

    @task
    def perma_del_trash(token_path: str, redis_url: str) -> None:
        """
            Permanently delete the ids in trash.
        """
        encoded_chunks: bytes = asyncio.run(pop_data(redis_url, PREV_Q))
        id_chunks: list[list[str]] = Deserialize(encoded_chunks)

        from utils.main_utils import batch_perma_del_main
        asyncio.run(batch_perma_del_main(token_path=token_path, chunks=id_chunks))

    _my_task_1 = get_ids(token_path=token_path, redis_url=redis_url)
    _my_task_2 = get_payload(token_path=token_path, redis_url=redis_url)
    _my_task_3 = decode_payload(redis_url=redis_url)
    _my_task_4 = write_to_PG(redis_url=redis_url)
    _my_task_5 = get_embeds(redis_url=redis_url)
    _my_task_6 = predict(redis_url=redis_url)
    _my_task_7 = trash_ids(token_path=token_path, redis_url=redis_url)
    _my_task_8 = get_prev_trash_ids(token_path=token_path, redis_url=redis_url)
    _my_task_9 = perma_del_trash(token_path = token_path, redis_url=redis_url)
    
    chain(
        _my_task_1,
        _my_task_2,
        _my_task_3,
        _my_task_5,
        _my_task_6,
        _my_task_7,
    )
    chain(
        _my_task_1, 
        _my_task_5
    )
    chain(
        _my_task_3,
        _my_task_4,
    )
    chain(
        _my_task_8,
        _my_task_9,
    )

gmail_redis_intermediate()
