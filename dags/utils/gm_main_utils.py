import time, asyncio, gc
from functools import partial
from collections import defaultdict


""" Helper functions for ASYNC DAG """

def get_dates() -> list[tuple]:
    from datetime import datetime, timedelta
    today = datetime.now().date()
    ranges = []
    for i in range(1, 10):
        after_date = (today - timedelta(days=i)).strftime("%Y/%m/%d")
        before_date = (today - timedelta(days=i - 1)).strftime("%Y/%m/%d")
        ranges.append((after_date, before_date))
    return ranges
    
#For creating multiple coroutines/tasks with different arguments
async def worker(id: int, function, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None :
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        res = await function(num)
        await out_queue.put(res)
        in_queue.task_done()

#Generates service for each coroutine
def generate_services(num: int, token_path: str) -> list:
    """Importing libraries. """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds) for _ in range(num)]
    return services

""" service is synchronous method, wrapper to make it asynchronous. """
def driver_for_ids(service, date: tuple):
    return service.users().messages().list(userId='me', q=f"after:{date[0]} before:{date[1]}").execute()

""" Returns list of ids for a given date range. """
async def async_get_ids(service, date: tuple) -> list[str]:
    results = await asyncio.to_thread(driver_for_ids, service, date) #for type list of dictionaries(having id, thread id)
    return [dict_["id"]  for dict_ in results.get('messages', [])]

""" Main function to create multiple coroutines,
    Returns a list with ids. """
async def async_get_ids_main(dates: list[tuple], token_path: str, coro_num: int, id_list: list = None) -> list[str]:
    services = generate_services(coro_num, token_path)
    partial_functions = [partial(async_get_ids, service) for service in services]
    
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    if id_list is None: id_list = []
    
    for date in dates:
        await in_queue.put(date)

    # Add one stop signal per worker. 
    # i,e when the coroutine/worker gets None it breaks the loop of accepting/getting dates.
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(id+1, function, in_queue, out_queue)) for id, function in enumerate(partial_functions)]    
    await asyncio.gather(*tasks)
        
    while not out_queue.empty():
        id_list.extend(await out_queue.get())        
    print(f"Fetched >>>> {len(id_list)} Ids")   
    return id_list

# Wrapper for main function for airflow.
def wrapper_for_ids(dates: list[tuple], token_path: str, coro_num: int) -> list[str]:
    return asyncio.run(async_get_ids_main(dates=dates, token_path=token_path, coro_num=coro_num))

""" Helper functions for getting payload. """

""" service is synchronous method, wrapper to make it asynchronous. """
def driver_for_payload(service, message_id: str):
    return service.users().messages().get(userId="me", id=message_id, format="full").execute()

""" Returns tuple of id, payload for a given id. """
async def async_get_payload(service, message_id: str):    
    results = await asyncio.to_thread(driver_for_payload, service, message_id)
    return (message_id, results.get("payload", {}))

""" Main function to create multiple coroutines,
    Returns a dictionary with ids, payload. """
async def async_get_payload_main(id_list: list[str], token_path: str, coro_num: int) -> dict:
    services = generate_services(coro_num, token_path)
    partial_functions = [partial(async_get_payload, service) for service in services]
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    out_dict = defaultdict(list)                       
    
    for date in id_list:
        await in_queue.put(date)
        
    # Add one stop signal per worker. 
    # i,e when the coroutine/worker gets None it breaks the loop of accepting/getting ids.
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(id+1, function, in_queue, out_queue)) for id, function in enumerate(partial_functions)]
    await asyncio.gather(*tasks)
        
    while not out_queue.empty():
        val = await out_queue.get()
        out_dict['Id'].append(val[0])
        out_dict['Payload'].append(val[1])    
    print("Succesfully fetched Payload")                
    return out_dict

# Wrapper for main function for airflow
def wrapper_for_payload(id_list: list[str], token_path: str, coro_num: int) -> dict:
    return asyncio.run(async_get_payload_main(id_list, token_path, coro_num))

"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Helper functions for Training Model """

def get_embeddings(df, model_name: str):
    """Importing libraries."""
    import torch
    from tempfile import NamedTemporaryFile
    from transformers import AutoModel, AutoTokenizer
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
        
    sub_list = df.loc[:, "Subject"].astype(str).tolist() # turns object to string and returns list of strs
    body_list = df.loc[:, "Body"].astype(str).tolist()
    
    """ Tokenizer takes input list[str], list[list[str]], not just str!!! """
    sub_tokenized = tokenizer(sub_list, truncation=True, max_length=30, padding=True, return_tensors="pt")
    body_tokenized = tokenizer(body_list, truncation=True, max_length=512, padding=True, return_tensors="pt")
    
    if torch.cuda.is_available():
        model.cuda()
        sub_tokenized = {k: v.cuda() for k, v in sub_tokenized.items()}
        body_tokenized = {k: v.cuda() for k, v in body_tokenized.items()}

    with torch.no_grad():
        sub_outputs = model(**sub_tokenized)
        body_outputs = model(**body_tokenized)

        sub_cls_embeddings_t = sub_outputs.last_hidden_state[:, 0, :]
        body_cls_embeddings_t = body_outputs.last_hidden_state[:, 0, :]
        # removes gradient updation
        embd = torch.cat((sub_cls_embeddings_t, body_cls_embeddings_t), 1).detach()
        
    del sub_tokenized
    del body_tokenized
    del model
    del tokenizer

    gc.collect()
    torch.cuda.empty_cache()

    return embd

