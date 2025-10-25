import time, asyncio
from functools import partial
from collections import defaultdict
from .get_logger import make_logger

task_logger = make_logger()

""" Helper functions for ASYNC DAG """

def get_dates(from_date, num_of_days: int) -> list[tuple]:
    """
        Generates date ranges with time-delta-1.
        To get ids per day.
    """
    from datetime import timedelta
    
    ranges = [None]*num_of_days
    for i in range(num_of_days):
        after_date = (from_date - timedelta(days=i+1)).strftime("%Y/%m/%d")
        before_date = (from_date - timedelta(days=i)).strftime("%Y/%m/%d")
        ranges[i] = (after_date, before_date)
    return ranges
    
async def worker(id: int, function, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None :
    """
        Worker function that loops till in_queue is empty/recieves None.
        For creating multiple coroutines/tasks with different arguments.
    """
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            task_logger.info(f"[S-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        res = await function(num)
        await out_queue.put(res)
        in_queue.task_done()


def generate_services(num: int, token_path: str, for_del: bool = False) -> list:
    """
        Generates services to be used for api calls.
        Generates service for each coroutine.
    """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
    if for_del:
        SCOPES.append('https://mail.google.com/')

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds, cache_discovery=False) for _ in range(num)]
    return services

# service is synchronous method, wrapper to make it asynchronous.
def wrapper_for_get_ids(service, date: tuple):
    return service.users().messages().list(userId='me', q=f"after:{date[0]} before:{date[1]}").execute()

# Returns list of ids for a given date range.
async def async_get_ids(service, date: tuple) -> list[str]:
    results = await asyncio.to_thread(wrapper_for_get_ids, service, date) #for type list of dictionaries(having id, thread id)
    return [dict_["id"]  for dict_ in results.get('messages', [])]

async def async_get_ids_main(date_list: list[tuple],
                             token_path: str,
                             coro_num: int) -> list:
    """
        Generates email ids for the given date range.
    """
    services = generate_services(coro_num, token_path)
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    id_list = []
                         
    for date in date_list:
        await in_queue.put(date)
        
    # Add one stop signal per worker. 
    # i,e when the coroutine/worker gets None it breaks the loop of accepting/getting ids.
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(id, partial(async_get_ids, service), in_queue, out_queue)) for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)

    while not out_queue.empty():
        id_list.extend(await out_queue.get())        
    task_logger.info(f"Fetched >>>> {len(id_list)} Ids")
    return id_list

#################################################################################################################################

# service is synchronous method, wrapper to make it asynchronous.
def wrapper_for_get_payload(service, message_id: str):
    return service.users().messages().get(userId="me", id=message_id, format="full").execute()

# Returns tuple of id, payload for a given id.
async def async_get_payload(service, message_id: str):    
    results = await asyncio.to_thread(wrapper_for_get_payload, service, message_id)
    return (message_id, results.get("payload", {}))

async def async_get_paylaod_main(ids_list: list[str],
                                token_path: str,
                                coro_num: int) -> dict:
    """
        Gets payload for the given ids list.
    """
    services = generate_services(coro_num, token_path)
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    out_dict = defaultdict(list)                       
    
    for ids in ids_list:
        await in_queue.put(ids)
        
    # Add one stop signal per worker. 
    # i,e when the coroutine/worker gets None it breaks the loop of accepting/getting ids.
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(id, partial(async_get_payload, service), in_queue, out_queue)) for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)

    while not out_queue.empty():
        val = await out_queue.get()
        out_dict['Id'].append(val[0])
        out_dict['Payload'].append(val[1])    
    task_logger.info(f"Fetched Payload >>>> {len(out_dict['Id'])}")                
    return out_dict

#################################################################################################################################
""" Helper functions for Training Model """

def get_embeddings(df, model_name: str):
    """
        Generates embeddings of cleaned corpus for ml classification.
    """
    import torch, gc
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

#################################################################################################################################
""" To get ids in trash."""

def get_ids_in_trash(token_path: str) ->list[list[str]]:
    """
        Gets ids in gmail trash to be permenantly deleted.
    """

    service = generate_services(1, token_path)
    results = service[0].users().messages().list(userId='me', q="in:trash", maxResults=500).execute() # default maxres is 100
    ids_size = results.get("resultSizeEstimate", 0)
    l = results.get('messages', [])
    return [[dict_["id"] for dict_ in l[i:i+100]] for i in range(0, ids_size, 100)]

def batch_del_ids_in_trash(ids_list: list[str], token_path: str) -> None:
    """
        Permanently deletes the ids in batches.
    """

    service = generate_services(1, token_path, for_del=True)
    service[0].users().messages().batchDelete(userId="me", body={"ids": ids_list}).execute()
    task_logger.info("Completed Task!!")

#################################################################################################################################
