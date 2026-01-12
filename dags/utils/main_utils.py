import time, asyncio
from functools import partial
from collections import defaultdict
from .get_logger import make_logger
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

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
    start_time = time.perf_counter()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            task_logger.info(f"[S-CORO - {id}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")
            break
        res = await function(num)
        await out_queue.put(res)
        in_queue.task_done()


def generate_services(num: int, token_path: str, for_del: bool = False) -> list:
    """
        Generates services to be used for api calls.
        Generates service for each coroutine.
    """

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
                             coro_num: int) -> tuple[list, str]:
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
    id_length = len(id_list)
    task_logger.info(f"Fetched >>>> {id_length} Ids")
    task_logger.info(id_list[:5])
    return id_list, id_length

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
""" To send ids in trash."""

def batch_modify(ids_list: list[str], token_path: str):
    """ 
        Send emails to trash using Dynamic task mapping.
    """
    services = generate_services(token_path=token_path, num=1)
    body = {"ids": ids_list,
            "addLabelIds": ["TRASH"],     # move to trash
            "removeLabelIds": []  }       # optional: remove other labels
    
    services[0].users().messages().batchModify(userId="me", body=body).execute()
#################################################################################################################################
""" To get ids in trash."""

def get_ids_in_trash(token_path: str, chunk_size: int=100) ->list[list[str], ]:
    """
        Gets ids in gmail trash to be permenantly deleted.
    """

    service = generate_services(1, token_path)
    results = service[0].users().messages().list(userId='me', q="in:trash", maxResults=500).execute() # default maxres is 100
    ids_size = results.get("resultSizeEstimate", 0)
    l = results.get('messages', [])
    task_logger.info(f"The number of emails in trash >> {ids_size}")
    return [[dict_["id"] for dict_ in l[i:i+chunk_size]] for i in range(0, ids_size, chunk_size)]

def batch_del_ids_in_trash(ids_list: list[str], token_path: str) -> None:
    """
        Permanently deletes the ids in batches.
    """

    service = generate_services(1, token_path, for_del=True)
    service[0].users().messages().batchDelete(userId="me", body={"ids": ids_list}).execute()
    task_logger.info("Completed Task!!")

#################################################################################################################################
"""  For Redis version, To send ids in trash. """

def modify_trash_wrapper(service, ids_list: list[str]):
    service.users().messages().batchModify(userId="me", body = {"ids": ids_list,
                                                                "addLabelIds": ["TRASH"],   # move to trash
                                                                "removeLabelIds": [] }      # optional: remove other labels
                                                                ).execute()

async def modify_trash(id_, service, ids_list: list[str]):
    start_time = time.perf_counter()
    await asyncio.to_thread(modify_trash_wrapper, service, ids_list)
    task_logger.info(f"[Worker - {id_}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")

async def modify_trash_main(token_path: str, chunks: list[list[str]]):
    """ 
        Send emails to trash using modify call.
    """
    n = len(chunks)
    services = generate_services(token_path=token_path, num=n)
    tasks = [asyncio.create_task(modify_trash(id_, service, ids_list)) for id_, service, ids_list in zip(range(1,n+1), services, chunks)]
    await asyncio.gather(*tasks)

#################################################################################################################################
"""  For Redis version, To get ids in trash. """

def batch_perma_del_wrapper(service, ids_list: list[str]):
    service.users().messages().batchDelete(userId="me", body={"ids": ids_list}).execute()

async def batch_perma_del(id_, service, ids_list: list[str]):
    start_time = time.perf_counter()
    await asyncio.to_thread(batch_perma_del_wrapper, service, ids_list)
    task_logger.info(f"[Worker - {id_}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")

async def batch_perma_del_main(token_path: str, chunks: list[list[str]]):
    """ 
        Permanently deletes emails with batchDelete.
    """
    n = len(chunks)
    services = generate_services(n, token_path, for_del=True)
    tasks = [asyncio.create_task(batch_perma_del(id_, service, ids_list)) for id_, service, ids_list in zip(range(1,n+1), services, chunks)]
    await asyncio.gather(*tasks)

#################################################################################################################################
