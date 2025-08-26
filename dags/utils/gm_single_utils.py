import time, asyncio, gc
from functools import partial
from collections import defaultdict

""" Helper functions for ASYNC DAG """

def get_dates(from_date, num_of_days: int) -> list[tuple]:
    #from_date is of type datetime.date
    from datetime import timedelta
    
    ranges = []
    for i in range(1, num_of_days+1):
        after_date = (from_date - timedelta(days=i)).strftime("%Y/%m/%d")
        before_date = (from_date - timedelta(days=i - 1)).strftime("%Y/%m/%d")
        ranges.append((after_date, before_date))
    return ranges
    
#For creating multiple coroutines/tasks with different arguments
async def worker(id: int, function, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None :
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[S-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        res = await function(num)
        await out_queue.put(res)
        in_queue.task_done()

#Generates service for each coroutine
def generate_services(num: int, token_path: str) -> list:
    """Importing libraries. """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    SCOPES = ['https://www.googleapis.com/auth/gmail.modify']    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds) for _ in range(num)]
    return services

""" service is synchronous method, wrapper to make it asynchronous. """
def wrapper_for_ids(service, date: tuple):
    return service.users().messages().list(userId='me', q=f"after:{date[0]} before:{date[1]}").execute()

""" Returns list of ids for a given date range. """
async def async_get_ids(service, date: tuple) -> list[str]:
    results = await asyncio.to_thread(wrapper_for_ids, service, date) #for type list of dictionaries(having id, thread id)
    return [dict_["id"]  for dict_ in results.get('messages', [])]

""" service is synchronous method, wrapper to make it asynchronous. """
def wrapper_for_payload(service, message_id: str):
    return service.users().messages().get(userId="me", id=message_id, format="full").execute()

""" Returns tuple of id, payload for a given id. """
async def async_get_payload(service, message_id: str):    
    results = await asyncio.to_thread(wrapper_for_payload, service, message_id)
    return (message_id, results.get("payload", {}))

""" Main function to create multiple coroutines,
    Returns a list of ids or dictionary with ids & payload. """

async def async_get_single_main(func, a_list: list[str] | list[tuple],
                                token_path: str, coro_num: int, for_ids: bool) -> dict | list:
    
    services = generate_services(coro_num, token_path)
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    if for_ids:
        id_list = []
    else:
        out_dict = defaultdict(list)                       
    
    for msg_id in a_list:
        await in_queue.put(msg_id)
        
    # Add one stop signal per worker. 
    # i,e when the coroutine/worker gets None it breaks the loop of accepting/getting ids.
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(id, partial(func, service), in_queue, out_queue)) for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)

    if for_ids:
        while not out_queue.empty():
            id_list.extend(await out_queue.get())        
        print(f"Fetched >>>> {len(id_list)} Ids")
        return id_list
    else:
        while not out_queue.empty():
            val = await out_queue.get()
            out_dict['Id'].append(val[0])
            out_dict['Payload'].append(val[1])    
        print(f"Fetched Payload >>>> {len(out_dict['Id'])}")                
        return out_dict
    
def wrapper_get_single_main(func, a_list: list[str], token_path: str, coro_num: int, for_ids: bool) -> dict | list:
    return asyncio.run(async_get_single_main(func, a_list, token_path, coro_num, for_ids))

"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Helper functions for Training Model """

def get_embeddings(df, model_name: str):
    """Importing libraries."""
    import torch
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