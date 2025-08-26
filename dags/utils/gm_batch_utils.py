import time, asyncio
from functools import partial
from collections import defaultdict

""" Helper functions for ASYNC BATCHED DAG """

""" Class to hold ids, payload, failed ones if any. """
class Batched_Payload:
    def __init__(self):
        self.req_id = []
        self.payload = []
        self.failed = []

    def handle_message(self, request_id, response, exception):
        if exception:
            self.failed.append(request_id)
        else:
            self.req_id.append(request_id)
            self.payload.append(response.get("payload", {}))

""" Wrapping batch.execute() """
def driver_for_batch(batch):
    batch.execute()

async def worker_for_batched(id: int, function, in_queue: asyncio.Queue, out_queue: asyncio.Queue, fail_queue: asyncio.Queue) -> None :
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[B-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        res = await function(num)
        await out_queue.put((res.req_id, res.payload))
        if res.failed != []:
            await fail_queue.put(res.failed)
        in_queue.task_done()

async def async_get_batched_payload(service, ids_list: list[str]):
    custom_ds = Batched_Payload()
    batch = service.new_batch_http_request(callback=custom_ds.handle_message)
    for msg_id in ids_list:  # limit to 25
        request = service.users().messages().get(userId="me", id=msg_id, format="full")
        batch.add(request, request_id=msg_id)

    # Execute batch request
    await asyncio.to_thread(driver_for_batch, batch)
    return custom_ds

""" Main function to create multiple Batched coroutines,
    Returns a dictionary with ids, payload. """
async def async_get_batched_main(id_chunks: list[list[str]], token_path: str, coro_num: int) -> dict:
    """Importing Functions."""
    from utils.gm_single_utils import worker, generate_services, async_get_payload

    services = generate_services(coro_num, token_path)
    in_queue, out_queue = asyncio.Queue(), asyncio.Queue()
    fail_queue = asyncio.Queue()
    out_dict = defaultdict(list)                       
    
    for chunk in id_chunks:
        await in_queue.put(chunk)
        
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker_for_batched(id, partial(async_get_batched_payload, service), in_queue, out_queue, fail_queue))
             for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)
    
    print(f"Number of failed Ids >>>> {fail_queue.qsize()}")

    while not fail_queue.empty():
        failed_ids = await fail_queue.get()
        for msg_id in failed_ids:
            await in_queue.put(msg_id)
    for _ in range(coro_num):
        await in_queue.put(None)

    print('='*38)

    tasks = [asyncio.create_task(worker(id, partial(async_get_payload, service), in_queue, out_queue))
             for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)

    while not out_queue.empty():
        val = await out_queue.get()
        if type(val[0]) is list:
            out_dict['Id'].extend(val[0])
            out_dict['Payload'].extend(val[1])
        else:
            out_dict['Id'].append(val[0])
            out_dict['Payload'].append(val[1])  

    print("Succesfully fetched Payload")             
    return out_dict

# Wrapper for main function for airflow
def wrapper_for_batched_payload(id_chunks: list[list[str]], token_path: str, coro_num: int) -> dict:
    return asyncio.run(async_get_batched_main(id_chunks, token_path, coro_num))


"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""


""" For Trashing. """
""" Trashes Emails by Single Id. """

#For creating multiple coroutines/tasks with different arguments
async def worker_single_trash(id: int, function, in_queue: asyncio.Queue) -> None :
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[S-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        await function(num)
        in_queue.task_done()

""" service is synchronous method, wrapper to make it asynchronous. """
def wrapper_single_trash(service, message_id: str):
    service.users().messages().trash(userId="me", id=message_id).execute()

""" Trashes Emails by id """
async def async_single_trash(service, message_id: str):    
    await asyncio.to_thread(wrapper_single_trash, service, message_id)
   
"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Trashes Emails by Batches. """
async def worker_batch_trash(id: int, function, in_queue: asyncio.Queue, fail_queue: asyncio.Queue) -> None :
    start_time = time.time()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[B-CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
            break
        res = await function(num)
        if res.failed != []:
            await fail_queue.put(res.failed)
        in_queue.task_done()

class Batched_Trash:
    def __init__(self):
        self.failed = []

    def handle_message(self, request_id, response, exception):
        if exception:
            self.failed.append(request_id)

def wrapper_batch_trash(batch):
    batch.execute()

async def async_batch_trash(service, ids_list: list[str]):
    trash_ds = Batched_Trash()
    batch = service.new_batch_http_request(callback = trash_ds.handle_message)
    for msg_id in ids_list:  # limit to 25
        request = service.users().messages().trash(userId="me", id=msg_id)
        batch.add(request, request_id=msg_id)
    # Execute batch request
    await asyncio.to_thread(wrapper_batch_trash, batch)
    return trash_ds

""" Main function to create multiple Batched coroutines. """
async def async_batch_trash_main(id_chunks: list[list[str]], token_path: str, coro_num: int) -> dict:
    #"""Importing Functions."""
    from utils.gm_single_utils import generate_services
    
    services = generate_services(coro_num, token_path)
    in_queue, fail_queue = asyncio.Queue(), asyncio.Queue()
                
    for chunk in id_chunks:
        await in_queue.put(chunk)
        
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker_batch_trash(id, partial(async_batch_trash, service), in_queue, fail_queue))
             for id, service in enumerate(services, start=1)]
    await asyncio.gather(*tasks)

    print(f"Number of failed Ids >>>> {fail_queue.qsize()}")
    if not fail_queue.empty():
        while not fail_queue.empty():
            failed_ids = await fail_queue.get()
            for msg_id in failed_ids:
                await in_queue.put(msg_id)
        for _ in range(coro_num):
            await in_queue.put(None)

        print('='*38)

        tasks = [asyncio.create_task(worker_single_trash(id, partial(async_single_trash, service), in_queue))
                for id, service in enumerate(services, start=1)]
        await asyncio.gather(*tasks)


def wrapper_for_batch_trash_main(id_chunks: list[list[str]], token_path: str, coro_num: int) -> dict:
    return asyncio.run(async_batch_trash_main(id_chunks, token_path, coro_num))





