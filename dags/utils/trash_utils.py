import asyncio, time
from functools import partial

# Notes-1 : Most of the batch trashing is failing and ids are put into single trash queue
# Notes-2 : Batch modifying the email's label to trash is significantly faster!!


""" For Trashing. """
""" Trashes Emails by Single Id. """

#For creating multiple coroutines/tasks with different arguments.
async def worker_single_trash(id: int, function, in_queue: asyncio.Queue) -> None :
    start_time = time.perf_counter()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[S-CORO - {id}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")
            break
        await function(num)
        in_queue.task_done()

# service is synchronous method, wrapper to make it asynchronous. """
def wrapper_single_trash(service, message_id: str):    # Edited for HttpError:  500
    service.users().messages().trash(userId="me", id=message_id).execute()

# Trashes Emails by id. 
async def async_single_trash(service, message_id: str):
    for _ in range(2):                                 # Getiing error while deleting last 10 emails. Retry loop added!
        try:
            await asyncio.to_thread(wrapper_single_trash, service, message_id)
            return
        except Exception as e:
            if e.resp.status == 500:
                asyncio.sleep(1)    

#################################################################################################################################

# Trashes Emails by Batches.
async def worker_batch_trash(id: int, function, in_queue: asyncio.Queue, fail_queue: asyncio.Queue) -> None :
    start_time = time.perf_counter()
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            print(f"[B-CORO - {id}] >> Time taken: {time.perf_counter() - start_time:.4f} sec.")
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
async def async_batch_trash_main(id_chunks: list[list[str]], token_path: str) -> dict:
    #"""Importing Functions."""
    from dags.utils.main_utils import generate_services
    
    coro_num = len(id_chunks)
    services = generate_services(coro_num*2, token_path)
    in_queue, fail_queue = asyncio.Queue(), asyncio.Queue()
                
    for chunk in id_chunks:
        await in_queue.put(chunk)
        
    for _ in range(coro_num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker_batch_trash(id, partial(async_batch_trash, service), in_queue, fail_queue))
             for id, service in enumerate(services[:coro_num], start=1)]
    await asyncio.gather(*tasks)

    if not fail_queue.empty():
        while not fail_queue.empty():
            failed_ids = await fail_queue.get()
            for msg_id in failed_ids:
                await in_queue.put(msg_id)
        for _ in range(coro_num*2):
            await in_queue.put(None)

        print(f"Number of failed Ids >>>> {in_queue.qsize()}")
        print('#'*50)

        tasks = [asyncio.create_task(worker_single_trash(id, partial(async_single_trash, service), in_queue))
                for id, service in enumerate(services, start=1)]
        await asyncio.gather(*tasks)

def wrapper_for_batch_trash_main(id_chunks: list[list[str]], token_path: str) -> dict:
    return asyncio.run(async_batch_trash_main(id_chunks, token_path))

#################################################################################################################################
