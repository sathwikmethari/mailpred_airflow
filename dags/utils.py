import email, imapclient, re, time, asyncio, collections, base64 
import googleapiclient, sys, msgspec, gzip, torch
import numpy as np
from typing import List, Tuple
from queue import Queue
from functools import partial 
from datetime import datetime
from bs4 import BeautifulSoup
from email.policy import default
from imapclient import IMAPClient
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from transformers import AutoModel, AutoTokenizer


""" Helper functions for zipping/parsing"""
def decode_zip(path: str):
    #Decompress and load
    with gzip.open(path, 'rb') as f:
        decompressed_bytes = f.read()
    return msgspec.msgpack.decode(decompressed_bytes)

""" Extract Subject and From headers. """
def extract_headers(payload) -> str:
    subject = None
    headers = payload.get("headers", [])
    out = set()
    for header in headers:
        name = header.get("name", "")
        if name == "Subject":
            out.add(name)
            subject = header.get("value", "")
            return preprocess_email_body(subject)

""" mimeType may be:
>>> "text/plain" - plain text only
>>> "text/html" - HTML only
>>> "multipart/alternative" - contains both text/plain and text/html as parts
>>> "multipart/mixed" or "multipart/related" - may include attachments
The actual message body is in body.data, Base64url encoded. """

""" Decode base64 content. """
def decode_body(payload) -> str:
    if "mimeType" in payload and payload["body"].get("data"):
        mime_type = payload["mimeType"]
        data = payload.get("body", {}).get("data", "")
        body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        if mime_type == "text/plain":
            return preprocess_email_body(body)
        elif mime_type == "text/html":
            #soup = BeautifulSoup(body, "html.parser")
            soup = BeautifulSoup(body, "lxml")
            for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
                tag.decompose()
            body_text = soup.get_text()
            return preprocess_email_body(body_text)

    if "parts" in payload:
        html_text = None
        plain_text = None

        for part in payload["parts"]:
            decoded = decode_body(part)
            if part.get("mimeType") == "text/html" and decoded:
                html_text = decoded   # prefer HTML
            elif part.get("mimeType") == "text/plain" and decoded:
                plain_text = decoded

        return html_text or plain_text  # return HTML if available, else plain

    return None

def preprocess_email_body(s: str) -> str: 
    #removes urls
    s = re.sub(r"https?://\S+|http?://\S+|www\.\S+", "", s)
    # removes non alphanumerics
    s = re.sub(r"[^a-zA-Z0-9£$€¥₹\s]", "", s)
    #lowers alpabet, removes unnecessary tab spaces/spaces
    return " ".join(s.lower().strip().split())
     
def size_in_mb(x):
    size_in_bytes=sys.getsizeof(x)
    print(f"Size in MB: {size_in_bytes / (1024 * 1024)}")


"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Helper functions for IMAP version. """
def fetch_batch(email : str, password : str, ids : list[int]):
    with IMAPClient('imap.gmail.com', ssl=True) as server:
        server.login(email, password)
        server.select_folder('INBOX')
        msg_data = server.fetch(ids, ['RFC822', 'ENVELOPE'])
        return msg_data
        
def get_ids_imap(email : str, password : str, from_date : datetime.date) -> List[int]:
    
    server = IMAPClient('imap.gmail.com', ssl=True, use_uid=True)
    server.login(email, password)
    server.select_folder('INBOX')
    ids = server.search(criteria=[u'SINCE', from_date])
    server.logout()
    return ids

    # ABOVE IS FASTER    
    # with IMAPClient('imap.gmail.com', ssl=True, use_uid=True) as server:
    #     server.login(email, password)
    #     server.select_folder('INBOX')
    #     return server.search(criteria=[u'SINCE', from_date])   

def parse_email_address(addr: imapclient.response_types.Address):
    if not addr:
        return None, None
    name = addr.name.decode() if addr.name else None
    email_addr = f"{addr.mailbox.decode()}@{addr.host.decode()}" if addr.mailbox and addr.host else None
    return name, email_addr

def has_attachments(message : email.message.EmailMessage) -> bool:
    return  any(message.iter_attachments())

def get_msg_data(server : imapclient.imapclient.IMAPClient, list_of_ids: List[int]):    
    #UPDATE THIS(LIST OF IDS) AFTER TESTING
    for msgid, data in server.fetch(list_of_ids[-6], ['ENVELOPE', 'BODY[]']).items():
        # Extract envelope info
        msg_data = data[b'ENVELOPE']
        sender = msg_data.sender[0]
        name, email_addr = parse_email_address(sender)
        
        # Get raw email and parse body
        msg = email.message_from_bytes(data[b'BODY[]'], policy=default)
        # Extract body
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))

                if "attachment" not in content_disposition and content_type == "text/plain":
                    body = part.get_payload(decode=True).decode()
                    break
        else:
            # Non-multipart email
            charset =msg.get_content_charset()
            body = msg.get_payload(decode=True).decode(charset)
        
    return msg_data, name, email_addr, body


"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Helper functions for threaded gmail DAG """
def get_ids_gmail(token_path: str, q: Queue, dates: list[tuple]) -> None:
    start_time = time.time()
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(userId='me', q=f"after:{dates[0]} before:{dates[1]}").execute()
    messages = results.get('messages', [])
    q.put(messages)
    print(f"[Thread] >> Time taken: {time.time() - start_time:.4f} sec.")
    
    
def get_payload(token_path: str, payload_queue: Queue, message_ids: list[dict]):
    start_time = time.time()
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    for msg in message_ids:
        msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()

        payload = msg_data.get("payload", {})
        payload_queue.put((msg_data["id"],payload))
    print(f"[Thread] >> Time taken: {time.time() - start_time:.4f} sec.")

"""++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

""" Helper functions for ASYNC DAG """
    
#For creating multiple coroutines/tasks with different arguments
async def worker(id: int, function, in_queue: Queue, out_queue: Queue) -> None :
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
def generate_services(num: int, token_path: str) -> list[googleapiclient.discovery.Resource]:        
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds) for _ in range(num)]
    return services

""" service is synchronous method, wrapper to make it asynchronous. """
def driver_for_ids(service: googleapiclient.discovery.Resource, date: tuple):
    return service.users().messages().list(userId='me', q=f"after:{date[0]} before:{date[1]}").execute()

""" Returns list of ids for a given date range. """
async def async_get_ids(service: googleapiclient.discovery.Resource, date: tuple) -> list[str]:
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
def driver_for_payload(service: googleapiclient.discovery.Resource, message_id: str):
    return service.users().messages().get(userId="me", id=message_id, format="full").execute()

""" Returns tuple of id, payload for a given id. """
async def async_get_payload(service: googleapiclient.discovery.Resource, message_id: str):    
    results = await asyncio.to_thread(driver_for_payload, service, message_id)
    return (message_id, results.get("payload", {}))

""" Main function to create multiple coroutines,
    Returns a dictionary with ids, payload. """
async def async_get_payload_main(id_list: list[str], token_path: str, coro_num: int) -> dict:
    services = generate_services(coro_num, token_path)
    partial_functions = [partial(async_get_payload, service) for service in services]
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    out_dict = collections.defaultdict(list)                       
    
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

def get_embeddings_from_token(model_name: str, tokens: np.ndarray) -> np.ndarray:
    model = AutoModel.from_pretrained(model_name)

    embeddings = []

    with torch.no_grad():
        for token in tokens:
            outputs = model(**token)
            # Use [CLS] token embedding (first token)
            cls_embedding = outputs.last_hidden_state[:, 0, :].squeeze(0).numpy()
            embeddings.append(cls_embedding)

    return np.array(embeddings)



