import email, datetime, imapclient, re, queue, time, asyncio
from imapclient import IMAPClient
from typing import List
from email.policy import default
import googleapiclient
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from functools import partial 

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



def has_html(text):
    return bool(re.search(r'<[a-z/][^>]*>', text, re.IGNORECASE))

token_path = ""

def get_ids_gmail(token_path: str, q: queue.Queue, dates: list[tuple]) -> None:
    start_time = time.time()
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(userId='me', q=f"after:{dates[0]} before:{dates[1]}").execute()
    messages = results.get('messages', [])
    q.put(messages)
    print(f"[Thread] >> Time taken: {time.time() - start_time:.4f} sec.")
    
    #time.sleep(0.2)

    
def get_payload(token_path: str, payload_queue: queue.Queue, message_ids: list[dict]):
    start_time = time.time()
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    for msg in message_ids:
        msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()

        payload = msg_data.get("payload", {})
        payload_queue.put((msg_data["id"],payload))
    print(f"[Thread] >> Time taken: {time.time() - start_time:.4f} sec.")
    
#For creating multiple tasks with different arguments
async def worker(function, in_queue, out_queue):
    while True:
        num = await in_queue.get()
        if num is None:
            in_queue.task_done()
            break
        res = await function(num)
        await out_queue.put(res)
        in_queue.task_done()

#Generates services
def generate_services(num: int, token_path: str) -> list[googleapiclient.discovery.Resource]:        
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    services = [build('gmail', 'v1', credentials=creds) for _ in range(num)]
    return services

#service is synchronous function
def driver_for_ids(service: googleapiclient.discovery.Resource, dates: list[tuple]):
    return service.users().messages().list(userId='me', q=f"after:{dates[0]} before:{dates[1]}").execute()

async def async_get_ids(id: int, service, dates: list[tuple]) -> dict:
    start_time = time.time()
    results = await asyncio.to_thread(driver_for_ids, service, dates)
    messages = results.get('messages', [])
    print(f"[CORO - {id}] >> Time taken: {time.time() - start_time:.4f} sec.")
    return messages

async def async_get_ids_main(dates: list[tuple], token_path: str) -> list[dict]:
    num = 20
    services = generate_services(num, token_path)
    partial_functions = [partial(async_get_ids, i, services[i]) for i in range(1, num)]
    
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    
    for date in dates:
        await in_queue.put(date)

    # Add one stop signal per worker
    for _ in range(num):
        await in_queue.put(None)
        
    tasks = [asyncio.create_task(worker(function, in_queue, out_queue)) for function in partial_functions]
    
    await asyncio.gather(*tasks)
    
    return [await out_queue.get() for _ in range(out_queue.qsize())]

def wrapper_for_ids(dates: list[tuple], token_path: str):
    return asyncio.run(async_get_ids_main(dates, token_path))