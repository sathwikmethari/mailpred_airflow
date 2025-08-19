import os, msgspec, gzip
from dotenv import load_dotenv 
from datetime import  datetime
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from dags.utils import wrapper_for_payload               

def get_starred_emails(token_path):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    #query = "is:starred"
    query = "label:unimportant"
    results = service.users().messages().list(userId='me', q=query, maxResults=500).execute()
    ids = [dict_["id"]  for dict_ in results.get('messages', [])]
    print(f"Fetched >>>> {len(ids)} Ids")
    return ids
    
if __name__ == "__main__":
    load_dotenv()
    token_path = os.getenv("token_path")
    ids = get_starred_emails(token_path)
    # out_dict  = wrapper_for_payload(ids, token_path)
    # print("Starting encoding")
    # # Compressing
    # bytes = msgspec.msgpack.encode(out_dict)        
    # zip_path = f"starred-{datetime.now().strftime('%d-%m-%Y-%H-%M')}.json.gz"
    # with gzip.open(zip_path, 'wb') as f:
    #     f.write(bytes)

