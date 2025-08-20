import os, gzip, msgspec
from airflow.sdk import dag, task, chain
from datetime import  datetime, timedelta

@dag
def get_training_data():    
    @task
    def get_ids(query: str) -> list[str]:
        """Importing libraries/functions/paths."""
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials
        token_path = os.environ.get("token_path_airflow")
        
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']    
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        service = build('gmail', 'v1', credentials=creds)
        results = service.users().messages().list(userId='me', q=f"is:{query}", maxResults=500).execute()
        print(f"Query: {query}. Fetched >>>> {results.get("resultSizeEstimate", 0)} Ids.")        
        return [query, [dict_["id"]  for dict_ in results.get('messages', [])]]
    
    #Dynamic task mapping
    _my_task_1 = get_ids.expand(query = ["starred", "unimportant"])
    
    @task
    def get_payload(input: list[str, list]):
        """Importing libraries/functions/paths."""
        from utils import wrapper_for_payload        
        token_path = os.environ.get("token_path_airflow")
               
        out_dict = wrapper_for_payload(input[1], token_path, 15) 

        print("Starting encoding")
        # Compressing
        bytes = msgspec.msgpack.encode(out_dict)        
        zip_path = f"/opt/airflow/data/{input[0]}_{datetime.now().strftime('%d-%m-%Y-%H-%M')}.json.gz"
        with gzip.open(zip_path, 'wb') as f:
            f.write(bytes)
        
    _my_task_2 = get_payload.expand(input = _my_task_1)
        
    
    chain(
    _my_task_1,
    _my_task_2)
    
get_training_data()
