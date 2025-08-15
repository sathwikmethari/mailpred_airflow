import os
from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

def authenticate_gmail(credentials_path: str):
    # SCOPES: Gmail read-only
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
    creds = flow.run_local_server(port=0)
    service = build('gmail', 'v1', credentials=creds)
    return service

if __name__ == "__main__":
    load_dotenv()
    token_path = os.getenv("token_path")
    credentials_path = os.getenv("credentials_path")
    service = authenticate_gmail(credentials_path)