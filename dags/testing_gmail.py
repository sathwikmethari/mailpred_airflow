from airflow.sdk import dag, task, chain

import base64
import email

@dag
def gmail_dag():
    @task
    def get_mail_ids() -> list[dict]:
        """Importing Libraries."""
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build    
        # SCOPES: Gmail read-only
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
        
        token_path = "include/secrets/gmail/token.json"

        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        service = build('gmail', 'v1', credentials=creds)
        
        results = service.users().messages().list(userId='me', maxResults=10, q="after:2025/08/01").execute() #of form [{'id': '1988883dd0cf479e', 'threadId': '1988883dd0cf479e'},....]
        return results.get('messages', [])
    
    _my_task_1 = get_mail_ids()
    
    @task
    def get_mail_data(messages:list[dict]) -> list[dict]:
        pass
        # for i, msg in enumerate(messages):
        #     msg_data = service.users().messages().get(userId='me', id=msg['id'], format='metadata', metadataHeaders=['Subject', 'From', 'Date']).execute()
        #     headers = msg_data.get('payload', {}).get('headers', [])

        #     email_info = {header['name']: header['value'] for header in headers if header['name'] in ['Subject', 'From', 'Date']}

        #     print(f"\n📧 Email {i+1}")
        #     print(f"From: {email_info.get('From')}")
        #     # print(f"Subject: {email_info.get('Subject')}")
        #     print(f"Date: {email_info.get('Date')}")
        return
    _my_task_2 = get_mail_data(_my_task_1)
    
    chain(
    _my_task_1,
    _my_task_2,
    )
    
gmail_dag()