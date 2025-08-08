# from airflow.sdk import dag, task, chain
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# import base64
# import email


# @dag
# def gmail_dag():
#     @task
#     def authenticate_gmail():    
#         # SCOPES: Gmail read-only
#         SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
#         creds = None
#         flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
#         creds = flow.run_local_server(port=0)
#         service = build('gmail', 'v1', credentials=creds)
#         return service
    
#     _my_task_1 = authenticate_gmail()
#     chain(
#     _my_task_1,
#     )
    

# gmail_dag()