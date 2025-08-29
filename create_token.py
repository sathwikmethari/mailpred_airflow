import os
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

#https://developers.google.com/workspace/gmail/api/quickstart/python
def main(creds_path: str, token_path: str, SCOPES) -> None:
  """Shows basic usage of the Gmail API.
  Lists the user's Gmail labels.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists(token_path):
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          creds_path, SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(token_path, "w") as token:
      token.write(creds.to_json())
      
if __name__ == "__main__":    
  # If modifying these scopes, delete the file token.json.
  SCOPES = ['https://mail.google.com/', "https://www.googleapis.com/auth/gmail.modify"]
  load_dotenv()
  token_path = os.getenv("token_path")
  creds_path = os.getenv("credentials_path")
  main(creds_path, token_path, SCOPES)