import email, datetime, imapclient, re
from imapclient import IMAPClient
from typing import List
from email import message_from_bytes
from email.policy import default

def fetch_batch(email : str, password : str, ids : list[int]):
    with IMAPClient('imap.gmail.com', ssl=True) as server:
        server.login(email, password)
        server.select_folder('INBOX')
        msg_data = server.fetch(ids, ['RFC822', 'ENVELOPE'])
        return msg_data