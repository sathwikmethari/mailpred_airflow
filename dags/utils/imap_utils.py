import email
from typing import List
from datetime import datetime
from email.policy import default

""" Helper functions for IMAP version. """
def fetch_batch(email : str, password : str, ids : list[int]):
    """Importing libraries."""
    from imapclient import IMAPClient

    with IMAPClient('imap.gmail.com', ssl=True) as server:
        server.login(email, password)
        server.select_folder('INBOX')
        msg_data = server.fetch(ids, ['RFC822', 'ENVELOPE'])
        return msg_data
        
def get_ids_imap(email : str, password : str, from_date : datetime.date) -> List[int]:
    """Importing libraries."""
    from imapclient import IMAPClient

    server = IMAPClient('imap.gmail.com', ssl=True, use_uid=True)
    server.login(email, password)
    server.select_folder('INBOX')
    ids = server.search(criteria=[u'SINCE', from_date])
    server.logout()
    return ids 

def parse_email_address(addr):
    if not addr:
        return None, None
    name = addr.name.decode() if addr.name else None
    email_addr = f"{addr.mailbox.decode()}@{addr.host.decode()}" if addr.mailbox and addr.host else None
    return name, email_addr

def has_attachments(message : email.message.EmailMessage) -> bool:
    return  any(message.iter_attachments())

def get_msg_data(server, list_of_ids: List[int]):    
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