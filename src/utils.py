import email, datetime, imapclient, re, base64
from imapclient import IMAPClient
from typing import List
from email import message_from_bytes
from email.policy import default

def get_ids(email : str, password : str, from_date : datetime.date) -> List[int]:
    
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


def extract_headers(payload):
    """Extract Subject and From headers."""
    headers = payload.get("headers", [])
    subject = sender = None
    for h in headers:
        name = h.get("name", "").lower()
        if name == "subject":
            subject = h.get("value", "")
        elif name == "from":
            sender = h.get("value", "")
    return subject, sender


def decode_body(payload, prefer_plain=True):
    """
    Decode base64 content.
    If prefer_plain=True, returns text/plain first, then text/html.
    """
    if "mimeType" in payload and payload["body"].get("data"):
        mime_type = payload["mimeType"]
        body = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
        if not prefer_plain or mime_type == "text/plain":
            return body

    if "parts" in payload:
        plain_text = ""
        html_text = ""
        for part in payload["parts"]:
            text = decode_body(part, prefer_plain)
            if part.get("mimeType") == "text/plain" and not plain_text:
                plain_text = text
            elif part.get("mimeType") == "text/html" and not html_text:
                html_text = text
        return plain_text if plain_text else html_text

    return ""