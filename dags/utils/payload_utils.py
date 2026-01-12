""" Helper functions for zipping/parsing"""
import re, base64
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup

""" mimeType may be:
>>> "text/plain" - plain text only
>>> "text/html" - HTML only
>>> "multipart/alternative" - contains both text/plain and text/html as parts
>>> "multipart/mixed" or "multipart/related" - may include attachments
The actual message body is in body.data, Base64url encoded. """

def decode_gmail_payload(payload) -> str:
    """Importing libraries."""
    
    body = ""
    mime_type = payload.get("mimeType", "")
    data = payload.get("body", {}).get("data", "")
    if data:
        body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        if mime_type == "text/plain":
            body = preprocess_email_body(body)
        elif mime_type == "text/html":
            soup = BeautifulSoup(body, "lxml")
            for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
                tag.decompose()
            body_text = soup.get_text()
            body = preprocess_email_body(body_text)

        elif "parts" in payload:
            html_body = None
            plain_body = None
            for part in payload["parts"]:
                _, _, part_body = decode_gmail_payload(part)
                if not part_body:
                    continue

                part_type = part.get("mimeType", "")
                if "html" in part_type:
                    html_body = part_body
                elif "plain" in part_type:
                    plain_body = part_body

            body = html_body or plain_body or ""  
    
    recieved_date = None
    subject = None
    headers = payload.get("headers", [])
    for header in headers:
        name = header.get("name", "")
        if name == "Subject":
            subject = header.get("value", "")           
        elif name == "Date":
            recieved_date = header.get("value", None)
            if recieved_date is not None:
                recieved_date = parsedate_to_datetime(recieved_date)#.date() # Gets the date of the recieving
        if subject is not None and recieved_date is not None:
            break
    if subject is None or subject=="":
        subject = "Email has no Subject"
    if body is None or body=="":
        body = f"Email has a subject '{subject}' and no body."

    return recieved_date, subject, body

# Clean text data
def preprocess_email_body(s: str) -> str:
    """Importing libraries."""

    #removes urls
    s = re.sub(r"https?://\S+|http?://\S+|www\.\S+", "", s)
    # removes non alphanumerics
    s = re.sub(r"[^a-zA-Z0-9£$€¥₹\s]", "", s)
    #lowers alpabet, removes unnecessary tab spaces/spaces
    return " ".join(s.lower().strip().split())
     