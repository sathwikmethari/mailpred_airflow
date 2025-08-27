""" Helper functions for zipping/parsing"""

def decode_zip(path: str):
    """Importing libraries."""
    import gzip, msgspec
    #Decompress and load
    with gzip.open(path, 'rb') as f:
        decompressed_bytes = f.read()
    return msgspec.msgpack.decode(decompressed_bytes)

# Extract Subject and From headers.
def extract_headers(payload) -> str:
    subject = None
    headers = payload.get("headers", [])
    for header in headers:
        name = header.get("name", "")
        if name == "Subject":
            subject = header.get("value", "")
            return preprocess_email_body(subject)
    return subject

""" mimeType may be:
>>> "text/plain" - plain text only
>>> "text/html" - HTML only
>>> "multipart/alternative" - contains both text/plain and text/html as parts
>>> "multipart/mixed" or "multipart/related" - may include attachments
The actual message body is in body.data, Base64url encoded. """

# Decode base64 content.
def decode_body(payload) -> str:
    """Importing libraries."""
    import base64
    from bs4 import BeautifulSoup

    if "mimeType" in payload and payload["body"].get("data"):
        mime_type = payload["mimeType"]
        data = payload.get("body", {}).get("data", "")
        body = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        if mime_type == "text/plain":
            return preprocess_email_body(body)
        elif mime_type == "text/html":
            #soup = BeautifulSoup(body, "html.parser")
            soup = BeautifulSoup(body, "lxml")
            for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
                tag.decompose()
            body_text = soup.get_text()
            return preprocess_email_body(body_text)

    if "parts" in payload:
        html_text = None
        plain_text = None

        for part in payload["parts"]:
            decoded = decode_body(part)
            if part.get("mimeType") == "text/html" and decoded:
                html_text = decoded   # prefer HTML
            elif part.get("mimeType") == "text/plain" and decoded:
                plain_text = decoded

        return html_text or plain_text  # return HTML if available, else plain

    return None

# Clean text data
def preprocess_email_body(s: str) -> str:
    """Importing libraries."""
    import re

    #removes urls
    s = re.sub(r"https?://\S+|http?://\S+|www\.\S+", "", s)
    # removes non alphanumerics
    s = re.sub(r"[^a-zA-Z0-9£$€¥₹\s]", "", s)
    #lowers alpabet, removes unnecessary tab spaces/spaces
    return " ".join(s.lower().strip().split())
     