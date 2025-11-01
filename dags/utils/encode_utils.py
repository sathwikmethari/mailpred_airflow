import msgspec, gzip

def Serialize(data) -> bytes:
    return msgspec.msgpack.encode(data)

def Deserialize(data: bytes):
    return msgspec.msgpack.decode(data)

def decode_zip(path: str):
    """Importing libraries."""
    #Deserialize and load
    with gzip.open(path, 'rb') as f:
        decompressed_bytes = f.read()
    return Deserialize(decompressed_bytes)
