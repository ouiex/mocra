import hashlib

def md5(input_bytes: bytes) -> str:
    """
    Computes the MD5 hash of the input bytes and returns it as a hexadecimal string.
    """
    return hashlib.md5(input_bytes).hexdigest()
