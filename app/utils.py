import hashlib
from typing import Optional

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def none_if_empty(s: Optional[str]):
    if s is None: return None
    s2 = s.strip()
    return s2 if s2 else None
