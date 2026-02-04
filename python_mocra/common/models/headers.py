from typing import List, Dict, Optional, Union, Any
from pydantic import BaseModel, Field

class HeaderItem(BaseModel):
    key: str
    value: str
    
    def __repr__(self):
        key_lower = self.key.lower()
        if any(x in key_lower for x in ["auth", "cookie", "secret", "token"]):
            value = "***REDACTED***"
        else:
            value = self.value
        return f"HeaderItem(key='{self.key}', value='{value}')"

class Headers(BaseModel):
    headers: List[HeaderItem] = Field(default_factory=list)

    def add(self, key: str, value: str) -> "Headers":
        for h in self.headers:
            if h.key.lower() == key.lower():
                h.value = value
                return self
        self.headers.append(HeaderItem(key=key, value=value))
        return self

    def merge(self, other: "Headers"):
        for h in other.headers:
            self.headers.append(h)

    def is_empty(self) -> bool:
        return len(self.headers) == 0

    def contains(self, key: str) -> bool:
        return any(h.key.lower() == key.lower() for h in self.headers)

    def get(self, key: str) -> Optional[str]:
        for h in self.headers:
            if h.key.lower() == key.lower():
                return h.value
        return None

    def to_dict(self) -> Dict[str, str]:
        # Simple conversion, last one wins
        return {h.key: h.value for h in self.headers}

    @staticmethod
    def from_dict(d: Dict[str, str]) -> "Headers":
        headers = Headers()
        for k, v in d.items():
            headers.add(k, v)
        return headers
