from typing import List, Optional
from pydantic import BaseModel, Field

class CookieItem(BaseModel):
    name: str
    value: str
    domain: str
    path: str = "/"
    expires: Optional[int] = None # Unix timestamp in seconds
    max_age: Optional[int] = None # Seconds
    secure: bool = False
    http_only: Optional[bool] = None

    def __repr__(self):
        return f"CookieItem(name='{self.name}', value='***REDACTED***', domain='{self.domain}', path='{self.path}')"

class Cookies(BaseModel):
    cookies: List[CookieItem] = Field(default_factory=list)

    def add(self, name: str, value: str, domain: str):
        self.cookies.append(CookieItem(name=name, value=value, domain=domain))

    def merge(self, other: "Cookies"):
        for cookie in other.cookies:
            if not self.contains(cookie.name, cookie.domain):
                self.cookies.append(cookie)

    def is_empty(self) -> bool:
        return len(self.cookies) == 0

    def contains(self, name: str, domain: str) -> bool:
        return any(c.name == name and c.domain == domain for c in self.cookies)
    
    def string(self) -> str:
        return ";".join([f"{c.name}={c.value}" for c in self.cookies])
