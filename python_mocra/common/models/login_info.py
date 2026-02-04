from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field
from .cookies import CookieItem, Cookies
from .headers import Headers, HeaderItem

class LoginInfo(BaseModel):
    cookies: List[CookieItem] = Field(default_factory=list)
    useragent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    extra: Dict[str, Any] = Field(default_factory=dict)

    def to_cookies(self) -> Cookies:
        return Cookies(cookies=self.cookies)

    def to_headers(self) -> Headers:
        return Headers(headers=[HeaderItem(key="User-Agent", value=self.useragent)])

    def get_extra(self, key: str) -> Optional[Any]:
        return self.extra.get(key)
    
    def get_shop_id(self) -> Optional[int]:
        return self.extra.get("shopid")
