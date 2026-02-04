from typing import Optional, Union
from pydantic import BaseModel

class IpProxy(BaseModel):
    ip: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    proxy_type: str = "http" # http, socks5, etc
    rate_limit: float = 0.0

    def __str__(self):
        # Format as URL string
        scheme = self.proxy_type
        if self.username and self.password:
            return f"{scheme}://{self.username}:{self.password}@{self.ip}:{self.port}"
        return f"{scheme}://{self.ip}:{self.port}"

class Tunnel(BaseModel):
    name: str
    endpoint: str
    username: Optional[str] = None
    password: Optional[str] = None
    tunnel_type: str
    expire_time: str
    rate_limit: float

    def __str__(self):
         # Approximate string representation
        return f"Tunnel({self.name}, {self.endpoint})"

# ProxyEnum can be represented as a Union in Pydantic or a wrapper class
# Rust enum: ProxyEnum::Tunnel(Tunnel), ProxyEnum::IpProxy(IpProxy)
# In Python, we can use Union[Tunnel, IpProxy] usually.
# But Request has `proxy: Option<ProxyEnum>`.
ProxyEnum = Union[Tunnel, IpProxy]
