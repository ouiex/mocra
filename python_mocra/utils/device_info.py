import socket
import platform
import subprocess
import re
import logging
import asyncio
import aiohttp
from typing import List, Optional

logger = logging.getLogger(__name__)

class DeviceInfo:
    def __init__(self, hostname: str, local_ips: List[str], public_ip: Optional[str] = None):
        self.hostname = hostname
        self.local_ips = local_ips
        self.public_ip = public_ip

    def __str__(self):
        return f"Device Information:\n  Hostname: {self.hostname}\n  Local IPs: {', '.join(self.local_ips)}\n  Public IP: {self.public_ip or 'Not available'}"

def get_hostname() -> str:
    return socket.gethostname()

def get_local_ips() -> List[str]:
    ips = set()
    
    # Method 1: Connect to external address
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        # doesn't even have to be reachable
        s.connect(('8.8.8.8', 1))
        ip = s.getsockname()[0]
        ips.add(ip)
        s.close()
    except Exception:
        pass

    # Method 2: Iterate interfaces (Platform specific)
    try:
        ips.update(_get_network_interfaces())
    except Exception as e:
        logger.warning(f"Failed to get network interfaces: {e}")

    # Remove loopback
    return [ip for ip in ips if not ip.startswith('127.') and ip != '::1']

async def get_public_ip() -> Optional[str]:
    services = [
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://checkip.amazonaws.com",
    ]
    
    async with aiohttp.ClientSession() as session:
        for service in services:
            try:
                async with session.get(service, timeout=10) as response:
                    if response.status == 200:
                        ip = (await response.text()).strip()
                        # Simple validation
                        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ip):
                            return ip
            except Exception:
                continue
    return None

async def get_device_info() -> DeviceInfo:
    hostname = get_hostname()
    local_ips = get_local_ips()
    public_ip = await get_public_ip()
    return DeviceInfo(hostname, local_ips, public_ip)

def _get_network_interfaces() -> List[str]:
    ips = []
    system = platform.system()
    
    if system == "Windows":
        try:
            output = subprocess.check_output("ipconfig", shell=True).decode('utf-8', errors='ignore')
            for line in output.split('\n'):
                if "IPv4" in line and ":" in line:
                    parts = line.split(":")
                    if len(parts) >= 2:
                        ip = parts[1].strip()
                        # Remove any trailing suffix like (Preferred)
                        ip = ip.split("(")[0].strip() 
                        ips.append(ip)
        except Exception:
            pass
            
    elif system in ["Linux", "Darwin"]: # MacOS is Darwin
        # Try 'ip addr' first (Linux)
        try:
            output = subprocess.check_output(["ip", "addr"], stderr=subprocess.DEVNULL).decode('utf-8')
            for line in output.split('\n'):
                line = line.strip()
                if line.startswith("inet "):
                    parts = line.split()
                    if len(parts) >= 2:
                        ip = parts[1].split('/')[0]
                        ips.append(ip)
            return ips
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass

        # Try 'ifconfig' (MacOS/Linux)
        try:
            output = subprocess.check_output(["ifconfig", "-a"], stderr=subprocess.DEVNULL).decode('utf-8')
            for line in output.split('\n'):
                line = line.strip()
                if line.startswith("inet "):
                    parts = line.split()
                    if len(parts) >= 2:
                        ip = parts[1]
                        ips.append(ip)
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass
            
    return ips

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    info = asyncio.run(get_device_info())
    print(info)
