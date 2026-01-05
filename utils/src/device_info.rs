#![allow(unused)]
use std::io;
use std::net::IpAddr;
use std::process::Command;

/// 设备信息结构体
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    /// 设备名称/主机名
    pub hostname: String,
    /// 本地IP地址列表
    pub local_ips: Vec<IpAddr>,
    /// 公网IP地址（如果可获取）
    pub public_ip: Option<IpAddr>,
}

/// 获取设备完整信息
pub async fn get_device_info() -> Result<DeviceInfo, Box<dyn std::error::Error>> {
    let hostname = get_hostname()?;
    let local_ips = get_local_ips()?;
    let public_ip = get_public_ip().await.ok();

    Ok(DeviceInfo {
        hostname,
        local_ips,
        public_ip,
    })
}

/// 获取设备主机名
pub fn get_hostname() -> Result<String, io::Error> {
    #[cfg(target_os = "windows")]
    {
        get_hostname_windows()
    }
    #[cfg(target_os = "macos")]
    {
        get_hostname_unix()
    }
    #[cfg(target_os = "linux")]
    {
        get_hostname_unix()
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unsupported operating system",
        ))
    }
}

/// 获取本地IP地址列表
pub fn get_local_ips() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    use std::net::UdpSocket;

    let mut ips = Vec::new();

    // 方法1: 通过连接外部地址获取本地IP
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                ips.push(addr.ip());
            }
        }
    }

    // 方法2: 通过系统命令获取网络接口信息
    let system_ips = get_network_interfaces()?;
    for ip in system_ips {
        if !ips.contains(&ip) {
            ips.push(ip);
        }
    }

    // 过滤掉环回地址
    ips.retain(|ip| !ip.is_loopback());

    Ok(ips)
}

/// 获取公网IP地址
pub async fn get_public_ip() -> Result<IpAddr, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    // 尝试多个IP查询服务
    let services = vec![
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://checkip.amazonaws.com",
    ];

    for service in services {
        if let Ok(response) = client.get(service).send().await {
            if let Ok(ip_str) = response.text().await {
                let ip_str = ip_str.trim();
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    return Ok(ip);
                }
            }
        }
    }

    Err("Failed to get public IP from all services".into())
}

/// Unix系统获取主机名
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn get_hostname_unix() -> Result<String, io::Error> {
    let output = Command::new("hostname").output()?;

    if output.status.success() {
        let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(hostname)
    } else {
        // 备用方法：读取 /etc/hostname 文件
        match std::fs::read_to_string("/etc/hostname") {
            Ok(content) => Ok(content.trim().to_string()),
            Err(_) => {
                // 最后备用方法：使用环境变量
                std::env::var("HOSTNAME")
                    .or_else(|_| std::env::var("HOST"))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::NotFound, "Could not determine hostname")
                    })
            }
        }
    }
}

/// Windows系统获取主机名
#[cfg(target_os = "windows")]
fn get_hostname_windows() -> Result<String, io::Error> {
    let output = Command::new("hostname").output()?;

    if output.status.success() {
        let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(hostname)
    } else {
        // 备用方法：使用环境变量
        std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "Could not determine hostname"))
    }
}

/// 获取网络接口信息
fn get_network_interfaces() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    #[cfg(target_os = "macos")]
    {
        ips.extend(get_network_interfaces_macos()?);
    }
    #[cfg(target_os = "linux")]
    {
        ips.extend(get_network_interfaces_linux()?);
    }
    #[cfg(target_os = "windows")]
    {
        ips.extend(get_network_interfaces_windows()?);
    }

    Ok(ips)
}

/// macOS获取网络接口
#[cfg(target_os = "macos")]
fn get_network_interfaces_macos() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let output = Command::new("ifconfig").arg("-a").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ifconfig_output(&output_str)
}

/// Linux获取网络接口
#[cfg(target_os = "linux")]
fn get_network_interfaces_linux() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    // 优先使用 ip 命令
    if let Ok(output) = Command::new("ip").args(&["addr", "show"]).output() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        return parse_ip_addr_output(&output_str);
    }

    // 备用 ifconfig 命令
    let output = Command::new("ifconfig").arg("-a").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ifconfig_output(&output_str)
}

/// Windows获取网络接口
#[cfg(target_os = "windows")]
fn get_network_interfaces_windows() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let output = Command::new("ipconfig").arg("/all").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ipconfig_output(&output_str)
}

/// 解析ifconfig输出
fn parse_ifconfig_output(output: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        // 查找inet地址
        if line.starts_with("inet ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(ip) = parts[1].parse::<IpAddr>() {
                    if !ip.is_loopback() {
                        ips.push(ip);
                    }
                }
            }
        }
    }

    Ok(ips)
}

/// 解析ip addr输出
#[cfg(target_os = "linux")]
fn parse_ip_addr_output(output: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        if line.starts_with("inet ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let addr_with_mask = parts[1];
                let addr = addr_with_mask.split('/').next().unwrap_or(addr_with_mask);
                if let Ok(ip) = addr.parse::<IpAddr>() {
                    if !ip.is_loopback() {
                        ips.push(ip);
                    }
                }
            }
        }
    }

    Ok(ips)
}

/// 解析ipconfig输出
#[cfg(target_os = "windows")]
fn parse_ipconfig_output(output: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        if line.contains("IPv4") && line.contains(":") {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 2 {
                let ip_str = parts[1].trim();
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    if !ip.is_loopback() {
                        ips.push(ip);
                    }
                }
            }
        }
    }

    Ok(ips)
}

/// 获取主要的本地IP地址（通常是第一个非环回地址）
pub fn get_primary_local_ip() -> Result<IpAddr, Box<dyn std::error::Error>> {
    let ips = get_local_ips()?;
    ips.into_iter()
        .find(|ip| match ip {
            IpAddr::V4(ipv4) => {
                // 过滤掉私有地址范围中的特殊地址
                !ipv4.is_loopback() && !ipv4.is_multicast() && !ipv4.is_broadcast()
            }
            IpAddr::V6(ipv6) => !ipv6.is_loopback() && !ipv6.is_multicast(),
        })
        .ok_or_else(|| "No valid local IP address found".into())
}

/// 格式化设备信息为字符串
impl std::fmt::Display for DeviceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Device Information:")?;
        writeln!(f, "  Hostname: {}", self.hostname)?;
        writeln!(f, "  Local IPs:")?;
        for ip in &self.local_ips {
            writeln!(f, "    - {ip}")?;
        }
        if let Some(public_ip) = &self.public_ip {
            writeln!(f, "  Public IP: {public_ip}")?;
        } else {
            writeln!(f, "  Public IP: Not available")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hostname() {
        let hostname = get_hostname();
        assert!(hostname.is_ok());
        println!("Hostname: {hostname:?}");
    }

    #[test]
    fn test_get_local_ips() {
        let ips = get_local_ips();
        assert!(ips.is_ok());
        println!("Local IPs: {ips:?}");
    }

    #[tokio::test]
    async fn test_get_public_ip() {
        let public_ip = get_public_ip().await;
        println!("Public IP: {public_ip:?}");
    }

    #[tokio::test]
    async fn test_get_device_info() {
        let device_info = get_device_info().await;
        assert!(device_info.is_ok());
        if let Ok(info) = device_info {
            println!("{info}");
        }
    }

    #[test]
    fn test_get_primary_local_ip() {
        let ip = get_primary_local_ip();
        println!("Primary local IP: {ip:?}",);
    }
}
