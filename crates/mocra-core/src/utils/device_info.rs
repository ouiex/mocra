#![allow(unused)]
use std::io;
use std::net::IpAddr;
use std::process::Command;

/// Device information structure.
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    /// Device name / hostname.
    pub hostname: String,
    /// Local IP address list.
    pub local_ips: Vec<IpAddr>,
    /// Public IP address (if available).
    pub public_ip: Option<IpAddr>,
}

/// Gets full device information.
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

/// Gets device hostname.
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

/// Gets local IP address list.
pub fn get_local_ips() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    use std::net::UdpSocket;

    let mut ips = Vec::new();

    // Method 1: infer local IP by connecting to an external address.
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0")
        && socket.connect("8.8.8.8:80").is_ok()
            && let Ok(addr) = socket.local_addr() {
                ips.push(addr.ip());
            }

    // Method 2: collect network interface addresses via system commands.
    let system_ips = get_network_interfaces()?;
    for ip in system_ips {
        if !ips.contains(&ip) {
            ips.push(ip);
        }
    }

    // Filter out loopback addresses.
    ips.retain(|ip| !ip.is_loopback());

    Ok(ips)
}

/// Gets public IP address.
pub async fn get_public_ip() -> Result<IpAddr, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    // Try multiple IP lookup services.
    let services = vec![
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://checkip.amazonaws.com",
    ];

    for service in services {
        if let Ok(response) = client.get(service).send().await
            && let Ok(ip_str) = response.text().await {
                let ip_str = ip_str.trim();
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    return Ok(ip);
                }
            }
    }

    Err("Failed to get public IP from all services".into())
}

/// Gets hostname on Unix systems.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn get_hostname_unix() -> Result<String, io::Error> {
    let output = Command::new("hostname").output()?;

    if output.status.success() {
        let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(hostname)
    } else {
        // Fallback: read `/etc/hostname`.
        match std::fs::read_to_string("/etc/hostname") {
            Ok(content) => Ok(content.trim().to_string()),
            Err(_) => {
                // Last fallback: use environment variables.
                std::env::var("HOSTNAME")
                    .or_else(|_| std::env::var("HOST"))
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::NotFound, "Could not determine hostname")
                    })
            }
        }
    }
}

/// Gets hostname on Windows.
#[cfg(target_os = "windows")]
fn get_hostname_windows() -> Result<String, io::Error> {
    let output = Command::new("hostname").output()?;

    if output.status.success() {
        let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(hostname)
    } else {
        // Fallback: use environment variables.
        std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "Could not determine hostname"))
    }
}

/// Gets network interface addresses.
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

/// Gets network interfaces on macOS.
#[cfg(target_os = "macos")]
fn get_network_interfaces_macos() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let output = Command::new("ifconfig").arg("-a").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ifconfig_output(&output_str)
}

/// Gets network interfaces on Linux.
#[cfg(target_os = "linux")]
fn get_network_interfaces_linux() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    // Prefer `ip` command.
    if let Ok(output) = Command::new("ip").args(&["addr", "show"]).output() {
        let output_str = String::from_utf8_lossy(&output.stdout);
        return parse_ip_addr_output(&output_str);
    }

    // Fallback to `ifconfig` command.
    let output = Command::new("ifconfig").arg("-a").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ifconfig_output(&output_str)
}

/// Gets network interfaces on Windows.
#[cfg(target_os = "windows")]
fn get_network_interfaces_windows() -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let output = Command::new("ipconfig").arg("/all").output()?;

    let output_str = String::from_utf8_lossy(&output.stdout);
    parse_ipconfig_output(&output_str)
}

/// Parses `ifconfig` output.
fn parse_ifconfig_output(output: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        // Look for `inet` addresses.
        if line.starts_with("inet ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2
                && let Ok(ip) = parts[1].parse::<IpAddr>()
                    && !ip.is_loopback() {
                        ips.push(ip);
                    }
        }
    }

    Ok(ips)
}

/// Parses `ip addr` output.
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

/// Parses `ipconfig` output.
#[cfg(target_os = "windows")]
fn parse_ipconfig_output(output: &str) -> Result<Vec<IpAddr>, Box<dyn std::error::Error>> {
    let mut ips = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        if line.contains("IPv4") && line.contains(":") {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 2 {
                let ip_str = parts[1].trim();
                if let Ok(ip) = ip_str.parse::<IpAddr>()
                    && !ip.is_loopback() {
                        ips.push(ip);
                    }
            }
        }
    }

    Ok(ips)
}

/// Gets primary local IP address (usually first non-loopback address).
pub fn get_primary_local_ip() -> Result<IpAddr, Box<dyn std::error::Error>> {
    let ips = get_local_ips()?;
    ips.into_iter()
        .find(|ip| match ip {
            IpAddr::V4(ipv4) => {
                // Filter special-use addresses.
                !ipv4.is_loopback() && !ipv4.is_multicast() && !ipv4.is_broadcast()
            }
            IpAddr::V6(ipv6) => !ipv6.is_loopback() && !ipv6.is_multicast(),
        })
        .ok_or_else(|| "No valid local IP address found".into())
}

/// Formats device information as string.
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
