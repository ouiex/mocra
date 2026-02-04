pub fn md5(input: &[u8]) -> String {
    format!("{:x}", md5::compute(input))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_md5() {
        let input = "5a33fa9e0cd788ff2025445201742fda&1760086002619&12574478&{}";
        let output = md5(input.as_bytes());
        assert_eq!(output, "5574764443ac90fa331a441a58f7a5dd");
    }
}
