use flate2::read::GzDecoder;
use std::borrow::Cow;
use std::io::Read;

pub fn compress_payload(data: &[u8], threshold: usize) -> Vec<u8> {
    if data.len() > threshold {
        match zstd::stream::encode_all(data, 3) {
            Ok(compressed) => compressed,
            Err(_) => data.to_vec(),
        }
    } else {
        data.to_vec()
    }
}

pub fn compress_payload_owned(data: Vec<u8>, threshold: usize) -> Vec<u8> {
    if data.len() > threshold {
        match zstd::stream::encode_all(data.as_slice(), 3) {
            Ok(compressed) => compressed,
            Err(_) => data,
        }
    } else {
        data
    }
}

pub fn decompress_payload<'a>(payload: &'a [u8]) -> Cow<'a, [u8]> {
    // Try Zstd first (magic: 0x28 0xB5 0x2F 0xFD)
    if payload.len() > 4
        && payload[0] == 0x28 && payload[1] == 0xB5
        && payload[2] == 0x2F && payload[3] == 0xFD
    {
        match zstd::stream::decode_all(payload) {
            Ok(decoded) => Cow::Owned(decoded),
            Err(_) => Cow::Borrowed(payload),
        }
    } else if payload.len() > 2 && payload[0] == 0x1f && payload[1] == 0x8b {
        // Fallback to Gzip
        let mut decoder = GzDecoder::new(payload);
        let mut s = Vec::new();
        if decoder.read_to_end(&mut s).is_ok() {
            Cow::Owned(s)
        } else {
            Cow::Borrowed(payload)
        }
    } else {
        Cow::Borrowed(payload)
    }
}
