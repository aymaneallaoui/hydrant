use std::{sync::OnceLock, time::Instant};

pub const MIN_PAYLOAD_BYTES: usize = 42;
const FILLER: u8 = b'x';

static EPOCH: OnceLock<Instant> = OnceLock::new();

pub fn init_clock() {
    EPOCH.get_or_init(Instant::now);
}

#[must_use]
pub fn monotonic_nanos() -> u64 {
    u64::try_from(EPOCH.get_or_init(Instant::now).elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[must_use]
pub fn encode(send_nanos: u64, seq: u64, total_bytes: usize) -> Option<String> {
    let mut payload = format!("{send_nanos} {seq} ");
    if payload.len() > total_bytes {
        return None;
    }
    payload.extend(std::iter::repeat_n(
        FILLER as char,
        total_bytes - payload.len(),
    ));
    Some(payload)
}

#[must_use]
pub fn decode(payload: &[u8]) -> Option<(u64, u64)> {
    let mut fields = payload.split(|&b| b == b' ');
    let send_nanos = parse_u64(fields.next()?)?;
    let seq = parse_u64(fields.next()?)?;
    Some((send_nanos, seq))
}

fn parse_u64(digits: &[u8]) -> Option<u64> {
    std::str::from_utf8(digits).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_with_exact_length() {
        for total in [MIN_PAYLOAD_BYTES, 64, 1000] {
            let payload = encode(1_700_000_000_123_456_789, 42, total).expect("fits");
            assert_eq!(payload.len(), total);
            assert_eq!(
                decode(payload.as_bytes()),
                Some((1_700_000_000_123_456_789, 42))
            );
        }
    }

    #[test]
    fn worst_case_header_fits_in_minimum() {
        let payload = encode(u64::MAX, u64::MAX, MIN_PAYLOAD_BYTES).expect("fits");
        assert_eq!(payload.len(), MIN_PAYLOAD_BYTES);
        assert_eq!(decode(payload.as_bytes()), Some((u64::MAX, u64::MAX)));
    }

    #[test]
    fn rejects_too_small_total() {
        assert_eq!(encode(1_700_000_000_123_456_789, 42, 10), None);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(decode(b""), None);
        assert_eq!(decode(b"hello world"), None);
        assert_eq!(decode(b"123"), None);
        assert_eq!(decode(&[0xff, 0xfe, b' ', b'1']), None);
    }
}
