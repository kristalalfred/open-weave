use bytes::{BufMut, Bytes, BytesMut};

pub const HEADER_LENGTH: usize = 64;
pub const MAX_PAYLOAD: usize = 1024;
pub const MAX_MESSAGE: usize = HEADER_LENGTH + MAX_PAYLOAD;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub delivery_id: [u8; 16],
    pub epoch: u64,
    pub sequence: u64,
    pub origin_tai_ns: u64,
    pub origin_uncertainty_ns: u32,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("invalid timed record header")]
    Header,
    #[error("invalid timed record payload length")]
    Length,
    #[error("unauthorized timed record identity or epoch")]
    Identity,
}

impl Record {
    pub fn encode(&self) -> Result<Bytes, RecordError> {
        if self.payload.is_empty() || self.payload.len() > MAX_PAYLOAD {
            return Err(RecordError::Length);
        }
        let mut out = BytesMut::with_capacity(HEADER_LENGTH + self.payload.len());
        out.extend_from_slice(b"WVTD");
        out.put_u16(1);
        out.put_u16(0);
        out.extend_from_slice(&self.delivery_id);
        out.put_u64(self.epoch);
        out.put_u64(self.sequence);
        out.put_u64(self.origin_tai_ns);
        out.put_u32(self.origin_uncertainty_ns);
        out.put_u32(self.payload.len() as u32);
        out.put_u64(0);
        out.extend_from_slice(&self.payload);
        Ok(out.freeze())
    }

    pub fn decode(bytes: Bytes, delivery_id: [u8; 16], epoch: u64) -> Result<Self, RecordError> {
        if bytes.len() < HEADER_LENGTH
            || &bytes[..8] != b"WVTD\0\x01\0\0"
            || bytes[56..64] != [0; 8]
        {
            return Err(RecordError::Header);
        }
        let read64 = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let read32 = |offset| u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        if bytes[8..24] != delivery_id || read64(24) != epoch {
            return Err(RecordError::Identity);
        }
        let length = read32(52) as usize;
        if length == 0 || length > MAX_PAYLOAD || bytes.len() != HEADER_LENGTH + length {
            return Err(RecordError::Length);
        }
        Ok(Self {
            delivery_id,
            epoch,
            sequence: read64(32),
            origin_tai_ns: read64(40),
            origin_uncertainty_ns: read32(48),
            payload: bytes.slice(HEADER_LENGTH..),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Record {
        Record {
            delivery_id: [3; 16],
            epoch: 5,
            sequence: 7,
            origin_tai_ns: 11,
            origin_uncertainty_ns: 13,
            payload: Bytes::from_static(b"media"),
        }
    }

    #[test]
    fn wire_vector_has_exact_network_order_and_round_trips() {
        let record = sample();
        let wire = record.encode().unwrap();
        assert_eq!(&wire[..8], b"WVTD\0\x01\0\0");
        assert_eq!(&wire[8..24], &[3; 16]);
        assert_eq!(&wire[24..32], &5_u64.to_be_bytes());
        assert_eq!(&wire[32..40], &7_u64.to_be_bytes());
        assert_eq!(&wire[40..48], &11_u64.to_be_bytes());
        assert_eq!(&wire[48..52], &13_u32.to_be_bytes());
        assert_eq!(&wire[52..56], &5_u32.to_be_bytes());
        assert_eq!(&wire[56..64], &[0; 8]);
        assert_eq!(&wire[64..], b"media");
        assert_eq!(Record::decode(wire, [3; 16], 5), Ok(record));
    }

    #[test]
    fn malformed_or_unauthorized_records_never_create_payloads() {
        let wire = sample().encode().unwrap();
        for length in 0..wire.len() {
            assert!(Record::decode(wire.slice(..length), [3; 16], 5).is_err());
        }
        assert_eq!(
            Record::decode(wire.clone(), [4; 16], 5),
            Err(RecordError::Identity)
        );
        assert_eq!(
            Record::decode(wire.clone(), [3; 16], 6),
            Err(RecordError::Identity)
        );
        for index in [0, 4, 5, 6, 7, 52, 56, 63] {
            let mut changed = wire.to_vec();
            changed[index] ^= 1;
            assert!(Record::decode(changed.into(), [3; 16], 5).is_err());
        }
        let mut empty = sample();
        empty.payload = Bytes::new();
        assert_eq!(empty.encode(), Err(RecordError::Length));
        empty.payload = Bytes::from(vec![0; MAX_PAYLOAD + 1]);
        assert_eq!(empty.encode(), Err(RecordError::Length));
    }
}
