use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use tokio::net::UdpSocket;
use weave_core::ReflexiveAddress;

const HEADER_LEN: usize = 20;
const MAGIC_COOKIE: u32 = 0x2112_A442;
const BINDING_REQUEST: u16 = 0x0001;
const BINDING_SUCCESS: u16 = 0x0101;
const MAPPED_ADDRESS: u16 = 0x0001;
const XOR_MAPPED_ADDRESS: u16 = 0x0020;
const IPV4: u8 = 0x01;

pub(crate) async fn bindings(
    local: Ipv4Addr,
    servers: &[String],
    timeout: Duration,
) -> BTreeMap<String, ReflexiveAddress> {
    let socket = match UdpSocket::bind(SocketAddrV4::new(local, 0)).await {
        Ok(socket) => socket,
        Err(error) => {
            tracing::warn!(%error, %local, "cannot open a STUN socket");
            return BTreeMap::new();
        }
    };
    let mut answers = BTreeMap::new();
    for server in servers {
        match tokio::time::timeout(timeout, binding(&socket, server)).await {
            Ok(Ok(answer)) => {
                answers.insert(server.clone(), answer);
            }
            Ok(Err(error)) => tracing::debug!(server, %error, "STUN binding failed"),
            Err(_) => tracing::debug!(server, "no STUN answer"),
        }
    }
    answers
}

async fn binding(socket: &UdpSocket, server: &str) -> io::Result<ReflexiveAddress> {
    let server = tokio::net::lookup_host(server)
        .await?
        .find_map(|address| match address {
            SocketAddr::V4(address) => Some(address),
            SocketAddr::V6(_) => None,
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no IPv4 address"))?;
    let transaction = transaction_id()?;
    socket.send_to(&request(transaction), server).await?;
    let mut buffer = [0_u8; 576];
    loop {
        let (length, from) = socket.recv_from(&mut buffer).await?;
        if from == SocketAddr::V4(server)
            && let Some(mapped) = mapped_address(&buffer[..length], transaction)
        {
            return Ok(ReflexiveAddress { server, mapped });
        }
    }
}

fn transaction_id() -> io::Result<[u8; 12]> {
    let mut transaction = [0; 12];
    getrandom::fill(&mut transaction)?;
    Ok(transaction)
}

fn request(transaction: [u8; 12]) -> [u8; HEADER_LEN] {
    let mut request = [0; HEADER_LEN];
    request[..2].copy_from_slice(&BINDING_REQUEST.to_be_bytes());
    request[4..8].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
    request[8..].copy_from_slice(&transaction);
    request
}

fn mapped_address(response: &[u8], transaction: [u8; 12]) -> Option<SocketAddrV4> {
    let (header, mut attributes) = response.split_first_chunk::<HEADER_LEN>()?;
    if u16::from_be_bytes([header[0], header[1]]) != BINDING_SUCCESS
        || usize::from(u16::from_be_bytes([header[2], header[3]])) != attributes.len()
        || header[4..8] != MAGIC_COOKIE.to_be_bytes()
        || header[8..] != transaction
    {
        return None;
    }
    let mut mapped = None;
    while let Some((head, rest)) = attributes.split_first_chunk::<4>() {
        let kind = u16::from_be_bytes([head[0], head[1]]);
        let length = usize::from(u16::from_be_bytes([head[2], head[3]]));
        let value = rest.get(..length)?;
        match kind {
            XOR_MAPPED_ADDRESS => {
                if let Some(address) = address(value) {
                    return Some(unxor(address));
                }
            }
            MAPPED_ADDRESS => mapped = mapped.or_else(|| address(value)),
            _ => {}
        }
        attributes = rest.get(length.next_multiple_of(4)..)?;
    }
    mapped
}

fn address(value: &[u8]) -> Option<SocketAddrV4> {
    let &[_, IPV4, port_high, port_low, a, b, c, d] = value else {
        return None;
    };
    Some(SocketAddrV4::new(
        Ipv4Addr::new(a, b, c, d),
        u16::from_be_bytes([port_high, port_low]),
    ))
}

fn unxor(address: SocketAddrV4) -> SocketAddrV4 {
    let [cookie_high, cookie_low, ..] = MAGIC_COOKIE.to_be_bytes();
    SocketAddrV4::new(
        Ipv4Addr::from_bits(address.ip().to_bits() ^ MAGIC_COOKIE),
        address.port() ^ u16::from_be_bytes([cookie_high, cookie_low]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const RFC_5769_TRANSACTION: [u8; 12] = [
        0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae,
    ];

    const RFC_5769_IPV4_RESPONSE: [u8; 80] = [
        0x01, 0x01, 0x00, 0x3c, 0x21, 0x12, 0xa4, 0x42, 0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6,
        0x86, 0xfa, 0x87, 0xdf, 0xae, 0x80, 0x22, 0x00, 0x0b, 0x74, 0x65, 0x73, 0x74, 0x20, 0x76,
        0x65, 0x63, 0x74, 0x6f, 0x72, 0x20, 0x00, 0x20, 0x00, 0x08, 0x00, 0x01, 0xa1, 0x47, 0xe1,
        0x12, 0xa6, 0x43, 0x00, 0x08, 0x00, 0x14, 0x2b, 0x91, 0xf5, 0x99, 0xfd, 0x9e, 0x90, 0xc3,
        0x8c, 0x74, 0x89, 0xf9, 0x2a, 0xf9, 0xba, 0x53, 0xf0, 0x6b, 0xe7, 0xd7, 0x80, 0x28, 0x00,
        0x04, 0xc0, 0x7d, 0x4c, 0x96,
    ];

    fn message(kind: u16, transaction: [u8; 12], attributes: &[(u16, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for (attribute, value) in attributes {
            body.extend_from_slice(&attribute.to_be_bytes());
            body.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
            body.extend_from_slice(value);
            body.resize(body.len().next_multiple_of(4), 0);
        }
        let mut message = Vec::new();
        message.extend_from_slice(&kind.to_be_bytes());
        message.extend_from_slice(&u16::try_from(body.len()).unwrap().to_be_bytes());
        message.extend_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
        message.extend_from_slice(&transaction);
        message.extend_from_slice(&body);
        message
    }

    fn xor_mapped(mapped: SocketAddrV4) -> [u8; 8] {
        let [port_high, port_low] = (mapped.port() ^ 0x2112).to_be_bytes();
        let [a, b, c, d] = (mapped.ip().to_bits() ^ 0x2112_a442).to_be_bytes();
        [0, 1, port_high, port_low, a, b, c, d]
    }

    #[test]
    fn the_rfc_5769_sample_response_maps_to_192_0_2_1_port_32853() {
        assert_eq!(
            mapped_address(&RFC_5769_IPV4_RESPONSE, RFC_5769_TRANSACTION),
            Some("192.0.2.1:32853".parse().unwrap())
        );
    }

    #[test]
    fn a_response_to_another_transaction_is_ignored() {
        let mut other = RFC_5769_TRANSACTION;
        other[11] ^= 1;

        assert_eq!(mapped_address(&RFC_5769_IPV4_RESPONSE, other), None);
    }

    #[test]
    fn only_a_binding_success_response_is_accepted() {
        for kind in [[0x00, 0x01], [0x01, 0x11], [0x00, 0x11], [0x01, 0x02]] {
            let mut response = RFC_5769_IPV4_RESPONSE;
            response[..2].copy_from_slice(&kind);

            assert_eq!(mapped_address(&response, RFC_5769_TRANSACTION), None);
        }
    }

    #[test]
    fn a_response_without_the_magic_cookie_is_ignored() {
        let mut response = RFC_5769_IPV4_RESPONSE;
        response[4..8].copy_from_slice(&[0; 4]);

        assert_eq!(mapped_address(&response, RFC_5769_TRANSACTION), None);
    }

    #[test]
    fn every_truncation_of_a_response_is_ignored() {
        for length in 0..RFC_5769_IPV4_RESPONSE.len() {
            assert_eq!(
                mapped_address(&RFC_5769_IPV4_RESPONSE[..length], RFC_5769_TRANSACTION),
                None,
                "truncated to {length} bytes"
            );
        }
    }

    #[test]
    fn an_attribute_that_overruns_the_message_is_ignored() {
        let mapped = [0, 1, 0x1f, 0x90, 203, 0, 113, 7];
        for overrun in [[0x00, 0x0c], [0xff, 0xff]] {
            let mut response = message(
                BINDING_SUCCESS,
                RFC_5769_TRANSACTION,
                &[(MAPPED_ADDRESS, &mapped), (0x8022, b"overrun!")],
            );
            response[34..36].copy_from_slice(&overrun);

            assert_eq!(mapped_address(&response, RFC_5769_TRANSACTION), None);
        }
    }

    #[test]
    fn xor_mapped_address_is_preferred_over_mapped_address() {
        let xor = xor_mapped("198.51.100.4:61000".parse().unwrap());
        let plain = [0, 1, 0x1f, 0x90, 203, 0, 113, 7];
        let response = message(
            BINDING_SUCCESS,
            RFC_5769_TRANSACTION,
            &[(MAPPED_ADDRESS, &plain), (XOR_MAPPED_ADDRESS, &xor)],
        );

        assert_eq!(
            mapped_address(&response, RFC_5769_TRANSACTION),
            Some("198.51.100.4:61000".parse().unwrap())
        );
    }

    #[test]
    fn mapped_address_is_the_fallback_without_xor_mapped_address() {
        let response = message(
            BINDING_SUCCESS,
            RFC_5769_TRANSACTION,
            &[
                (0x8022, b"server"),
                (MAPPED_ADDRESS, &[0, 1, 0x1f, 0x90, 203, 0, 113, 7]),
            ],
        );

        assert_eq!(
            mapped_address(&response, RFC_5769_TRANSACTION),
            Some("203.0.113.7:8080".parse().unwrap())
        );
    }

    #[test]
    fn an_ipv6_mapped_address_is_not_an_answer() {
        let mut ipv6 = vec![0, 2, 0x1f, 0x90];
        ipv6.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8].repeat(4));
        let response = message(
            BINDING_SUCCESS,
            RFC_5769_TRANSACTION,
            &[(XOR_MAPPED_ADDRESS, &ipv6), (MAPPED_ADDRESS, &ipv6)],
        );

        assert_eq!(mapped_address(&response, RFC_5769_TRANSACTION), None);
    }

    #[test]
    fn every_single_byte_corruption_parses_without_panicking() {
        for position in 0..RFC_5769_IPV4_RESPONSE.len() {
            for byte in 0..=u8::MAX {
                let mut response = RFC_5769_IPV4_RESPONSE;
                response[position] = byte;
                let _ = mapped_address(&response, RFC_5769_TRANSACTION);
            }
        }
    }

    #[test]
    fn a_binding_request_is_a_bare_rfc_8489_header() {
        let mut expected = vec![0x00, 0x01, 0x00, 0x00, 0x21, 0x12, 0xa4, 0x42];
        expected.extend_from_slice(&RFC_5769_TRANSACTION);

        assert_eq!(request(RFC_5769_TRANSACTION).to_vec(), expected);
    }

    #[tokio::test]
    async fn an_answer_is_kept_under_the_configured_server_name() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let configured = server.local_addr().unwrap().to_string();
        let answering = tokio::spawn(async move {
            let mut buffer = [0; 576];
            let (length, from) = server.recv_from(&mut buffer).await.unwrap();
            let SocketAddr::V4(from) = from else {
                panic!("{from} is not IPv4");
            };
            assert_eq!(length, HEADER_LEN);
            let transaction = buffer[8..HEADER_LEN].try_into().unwrap();
            let response = message(
                BINDING_SUCCESS,
                transaction,
                &[(XOR_MAPPED_ADDRESS, &xor_mapped(from))],
            );
            server.send_to(&response, from).await.unwrap();
            from
        });

        let answers = bindings(
            Ipv4Addr::LOCALHOST,
            std::slice::from_ref(&configured),
            Duration::from_secs(5),
        )
        .await;
        let client = answering.await.unwrap();

        assert_eq!(
            answers,
            BTreeMap::from([(
                configured.clone(),
                ReflexiveAddress {
                    server: configured.parse().unwrap(),
                    mapped: client,
                }
            )])
        );
    }

    #[tokio::test]
    async fn each_silent_server_is_given_up_on_after_the_timeout() {
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server = silent.local_addr().unwrap().to_string();
        let servers = [server.clone(), server];

        let answers = tokio::time::timeout(
            Duration::from_secs(3),
            bindings(Ipv4Addr::LOCALHOST, &servers, Duration::from_millis(100)),
        )
        .await;

        assert_eq!(answers.unwrap(), BTreeMap::new());
    }
}
