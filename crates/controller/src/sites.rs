use std::fmt::Write;
use std::net::Ipv4Addr;

use sha2::{Digest, Sha256};
use weave_core::{
    NetworkAttachment, NetworkListeners, NodeObservation, NodeRegistration, NodeResource,
    NodeTopology, SITE_NETWORK_PREFIX, SiteAttachment, SiteState, SrtListener, Subnet, Uplink,
};

pub(crate) fn resolve(registration: &NodeRegistration) -> NodeResource {
    let declared = &registration.node.topology;
    let mut attachments = declared.attachments.clone();
    let site = declared
        .site
        .as_ref()
        .map(|site| match locate(registration.observation.as_ref()) {
            Ok(located) => {
                attachments.push(located.attachment(site));
                located.state()
            }
            Err(unresolved) => unresolved,
        });
    NodeResource {
        node: registration
            .node
            .clone()
            .with_topology(NodeTopology { attachments }),
        site,
    }
}

struct Located {
    host: Ipv4Addr,
    subnet: Subnet,
    public: Vec<Ipv4Addr>,
    network: String,
}

fn locate(observation: Option<&NodeObservation>) -> Result<Located, SiteState> {
    let uplink = observation
        .ok_or(SiteState::NotObserved)?
        .uplink
        .as_ref()
        .ok_or(SiteState::NoUplink)?;
    let subnet =
        Subnet::containing(uplink.address, uplink.prefix_len).ok_or(SiteState::NoUplink)?;
    let mut public: Vec<Ipv4Addr> = uplink
        .reflexive
        .iter()
        .map(|reflexive| *reflexive.mapped.ip())
        .collect();
    public.sort_unstable();
    public.dedup();
    if public.is_empty() {
        return Err(SiteState::NoPublicAddress);
    }
    Ok(Located {
        host: uplink.address,
        network: site_network(subnet, &public, uplink),
        subnet,
        public,
    })
}

fn site_network(subnet: Subnet, public: &[Ipv4Addr], uplink: &Uplink) -> String {
    let mut key = subnet.to_string();
    if public != [uplink.address] {
        for address in public {
            write!(key, " {address}").expect("writing to a string cannot fail");
        }
    }
    if let Some(gateway) = uplink.gateway {
        write!(key, " via {gateway}").expect("writing to a string cannot fail");
    }
    let digest = Sha256::digest(key.as_bytes());
    let mut network = String::from(SITE_NETWORK_PREFIX);
    for byte in &digest[..6] {
        write!(network, "{byte:02x}").expect("writing to a string cannot fail");
    }
    network
}

impl Located {
    fn attachment(&self, declared: &SiteAttachment) -> NetworkAttachment {
        NetworkAttachment {
            id: declared.id.clone(),
            network: self.network.clone(),
            dial: true,
            listeners: NetworkListeners {
                srt: declared.listeners.srt.as_ref().map(|listener| SrtListener {
                    host: self.host.to_string(),
                    port_range: listener.port_range,
                }),
                ..NetworkListeners::default()
            },
        }
    }

    fn state(self) -> SiteState {
        SiteState::Resolved {
            network: self.network,
            subnet: self.subnet,
            public: self.public,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddrV4;

    use super::*;
    use weave_core::{
        DeclaredTopology, HardwareAddress, NodeCapabilities, NodeDescriptor, NodeStatus,
        PROTOCOL_VERSION, PortRange, ReflexiveAddress, SiteListener, SiteListeners,
    };

    fn mac(id: &str, observation: Option<NodeObservation>) -> NodeRegistration {
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                id: id.to_string(),
                endpoint: format!("http://{id}"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities::default(),
                topology: DeclaredTopology {
                    site: Some(SiteAttachment {
                        id: "lan".to_string(),
                        listeners: SiteListeners {
                            srt: Some(SiteListener {
                                port_range: PortRange {
                                    start: 23000,
                                    end: 23099,
                                },
                            }),
                        },
                    }),
                    attachments: vec![NetworkAttachment {
                        id: "internet".to_string(),
                        network: "internet".to_string(),
                        dial: true,
                        listeners: NetworkListeners::default(),
                    }],
                },
                inputs: Vec::new(),
                outputs: Vec::new(),
            },
            observation,
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    fn uplink(address: &str, prefix_len: u8, public: &str) -> Uplink {
        Uplink {
            interface: "en0".to_string(),
            address: address.parse().unwrap(),
            prefix_len,
            gateway: None,
            reflexive: vec![ReflexiveAddress {
                server: "192.0.2.1:3478".parse().unwrap(),
                mapped: SocketAddrV4::new(public.parse().unwrap(), 61_000),
            }],
        }
    }

    fn observed(uplink: Uplink) -> Option<NodeObservation> {
        Some(NodeObservation {
            uplink: Some(uplink),
        })
    }

    fn site_attachment(registration: &NodeRegistration) -> Option<NetworkAttachment> {
        resolve(registration)
            .node
            .topology
            .attachments
            .into_iter()
            .find(|attachment| attachment.id == "lan")
    }

    fn site_network_of(registration: &NodeRegistration) -> String {
        site_attachment(registration)
            .expect("a resolved site attachment")
            .network
    }

    #[test]
    fn nodes_behind_one_public_address_on_one_subnet_share_a_site() {
        let office_a = mac("mac-a", observed(uplink("192.168.1.20", 24, "203.0.113.7")));
        let office_b = mac("mac-b", observed(uplink("192.168.1.31", 24, "203.0.113.7")));
        let home = mac(
            "mac-c",
            observed(uplink("192.168.1.20", 24, "198.51.100.4")),
        );

        assert_eq!(site_network_of(&office_a), site_network_of(&office_b));
        assert_ne!(site_network_of(&office_a), site_network_of(&home));
    }

    #[test]
    fn a_resolved_site_listens_on_the_uplink_address() {
        let registration = mac("mac-a", observed(uplink("192.168.1.20", 24, "203.0.113.7")));
        let site = site_attachment(&registration).unwrap();

        assert_eq!(
            resolve(&registration).site,
            Some(SiteState::Resolved {
                network: site.network.clone(),
                subnet: "192.168.1.0/24".parse().unwrap(),
                public: vec![Ipv4Addr::new(203, 0, 113, 7)],
            })
        );
        assert!(site.network.starts_with("site-"));
        assert!(weave_core::validate_resource_id(&site.network).is_ok());
        assert!(site.dial);
        let listener = site.listeners.srt.unwrap();
        assert_eq!(listener.host, "192.168.1.20");
        assert_eq!(
            (listener.port_range.start, listener.port_range.end),
            (23000, 23099)
        );
    }

    #[test]
    fn every_public_address_seen_is_listed_once_in_order() {
        let mut two_servers = uplink("192.168.1.20", 24, "203.0.113.9");
        for mapped in ["198.51.100.4:61002", "203.0.113.9:61003"] {
            two_servers.reflexive.push(ReflexiveAddress {
                server: "192.0.2.2:3478".parse().unwrap(),
                mapped: mapped.parse().unwrap(),
            });
        }
        let Some(SiteState::Resolved { public, .. }) =
            resolve(&mac("mac-a", observed(two_servers))).site
        else {
            panic!("a resolved site");
        };

        assert_eq!(
            public,
            [
                Ipv4Addr::new(198, 51, 100, 4),
                Ipv4Addr::new(203, 0, 113, 9)
            ]
        );
    }

    #[test]
    fn a_new_mapped_port_keeps_the_site() {
        let first = mac("mac-a", observed(uplink("192.168.1.20", 24, "203.0.113.7")));
        let mut remapped = first.clone();
        remapped
            .observation
            .as_mut()
            .and_then(|observation| observation.uplink.as_mut())
            .unwrap()
            .reflexive[0]
            .mapped
            .set_port(62_000);

        assert_eq!(resolve(&first), resolve(&remapped));
    }

    #[test]
    fn one_carrier_address_and_one_subnet_split_by_gateway() {
        let hotspot = |gateway: &str| {
            let mut uplink = uplink("172.20.10.2", 28, "100.64.9.1");
            uplink.gateway = Some(gateway.parse::<HardwareAddress>().unwrap());
            uplink
        };
        let first = mac("mac-a", observed(hotspot("02:00:00:00:00:01")));
        let second = mac("mac-b", observed(hotspot("02:00:00:00:00:02")));

        assert_ne!(site_network_of(&first), site_network_of(&second));
    }

    #[test]
    fn hosts_on_one_unnatted_subnet_share_a_site() {
        let first = mac(
            "host-a",
            observed(uplink("203.0.113.20", 24, "203.0.113.20")),
        );
        let second = mac(
            "host-b",
            observed(uplink("203.0.113.21", 24, "203.0.113.21")),
        );

        assert_eq!(site_network_of(&first), site_network_of(&second));
    }

    #[test]
    fn an_unlocated_site_is_left_out_and_reported() {
        let mut silent = uplink("192.168.1.20", 24, "203.0.113.7");
        silent.reflexive.clear();
        for (observation, state) in [
            (None, SiteState::NotObserved),
            (Some(NodeObservation { uplink: None }), SiteState::NoUplink),
            (observed(silent), SiteState::NoPublicAddress),
        ] {
            let resource = resolve(&mac("mac-a", observation));
            assert_eq!(resource.site, Some(state));
            let networks: Vec<&str> = resource
                .node
                .topology
                .attachments
                .iter()
                .map(|attachment| attachment.network.as_str())
                .collect();
            assert_eq!(networks, ["internet"]);
        }
    }

    #[test]
    fn a_node_without_a_site_reports_no_site_state() {
        let mut registration = mac("strom-node-1", None);
        registration.node.topology.site = None;

        assert_eq!(resolve(&registration).site, None);
    }
}
