use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    DeclaredNode, FormatConstraint, HopEndpointClass, LocalEnd, LocalEndpoint, MediaFormat,
    NodeInput, NodeObservation, NodeOutput, Passphrase, SITE_NETWORK_PREFIX, SignallingEndpoint,
    SrtEndpoint, StreamDefinition, StreamDestination, StreamTransport,
};

pub const RESOURCE_ID_MAX_LEN: usize = 63;
const MAX_REFLEXIVE_ADDRESSES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceIdError {
    #[error("must be between 1 and {RESOURCE_ID_MAX_LEN} characters")]
    InvalidLength,
    #[error(
        "must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
    )]
    InvalidCharacters,
}

pub fn validate_resource_id(value: &str) -> Result<(), ResourceIdError> {
    if value.is_empty() || value.chars().count() > RESOURCE_ID_MAX_LEN {
        return Err(ResourceIdError::InvalidLength);
    }

    let mut bytes = value.bytes();
    let first = bytes.next().expect("non-empty resource id");
    let last = value.as_bytes()[value.len() - 1];
    if !is_alphanumeric(first)
        || !is_alphanumeric(last)
        || !bytes.all(|byte| is_alphanumeric(byte) || byte == b'-')
    {
        return Err(ResourceIdError::InvalidCharacters);
    }

    Ok(())
}

fn is_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}

impl ValidationIssue {
    pub fn new(
        field: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

#[must_use]
pub fn resource_id_issue(field: &str, label: &str, error: ResourceIdError) -> ValidationIssue {
    let code = match error {
        ResourceIdError::InvalidLength => "invalid_length",
        ResourceIdError::InvalidCharacters => "invalid_characters",
    };
    ValidationIssue::new(field, code, format!("{label} {error}"))
}

#[must_use]
pub fn validate_stream(stream: &StreamDefinition) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    validate_id(&stream.name, "name", "stream name", &mut issues);
    if stream.destinations.is_empty() {
        issues.push(ValidationIssue::new(
            "destinations",
            "required",
            "stream must have at least one destination",
        ));
    }

    validate_transport(&stream.source, "source", true, &mut issues);
    let mut destination_ids = HashSet::new();
    for (index, destination) in stream.destinations.iter().enumerate() {
        let path = format!("destinations[{index}]");
        validate_id(
            &destination.id,
            &format!("{path}.id"),
            "destination id",
            &mut issues,
        );
        if !destination_ids.insert(&destination.id) {
            issues.push(ValidationIssue::new(
                format!("{path}.id"),
                "duplicate",
                "destination id must be unique within the stream",
            ));
        }
        validate_transport(&destination.endpoint, &path, false, &mut issues);
        validate_paths(destination, &path, &mut issues);
        if let Some(delivery) = &destination.delivery {
            for (field, message) in delivery.validate() {
                issues.push(ValidationIssue::new(
                    format!("{path}.delivery.{field}"),
                    "invalid_range",
                    message,
                ));
            }
            if !matches!(&stream.source, StreamTransport::Srt(source) if source.node.is_some())
                || !matches!(&destination.endpoint, StreamTransport::Srt(endpoint) if endpoint.node.is_some())
            {
                issues.push(ValidationIssue::new(
                    format!("{path}.delivery"),
                    "not_allowed",
                    "timed delivery requires managed SRT source and destination endpoints",
                ));
            }
        }
    }

    issues
}

fn validate_paths(destination: &StreamDestination, path: &str, issues: &mut Vec<ValidationIssue>) {
    let field = format!("{path}.paths");
    if !(1..=2).contains(&destination.paths) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_range",
            "paths must be 1 or 2",
        ));
        return;
    }
    if destination.paths == 1 {
        return;
    }
    let StreamTransport::Srt(endpoint) = &destination.endpoint else {
        return;
    };
    if endpoint.remote.is_some() {
        issues.push(ValidationIssue::new(
            field,
            "not_allowed",
            "a remote destination has no receiver to merge two paths",
        ));
    } else if !endpoint.via.is_empty() {
        issues.push(ValidationIssue::new(
            field,
            "not_allowed",
            "a destination with two paths must not pin via",
        ));
    }
}

#[must_use]
pub fn validate_node(node: &DeclaredNode) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    validate_id(&node.id, "node.id", "node id", &mut issues);
    let mut profile_ids = HashSet::new();
    for (index, profile) in node.capabilities.hop_profiles.iter().enumerate() {
        let field = format!("node.capabilities.hop_profiles[{index}].id");
        validate_id(&profile.id, &field, "profile id", &mut issues);
        if !profile_ids.insert(&profile.id) {
            issues.push(ValidationIssue::new(
                field,
                "duplicate",
                "profile id must be unique within the node",
            ));
        }
        if profile.max_egresses == Some(0) {
            issues.push(ValidationIssue::new(
                format!("node.capabilities.hop_profiles[{index}].max_egresses"),
                "invalid_range",
                "max_egresses must be at least one",
            ));
        }
        if let Some(delivery) = &profile.timed_delivery {
            let base = format!("node.capabilities.hop_profiles[{index}].timed_delivery");
            if delivery.protocol != crate::delivery::TIMED_PROTOCOL
                || delivery.roles.is_empty()
                || delivery.max_latency_ms == 0
                || delivery.origin_epoch == 0
                || delivery.clock_domain.is_empty()
            {
                issues.push(ValidationIssue::new(base, "invalid_capability", "timed delivery requires the supported protocol, roles, positive limits, origin epoch and clock domain"));
            }
        }
        if let Some(accepts) = &profile.accepts {
            validate_constraint(
                accepts,
                &format!("node.capabilities.hop_profiles[{index}].accepts"),
                &mut issues,
            );
        }
        for (end, class, wrong) in [
            ("ingress", &profile.ingress, LocalEnd::Output),
            ("egress", &profile.egress, LocalEnd::Input),
        ] {
            if *class == HopEndpointClass::Local(wrong) {
                issues.push(ValidationIssue::new(
                    format!("node.capabilities.hop_profiles[{index}].{end}"),
                    "wrong_end",
                    format!("a hop {end} cannot be an {wrong}"),
                ));
            }
        }
    }
    issues.extend(validate_local_io(&node.inputs, &node.outputs, "node."));
    let mut attachment_ids = HashSet::new();
    if let Some(site) = &node.topology.site {
        validate_id(
            &site.id,
            "node.topology.site.id",
            "attachment id",
            &mut issues,
        );
        attachment_ids.insert(&site.id);
        if let Some(listener) = &site.listeners.srt
            && listener.port_range.start > listener.port_range.end
        {
            issues.push(ValidationIssue::new(
                "node.topology.site.listeners.srt.port_range",
                "invalid_range",
                "port range start must not exceed end",
            ));
        }
    }
    for (index, attachment) in node.topology.attachments.iter().enumerate() {
        let base = format!("node.topology.attachments[{index}]");
        validate_id(
            &attachment.id,
            &format!("{base}.id"),
            "attachment id",
            &mut issues,
        );
        validate_id(
            &attachment.network,
            &format!("{base}.network"),
            "network id",
            &mut issues,
        );
        if attachment.network.starts_with(SITE_NETWORK_PREFIX) {
            issues.push(ValidationIssue::new(
                format!("{base}.network"),
                "reserved",
                format!(
                    "network ids starting with {SITE_NETWORK_PREFIX} are reserved for derived sites; rename the network, or declare topology.site for the controller to derive it"
                ),
            ));
        }
        if !attachment_ids.insert(&attachment.id) {
            issues.push(ValidationIssue::new(
                format!("{base}.id"),
                "duplicate",
                "attachment id must be unique within the node",
            ));
        }
        if let Some(listener) = &attachment.listeners.srt {
            if listener.host.trim().is_empty() {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.srt.host"),
                    "blank",
                    "SRT listener host must not be blank",
                ));
            }
            if listener.port_range.start > listener.port_range.end {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.srt.port_range"),
                    "invalid_range",
                    "port range start must not exceed end",
                ));
            }
        }
        if let Some(listener) = &attachment.listeners.rist {
            if listener.host.trim().is_empty() {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.rist.host"),
                    "blank",
                    "RIST listener host must not be blank",
                ));
            }
            if listener.port_range.rist_pairs().next().is_none() {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.rist.port_range"),
                    "invalid_range",
                    "RIST port range must hold an even port and the port after it",
                ));
            }
        }
    }
    issues
}

#[must_use]
pub fn validate_observation(observation: &NodeObservation) -> Vec<ValidationIssue> {
    let Some(uplink) = &observation.uplink else {
        return Vec::new();
    };
    let mut issues = Vec::new();
    if uplink.interface.trim().is_empty() {
        issues.push(ValidationIssue::new(
            "observation.uplink.interface",
            "blank",
            "uplink interface must not be blank",
        ));
    }
    let address = uplink.address;
    if address.is_unspecified()
        || address.is_loopback()
        || address.is_multicast()
        || address.is_broadcast()
    {
        issues.push(ValidationIssue::new(
            "observation.uplink.address",
            "not_routable",
            format!("{address} cannot be a node's uplink address"),
        ));
    }
    if uplink.prefix_len > 32 {
        issues.push(ValidationIssue::new(
            "observation.uplink.prefix_len",
            "invalid_range",
            format!("an IPv4 address has no /{} prefix", uplink.prefix_len),
        ));
    }
    if uplink.reflexive.len() > MAX_REFLEXIVE_ADDRESSES {
        issues.push(ValidationIssue::new(
            "observation.uplink.reflexive",
            "too_many",
            format!("at most {MAX_REFLEXIVE_ADDRESSES} reflexive addresses"),
        ));
    }
    for (index, reflexive) in uplink.reflexive.iter().enumerate() {
        if reflexive.mapped.ip().is_unspecified() {
            issues.push(ValidationIssue::new(
                format!("observation.uplink.reflexive[{index}].mapped"),
                "not_routable",
                "a mapped address cannot be 0.0.0.0",
            ));
        }
    }
    issues
}

#[must_use]
pub fn validate_local_io(
    inputs: &[NodeInput],
    outputs: &[NodeOutput],
    base: &str,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut ids = HashSet::new();
    for (index, input) in inputs.iter().enumerate() {
        let field = format!("{base}inputs[{index}]");
        validate_local(&input.id, &input.label, &field, &mut ids, &mut issues);
        let problem = if input.tracks.is_empty() {
            Some(("empty", "tracks must name at least one track"))
        } else if input.tracks.iter().collect::<HashSet<_>>().len() != input.tracks.len() {
            Some(("duplicate", "each track may be named once"))
        } else {
            None
        };
        if let Some((code, message)) = problem {
            issues.push(ValidationIssue::new(
                format!("{field}.tracks"),
                code,
                message,
            ));
        }
    }
    let mut ids = HashSet::new();
    for (index, output) in outputs.iter().enumerate() {
        let field = format!("{base}outputs[{index}]");
        validate_local(&output.id, &output.label, &field, &mut ids, &mut issues);
    }
    issues
}

fn validate_local<'a>(
    id: &'a str,
    label: &str,
    field: &str,
    ids: &mut HashSet<&'a str>,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_id(id, &format!("{field}.id"), "id", issues);
    if !ids.insert(id) {
        issues.push(ValidationIssue::new(
            format!("{field}.id"),
            "duplicate",
            "id must be unique among the node's inputs or outputs",
        ));
    }
    if label.trim().is_empty() {
        issues.push(ValidationIssue::new(
            format!("{field}.label"),
            "blank",
            "label must not be blank",
        ));
    }
}

fn validate_transport(
    transport: &StreamTransport,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    match transport {
        StreamTransport::Srt(endpoint) => {
            validate_srt(endpoint, &format!("{path}.srt"), is_source, issues);
        }
        StreamTransport::Input(endpoint) => {
            let path = format!("{path}.input");
            if !is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "source_only",
                    "input belongs on the source, not a destination",
                ));
            }
            validate_local_endpoint(endpoint, &path, issues);
        }
        StreamTransport::Output(endpoint) => {
            let path = format!("{path}.output");
            if is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "destination_only",
                    "output belongs on a destination, not the source",
                ));
            }
            validate_local_endpoint(endpoint, &path, issues);
        }
        StreamTransport::Whip(endpoint) => {
            let path = format!("{path}.whip");
            if !is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "source_only",
                    "whip belongs on the source, not a destination",
                ));
            }
            validate_signalling(endpoint, &path, is_source, issues);
        }
        StreamTransport::Whep(endpoint) => {
            let path = format!("{path}.whep");
            if is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "destination_only",
                    "whep belongs on a destination, not the source",
                ));
            }
            validate_signalling(endpoint, &path, is_source, issues);
        }
    }
}

fn validate_local_endpoint(
    endpoint: &LocalEndpoint,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_id(&endpoint.node, &format!("{path}.node"), "node id", issues);
    validate_id(&endpoint.id, &format!("{path}.id"), "id", issues);
    if let Some(network) = &endpoint.network {
        validate_id(network, &format!("{path}.network"), "network id", issues);
    }
}

fn validate_signalling(
    endpoint: &SignallingEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_id(&endpoint.node, &format!("{path}.node"), "node id", issues);
    if let Some(network) = &endpoint.network {
        validate_id(network, &format!("{path}.network"), "network id", issues);
    }
    validate_format(
        endpoint.format.as_ref(),
        endpoint.accepts.as_ref(),
        path,
        is_source,
        issues,
    );
}

fn validate_srt(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_via(endpoint, path, is_source, issues);
    validate_format(
        endpoint.format.as_ref(),
        endpoint.accepts.as_ref(),
        path,
        is_source,
        issues,
    );
    if let Some(passphrase) = &endpoint.passphrase {
        validate_passphrase(passphrase, &format!("{path}.passphrase"), issues);
    }
    if let Some(network) = &endpoint.network {
        validate_id(network, &format!("{path}.network"), "network id", issues);
    }

    match (&endpoint.node, &endpoint.remote) {
        (Some(_), Some(_)) => issues.push(ValidationIssue::new(
            path,
            "mutually_exclusive",
            "endpoint must set either node or remote, not both",
        )),
        (None, None) => issues.push(ValidationIssue::new(
            path,
            "required",
            "endpoint must set either node or remote",
        )),
        _ => {}
    }

    if let Some(node) = &endpoint.node {
        validate_id(node, &format!("{path}.node"), "node id", issues);
    }

    if let Some(remote) = &endpoint.remote {
        if is_source {
            issues.push(ValidationIssue::new(
                format!("{path}.remote"),
                "not_allowed",
                "source must be a node, not a remote endpoint",
            ));
        }
        if remote.host.trim().is_empty() {
            issues.push(ValidationIssue::new(
                format!("{path}.remote.host"),
                "blank",
                "remote host must not be empty",
            ));
        }
        validate_id(
            &remote.network,
            &format!("{path}.remote.network"),
            "network id",
            issues,
        );
        if endpoint.network.is_some() {
            issues.push(ValidationIssue::new(
                format!("{path}.network"),
                "not_allowed",
                "a remote endpoint declares its network inside remote",
            ));
        }
    }
}

fn validate_via(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_source && !endpoint.via.is_empty() {
        issues.push(ValidationIssue::new(
            format!("{path}.via"),
            "destination_only",
            "via belongs on a destination, not the source",
        ));
    }

    let mut seen = HashSet::new();
    for (index, node) in endpoint.via.iter().enumerate() {
        validate_id(node, &format!("{path}.via[{index}]"), "node id", issues);
        if !seen.insert(node) {
            issues.push(ValidationIssue::new(
                format!("{path}.via[{index}]"),
                "duplicate",
                "via must not repeat a node",
            ));
        }
    }
}

fn validate_passphrase(passphrase: &Passphrase, field: &str, issues: &mut Vec<ValidationIssue>) {
    let value = passphrase.expose();
    if !(Passphrase::MIN_LEN..=Passphrase::MAX_LEN).contains(&value.len()) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_length",
            format!(
                "passphrase must be between {} and {} bytes",
                Passphrase::MIN_LEN,
                Passphrase::MAX_LEN
            ),
        ));
    }
    if !value.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_characters",
            "passphrase must be printable ASCII, space to ~",
        ));
    }
}

fn validate_id(value: &str, field: &str, label: &str, issues: &mut Vec<ValidationIssue>) {
    if let Err(error) = validate_resource_id(value) {
        issues.push(resource_id_issue(field, label, error));
    }
}

fn validate_format(
    format: Option<&MediaFormat>,
    accepts: Option<&FormatConstraint>,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_source && accepts.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.accepts"),
            "destination_only",
            "accepts belongs on a destination, not the source",
        ));
    }
    if !is_source && format.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.format"),
            "source_only",
            "format belongs on the source, not a destination",
        ));
    }
    if let Some(accepts) = accepts {
        validate_constraint(accepts, &format!("{path}.accepts"), issues);
    }
}

fn validate_constraint(accepts: &FormatConstraint, path: &str, issues: &mut Vec<ValidationIssue>) {
    fn check<T>(values: &Option<Vec<T>>, path: String, issues: &mut Vec<ValidationIssue>) {
        if values.as_ref().is_some_and(Vec::is_empty) {
            issues.push(ValidationIssue::new(
                path,
                "empty",
                "accepts must not contain an empty list of values",
            ));
        }
    }

    check(&accepts.container, format!("{path}.container"), issues);
    if let Some(video) = &accepts.video {
        check(&video.codec, format!("{path}.video.codec"), issues);
        check(&video.width, format!("{path}.video.width"), issues);
        check(&video.height, format!("{path}.video.height"), issues);
        check(&video.framerate, format!("{path}.video.framerate"), issues);
        check(
            &video.chroma_subsampling,
            format!("{path}.video.chroma_subsampling"),
            issues,
        );
    }
    if let Some(audio) = &accepts.audio {
        check(&audio.codec, format!("{path}.audio.codec"), issues);
        check(
            &audio.sample_rate,
            format!("{path}.audio.sample_rate"),
            issues,
        );
        check(&audio.channels, format!("{path}.audio.channels"), issues);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AudioConstraint, Container, DeclaredNode, FormatConstraint, LocalEndpoint, MediaFormat,
        NodeDescriptor, NodeInput, NodeObservation, NodeOutput, ReflexiveAddress, RemoteAddr,
        SrtEndpoint, StreamDefinition, StreamDestination, StreamTransport, Track, Uplink,
        VideoConstraint,
    };

    use super::{
        RESOURCE_ID_MAX_LEN, ResourceIdError, ValidationIssue, validate_node, validate_observation,
        validate_resource_id, validate_stream,
    };

    fn srt_node(node: &str) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        }
    }

    fn destination(id: &str, endpoint: StreamTransport) -> StreamDestination {
        StreamDestination {
            delivery: None,
            id: id.to_string(),
            paths: 1,
            endpoint,
        }
    }

    fn stream() -> StreamDefinition {
        StreamDefinition {
            name: "camera".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(srt_node("source")),
            destinations: vec![destination(
                "destination",
                StreamTransport::Srt(srt_node("destination")),
            )],
        }
    }

    fn issue(field: &str, code: &str, message: &str) -> ValidationIssue {
        ValidationIssue {
            field: field.to_string(),
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    fn local(node: &str, id: &str) -> LocalEndpoint {
        LocalEndpoint {
            node: node.to_string(),
            id: id.to_string(),
            network: None,
        }
    }

    fn local_stream(source: StreamTransport, destination_end: StreamTransport) -> StreamDefinition {
        StreamDefinition {
            name: "music".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source,
            destinations: vec![destination("desk", destination_end)],
        }
    }

    #[test]
    fn valid_srt_and_local_streams_have_no_issues() {
        assert!(validate_stream(&stream()).is_empty());
        assert!(
            validate_stream(&local_stream(
                StreamTransport::Input(local("alfreds-mac", "music")),
                StreamTransport::Output(local("filips-mac", "speakers")),
            ))
            .is_empty()
        );
    }

    #[test]
    fn an_input_is_only_a_source_and_an_output_only_a_destination() {
        let issues = validate_stream(&local_stream(
            StreamTransport::Output(local("alfreds-mac", "speakers")),
            StreamTransport::Input(local("filips-mac", "Music")),
        ));
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                ("source.output", "destination_only"),
                ("destinations[0].input", "source_only"),
                ("destinations[0].input.id", "invalid_characters"),
            ]
        );
    }

    #[test]
    fn stream_definition_rejects_unknown_fields() {
        let parsed = serde_json::from_value::<StreamDefinition>(serde_json::json!({
            "name": "camera",
            "enabled": true,
            "enabld": true,
            "source": { "srt": { "node": "source" } },
            "destinations": [{ "id": "destination", "srt": { "node": "destination" } }]
        }));

        assert!(
            parsed
                .unwrap_err()
                .to_string()
                .contains("unknown field `enabld`")
        );
    }

    #[test]
    fn validation_collects_issues_with_stable_paths() {
        let mut invalid = stream();
        invalid.name = "  ".to_string();
        let StreamTransport::Srt(source) = &mut invalid.source else {
            unreachable!();
        };
        source.node = Some(String::new());
        source.remote = Some(RemoteAddr {
            host: " ".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });
        source.via = vec!["relay".to_string(), "relay".to_string()];
        source.accepts = Some(FormatConstraint::default());

        let issues = validate_stream(&invalid);
        assert_eq!(
            issues,
            vec![
                issue(
                    "name",
                    "invalid_characters",
                    "stream name must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
                issue(
                    "source.srt.via",
                    "destination_only",
                    "via belongs on a destination, not the source",
                ),
                issue(
                    "source.srt.via[1]",
                    "duplicate",
                    "via must not repeat a node",
                ),
                issue(
                    "source.srt.accepts",
                    "destination_only",
                    "accepts belongs on a destination, not the source",
                ),
                issue(
                    "source.srt",
                    "mutually_exclusive",
                    "endpoint must set either node or remote, not both",
                ),
                issue(
                    "source.srt.node",
                    "invalid_length",
                    "node id must be between 1 and 63 characters",
                ),
                issue(
                    "source.srt.remote",
                    "not_allowed",
                    "source must be a node, not a remote endpoint",
                ),
                issue(
                    "source.srt.remote.host",
                    "blank",
                    "remote host must not be empty",
                ),
            ]
        );
    }

    #[test]
    fn missing_destination_and_endpoint_identity_are_reported() {
        let mut invalid = stream();
        invalid.destinations.clear();
        invalid.source = StreamTransport::Srt(SrtEndpoint {
            node: None,
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        });

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations",
                    "required",
                    "stream must have at least one destination",
                ),
                issue(
                    "source.srt",
                    "required",
                    "endpoint must set either node or remote",
                ),
            ]
        );
    }

    #[test]
    fn destination_roles_and_node_fields_are_validated() {
        let mut invalid = stream();
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.node = None;
        endpoint.remote = Some(RemoteAddr {
            host: String::new(),
            port: 9000,
            network: "internet".to_string(),
        });
        endpoint.format = Some(MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: None,
        });
        invalid.destinations.push(destination(
            "screen",
            StreamTransport::Output(local(" ", "screen")),
        ));

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations[0].srt.format",
                    "source_only",
                    "format belongs on the source, not a destination",
                ),
                issue(
                    "destinations[0].srt.remote.host",
                    "blank",
                    "remote host must not be empty",
                ),
                issue(
                    "destinations[1].output.node",
                    "invalid_characters",
                    "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
            ]
        );
    }

    #[test]
    fn every_empty_constraint_list_is_reported() {
        let mut invalid = stream();
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.accepts = Some(FormatConstraint {
            container: Some(Vec::new()),
            video: Some(VideoConstraint {
                codec: Some(Vec::new()),
                width: Some(Vec::new()),
                height: Some(Vec::new()),
                framerate: Some(Vec::new()),
                chroma_subsampling: Some(Vec::new()),
            }),
            audio: Some(AudioConstraint {
                codec: Some(Vec::new()),
                sample_rate: Some(Vec::new()),
                channels: Some(Vec::new()),
            }),
        });

        let issues = validate_stream(&invalid);
        let fields: Vec<&str> = issues.iter().map(|issue| issue.field.as_str()).collect();
        assert_eq!(
            fields,
            vec![
                "destinations[0].srt.accepts.container",
                "destinations[0].srt.accepts.video.codec",
                "destinations[0].srt.accepts.video.width",
                "destinations[0].srt.accepts.video.height",
                "destinations[0].srt.accepts.video.framerate",
                "destinations[0].srt.accepts.video.chroma_subsampling",
                "destinations[0].srt.accepts.audio.codec",
                "destinations[0].srt.accepts.audio.sample_rate",
                "destinations[0].srt.accepts.audio.channels",
            ]
        );
        assert!(issues.iter().all(|issue| issue.code == "empty"
            && issue.message == "accepts must not contain an empty list of values"));
    }

    #[test]
    fn blank_and_duplicate_via_entries_are_each_identified() {
        let mut invalid = stream();
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.via = vec![" ".to_string(), "relay".to_string(), "relay".to_string()];

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations[0].srt.via[0]",
                    "invalid_characters",
                    "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
                issue(
                    "destinations[0].srt.via[2]",
                    "duplicate",
                    "via must not repeat a node",
                ),
            ]
        );
    }

    #[test]
    fn resource_ids_have_one_url_safe_grammar() {
        for valid in ["a", "0", "camera-1", &"a".repeat(RESOURCE_ID_MAX_LEN)] {
            assert_eq!(validate_resource_id(valid), Ok(()), "{valid}");
        }

        for invalid in [
            "Camera", "camera_1", "camera/1", "camera.1", "-camera", "camera-", "café",
        ] {
            assert_eq!(
                validate_resource_id(invalid),
                Err(ResourceIdError::InvalidCharacters),
                "{invalid}"
            );
        }
        assert_eq!(
            validate_resource_id(""),
            Err(ResourceIdError::InvalidLength)
        );
        assert_eq!(
            validate_resource_id(&"a".repeat(RESOURCE_ID_MAX_LEN + 1)),
            Err(ResourceIdError::InvalidLength)
        );
    }

    #[test]
    fn every_manifest_resource_reference_uses_the_id_grammar() {
        let mut invalid = stream();
        invalid.name = "camera/main".to_string();
        let StreamTransport::Srt(source) = &mut invalid.source else {
            unreachable!()
        };
        source.node = Some("Source".to_string());
        source.network = Some("studio_lan".to_string());
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!()
        };
        endpoint.node = Some("destination_one".to_string());
        endpoint.via = vec!["relay.one".to_string()];
        invalid.destinations.push(destination(
            "display/one",
            StreamTransport::Output(local("display/one", "screen")),
        ));

        let issues = validate_stream(&invalid);
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                ("name", "invalid_characters"),
                ("source.srt.network", "invalid_characters"),
                ("source.srt.node", "invalid_characters"),
                ("destinations[0].srt.via[0]", "invalid_characters"),
                ("destinations[0].srt.node", "invalid_characters"),
                ("destinations[1].id", "invalid_characters"),
                ("destinations[1].output.node", "invalid_characters"),
            ]
        );
    }

    #[test]
    fn two_paths_need_a_receiver_and_no_via() {
        let mut two = stream();
        two.destinations[0].paths = 2;
        assert!(validate_stream(&two).is_empty());

        let mut out_of_range = stream();
        out_of_range.destinations[0].paths = 3;
        assert_eq!(
            validate_stream(&out_of_range),
            [issue(
                "destinations[0].paths",
                "invalid_range",
                "paths must be 1 or 2"
            )]
        );
        out_of_range.destinations[0].paths = 0;
        assert_eq!(validate_stream(&out_of_range).len(), 1);

        let mut remote = srt_node("unused");
        remote.node = None;
        remote.remote = Some(crate::RemoteAddr {
            host: "198.51.100.5".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });
        let mut to_remote = stream();
        to_remote.destinations = vec![StreamDestination {
            paths: 2,
            ..destination("uplink", StreamTransport::Srt(remote))
        }];
        assert_eq!(
            validate_stream(&to_remote),
            [issue(
                "destinations[0].paths",
                "not_allowed",
                "a remote destination has no receiver to merge two paths"
            )]
        );

        let mut pinned = srt_node("destination");
        pinned.via = vec!["relay".to_string()];
        let mut via = stream();
        via.destinations = vec![StreamDestination {
            paths: 2,
            ..destination("destination", StreamTransport::Srt(pinned))
        }];
        assert_eq!(
            validate_stream(&via),
            [issue(
                "destinations[0].paths",
                "not_allowed",
                "a destination with two paths must not pin via"
            )]
        );
    }
    fn signalling(node: &str) -> crate::SignallingEndpoint {
        crate::SignallingEndpoint {
            node: node.to_string(),
            network: None,
            format: None,
            accepts: None,
        }
    }

    #[test]
    fn whip_is_a_source_and_whep_a_destination() {
        let mut outside = stream();
        outside.source = StreamTransport::Whip(signalling("gateway"));
        outside.destinations = vec![destination(
            "monitor",
            StreamTransport::Whep(signalling("player-edge")),
        )];
        assert_eq!(validate_stream(&outside), []);

        let mut reversed = stream();
        reversed.source = StreamTransport::Whep(signalling("gateway"));
        reversed.destinations = vec![destination(
            "monitor",
            StreamTransport::Whip(signalling("player-edge")),
        )];
        assert_eq!(
            validate_stream(&reversed),
            [
                issue(
                    "source.whep",
                    "destination_only",
                    "whep belongs on a destination, not the source",
                ),
                issue(
                    "destinations[0].whip",
                    "source_only",
                    "whip belongs on the source, not a destination",
                ),
            ]
        );
    }

    #[test]
    fn whip_and_whep_fields_are_validated() {
        let mut invalid = stream();
        invalid.source = StreamTransport::Whip(crate::SignallingEndpoint {
            network: Some("Internet".to_string()),
            accepts: Some(FormatConstraint::default()),
            ..signalling("gateway")
        });
        invalid.destinations = vec![destination(
            "monitor",
            StreamTransport::Whep(crate::SignallingEndpoint {
                format: Some(MediaFormat {
                    container: Container::MpegTs,
                    video: None,
                    audio: None,
                }),
                ..signalling("player_edge")
            }),
        )];
        assert_eq!(
            validate_stream(&invalid)
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                ("source.whip.network", "invalid_characters"),
                ("source.whip.accepts", "destination_only"),
                ("destinations[0].whep.node", "invalid_characters"),
                ("destinations[0].whep.format", "source_only"),
            ]
        );
    }

    #[test]
    fn a_whip_source_parses_from_its_manifest_tag() {
        let parsed: StreamDefinition = serde_json::from_value(serde_json::json!({
            "name": "encoder",
            "source": { "whip": { "node": "strom-node-1", "network": "internet" } },
            "destinations": [{ "id": "monitor", "whep": { "node": "strom-node-2" } }]
        }))
        .unwrap();
        assert_eq!(parsed.source.kind(), "whip");
        assert_eq!(parsed.source.node(), Some("strom-node-1"));
        assert_eq!(parsed.source.network(), Some("internet"));
        assert_eq!(parsed.destinations[0].endpoint.kind(), "whep");
        assert!(
            serde_json::from_value::<StreamDefinition>(serde_json::json!({
                "name": "encoder",
                "source": { "whip": { "node": "strom-node-1", "url": "http://x" } },
                "destinations": [{ "id": "monitor", "whep": { "node": "strom-node-2" } }]
            }))
            .is_err(),
            "a manifest never carries an address"
        );
    }

    #[test]
    fn a_hop_profile_constraint_follows_the_accepts_rules() {
        let class = crate::HopEndpointClass::Transport(crate::TransportClass {
            transport: crate::Transport::Srt,
            roles: crate::RoleSet::both(),
        });
        let node = crate::NodeDescriptor {
            inputs: Vec::new(),
            outputs: Vec::new(),
            id: "strom-node-1".to_string(),
            endpoint: "http://strom-node-1:8091".to_string(),
            status: crate::NodeStatus::Ready,
            capabilities: crate::NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![crate::HopProfile {
                    timed_delivery: None,
                    id: "srt-forward".to_string(),
                    ingress: class.clone(),
                    egress: class,
                    max_egresses: None,
                    media_behavior: crate::MediaBehavior::Unknown,
                    merge: false,
                    accepts: Some(FormatConstraint {
                        video: Some(crate::VideoConstraint {
                            codec: Some(Vec::new()),
                            ..crate::VideoConstraint::default()
                        }),
                        ..FormatConstraint::default()
                    }),
                }],
            },
            topology: crate::DeclaredTopology::default(),
        };
        assert_eq!(
            super::validate_node(&node),
            [issue(
                "node.capabilities.hop_profiles[0].accepts.video.codec",
                "empty",
                "accepts must not contain an empty list of values",
            )]
        );
    }

    fn node_with(inputs: Vec<NodeInput>, outputs: Vec<NodeOutput>) -> DeclaredNode {
        NodeDescriptor {
            inputs,
            outputs,
            ..serde_json::from_value(serde_json::json!({
                "id": "alfreds-mac",
                "endpoint": "spaghetti://alfreds-mac",
                "status": "ready",
                "capabilities": { "adapters": [], "hop_profiles": [
                    {
                        "id": "app-audio-to-srt",
                        "ingress": "input",
                        "egress": { "transport": "srt", "roles": ["connect"] },
                    },
                    {
                        "id": "srt-to-speaker",
                        "ingress": { "transport": "srt", "roles": ["listen"] },
                        "egress": "output",
                    },
                ]},
                "topology": { "attachments": [] },
            }))
            .expect("node fixture")
        }
    }

    fn input(id: &str, label: &str, tracks: Vec<Track>) -> NodeInput {
        NodeInput {
            id: id.to_string(),
            label: label.to_string(),
            tracks,
        }
    }

    fn output(id: &str, label: &str) -> NodeOutput {
        NodeOutput {
            id: id.to_string(),
            label: label.to_string(),
        }
    }

    fn codes(node: &DeclaredNode) -> Vec<(String, String)> {
        validate_node(node)
            .into_iter()
            .map(|issue| (issue.field, issue.code))
            .collect()
    }

    #[test]
    fn declared_inputs_and_outputs_are_valid_when_named_labelled_and_unique() {
        let node = node_with(
            vec![
                input("music", "Music", vec![Track::Audio]),
                input("camera", "Camera", vec![Track::Audio, Track::Video]),
            ],
            vec![
                output("speakers", "MacBook Pro Speakers"),
                output("music", "Music"),
            ],
        );
        assert!(codes(&node).is_empty());
    }

    #[test]
    fn declared_inputs_and_outputs_reject_bad_ids_labels_tracks_and_duplicates() {
        let node = node_with(
            vec![
                input("Music", " ", vec![]),
                input("camera", "Camera", vec![Track::Video, Track::Video]),
                input("camera", "Camera", vec![Track::Video]),
            ],
            vec![output("speakers", ""), output("speakers", "Speakers")],
        );
        let field = |path: &str| path.to_string();
        assert_eq!(
            codes(&node),
            [
                (field("node.inputs[0].id"), "invalid_characters".to_string()),
                (field("node.inputs[0].label"), "blank".to_string()),
                (field("node.inputs[0].tracks"), "empty".to_string()),
                (field("node.inputs[1].tracks"), "duplicate".to_string()),
                (field("node.inputs[2].id"), "duplicate".to_string()),
                (field("node.outputs[0].label"), "blank".to_string()),
                (field("node.outputs[1].id"), "duplicate".to_string()),
            ]
        );
    }

    #[test]
    fn a_profile_cannot_take_an_output_or_feed_an_input() {
        let mut node = node_with(Vec::new(), Vec::new());
        let profile = &mut node.capabilities.hop_profiles[0];
        std::mem::swap(&mut profile.ingress, &mut profile.egress);
        let mut reversed = node.capabilities.hop_profiles[1].clone();
        std::mem::swap(&mut reversed.ingress, &mut reversed.egress);
        reversed.id = "reversed".to_string();
        node.capabilities.hop_profiles.push(reversed);
        assert_eq!(
            codes(&node),
            [
                (
                    "node.capabilities.hop_profiles[0].egress".to_string(),
                    "wrong_end".to_string()
                ),
                (
                    "node.capabilities.hop_profiles[2].ingress".to_string(),
                    "wrong_end".to_string()
                ),
            ]
        );
    }

    #[test]
    fn a_site_shares_attachment_ids_and_no_declared_network_takes_its_prefix() {
        let mut node = node_with(Vec::new(), Vec::new());
        node.topology = serde_json::from_value(serde_json::json!({
            "site": {
                "id": "lan",
                "listeners": { "srt": { "port_range": { "start": 23099, "end": 23000 } } }
            },
            "attachments": [{ "id": "lan", "network": "site-3f9a2c1d7e4b", "dial": true }]
        }))
        .expect("topology fixture");
        let issues = validate_node(&node);
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                (
                    "node.topology.site.listeners.srt.port_range",
                    "invalid_range"
                ),
                ("node.topology.attachments[0].network", "reserved"),
                ("node.topology.attachments[0].id", "duplicate"),
            ]
        );
        assert!(
            issues[1].message.contains("declare topology.site"),
            "{}",
            issues[1].message
        );
    }

    fn observation(address: &str, mapped: &[&str]) -> NodeObservation {
        NodeObservation {
            uplink: Some(Uplink {
                interface: "en0".to_string(),
                address: address.parse().unwrap(),
                prefix_len: 24,
                gateway: None,
                reflexive: mapped
                    .iter()
                    .map(|mapped| ReflexiveAddress {
                        server: "192.0.2.1:3478".parse().unwrap(),
                        mapped: mapped.parse().unwrap(),
                    })
                    .collect(),
            }),
        }
    }

    fn observation_issues(observation: &NodeObservation) -> Vec<(String, String)> {
        validate_observation(observation)
            .into_iter()
            .map(|issue| (issue.field, issue.code))
            .collect()
    }

    #[test]
    fn an_uplink_must_be_an_address_a_peer_could_reach() {
        assert!(
            observation_issues(&observation("192.168.1.20", &["203.0.113.7:61000"])).is_empty()
        );
        for address in ["0.0.0.0", "127.0.0.1", "224.0.0.251", "255.255.255.255"] {
            assert_eq!(
                observation_issues(&observation(address, &[])),
                [(
                    "observation.uplink.address".to_string(),
                    "not_routable".to_string()
                )],
                "{address}"
            );
        }
    }

    #[test]
    fn reflexive_addresses_are_mapped_and_at_most_eight() {
        assert_eq!(
            observation_issues(&observation(
                "192.168.1.20",
                &["203.0.113.7:61000", "0.0.0.0:61000"]
            )),
            [(
                "observation.uplink.reflexive[1].mapped".to_string(),
                "not_routable".to_string()
            )]
        );
        assert!(
            observation_issues(&observation("192.168.1.20", &["203.0.113.7:61000"; 8])).is_empty()
        );
        assert_eq!(
            observation_issues(&observation("192.168.1.20", &["203.0.113.7:61000"; 9])),
            [(
                "observation.uplink.reflexive".to_string(),
                "too_many".to_string()
            )]
        );
    }
}

#[cfg(test)]
mod passphrase_tests {
    use super::*;
    use crate::{StreamDestination, StreamTransport};

    fn keyed(passphrase: &str) -> StreamDefinition {
        StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("source".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                passphrase: Some(Passphrase::new(passphrase)),
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamDestination {
                delivery: None,
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("studio".to_string()),
                    remote: None,
                    via: Vec::new(),
                    network: None,
                    latency: None,
                    passphrase: None,
                    format: None,
                    accepts: None,
                }),
            }],
        }
    }

    #[test]
    fn passphrase_length_follows_libsrt() {
        for (length, valid) in [(9, false), (10, true), (80, true), (81, false)] {
            let issues = validate_stream(&keyed(&"k".repeat(length)));
            assert_eq!(issues.is_empty(), valid, "{length}: {issues:?}");
        }
    }

    #[test]
    fn a_rejected_passphrase_is_named_but_never_quoted() {
        let secret = "short-key";
        let issues = validate_stream(&keyed(secret));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].field, "source.srt.passphrase");
        assert_eq!(issues[0].code, "invalid_length");
        assert!(!issues[0].message.contains(secret));

        let issues = validate_stream(&keyed("line\nbreak-in-key"));
        assert_eq!(issues[0].code, "invalid_characters");
    }

    #[test]
    fn a_passphrase_is_printable_ascii_so_its_characters_are_its_bytes() {
        for valid in [" spaced out ~", "0123456789", &"~".repeat(80)] {
            assert!(validate_stream(&keyed(valid)).is_empty(), "{valid}");
        }
        let accented = "é".repeat(40);
        assert_eq!(accented.chars().count(), 40);
        let codes: Vec<_> = validate_stream(&keyed(&accented))
            .into_iter()
            .map(|issue| issue.code)
            .collect();
        assert_eq!(codes, ["invalid_characters"]);
        let tab = validate_stream(&keyed("tab\tin-the-key"));
        assert_eq!(tab[0].code, "invalid_characters");
        let delete = validate_stream(&keyed("delete\u{7f}-in-key"));
        assert_eq!(delete[0].code, "invalid_characters");
    }
}
