use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::RequestBuilder;
use reqwest::header::{ETAG, HeaderValue, IF_MATCH, IF_NONE_MATCH};
use serde::de::DeserializeOwned;
use weave_core::auth::{self, MIN_NODE_KEY_LEN, NodeKey, NodeKeyError, Token};
use weave_core::{
    StreamConditionStatus, StreamDefinition, StreamSetApply, read, validate_resource_id,
};

#[derive(Parser)]
#[command(name = "weave", version, about = "open-weave control plane CLI")]
struct Cli {
    #[arg(help = "Northbound API base URL")]
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_URL",
        hide_env_values = true,
        default_value = "http://127.0.0.1:9080",
        global = true
    )]
    url: String,
    #[arg(
        help = "Bearer token presented to the northbound API. Required unless the server runs with WEAVE_AUTH_DISABLED=1"
    )]
    #[arg(
        long,
        env = auth::NORTHBOUND_TOKEN_VAR,
        hide_env_values = true,
        global = true
    )]
    token: Option<String>,
    #[arg(help = "Output format")]
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    output: OutputFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Human,
    Yaml,
    Json,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Apply a stream definition (YAML) as desired state")]
    Apply {
        #[arg(help = "Path to the stream YAML file")]
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    #[command(about = "Atomically apply a stream ownership set (YAML) as desired state")]
    ApplySet {
        #[arg(help = "Owner of the stream set")]
        owner: String,
        #[arg(help = "Path to the stream-set YAML file")]
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    #[command(about = "Preview validation and placement without changing desired state")]
    Plan {
        #[arg(help = "Path to the stream YAML file")]
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    #[command(about = "Get resources from the northbound API")]
    Get {
        #[command(subcommand)]
        resource: GetResource,
    },
    #[command(about = "Delete resources through the northbound API")]
    Delete {
        #[command(subcommand)]
        resource: DeleteResource,
    },
    #[command(
        about = "Print a node's southbound token, derived from the southbound key in `WEAVE_SOUTHBOUND_KEY`. Calls no service"
    )]
    NodeToken {
        #[arg(help = "Node id the token authenticates as")]
        id: String,
        #[arg(
            help = "Token epoch. Raising the node's minimum in `WEAVE_SOUTHBOUND_MIN_EPOCHS` past an epoch revokes its tokens"
        )]
        #[arg(long, default_value_t = 0)]
        epoch: u64,
    },
}

#[derive(Subcommand)]
enum GetResource {
    #[command(about = "List desired streams")]
    Streams,
    #[command(about = "Get one desired stream")]
    Stream {
        #[arg(help = "Name of the stream to get")]
        name: String,
    },
    #[command(about = "Get the current control-plane status")]
    Status,
    #[command(about = "List registered nodes")]
    Nodes,
    #[command(about = "Get the resolved endpoints for one stream")]
    Endpoints {
        #[arg(help = "Name of the stream whose endpoints to get")]
        name: String,
    },
    #[command(about = "List stream ownership sets")]
    StreamSets,
    #[command(about = "Get one stream ownership set")]
    StreamSet {
        #[arg(help = "Owner of the stream set to get")]
        owner: String,
    },
}

#[derive(Subcommand)]
enum DeleteResource {
    #[command(about = "Delete a desired stream")]
    Stream {
        #[arg(help = "Name of the stream to delete")]
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli {
        url,
        token,
        output,
        command,
    } = Cli::parse();
    let token = token.as_deref().and_then(Token::new);

    let rendered = match command {
        Command::Apply { file } => apply(&url, token.as_ref(), &file, output).await,
        Command::ApplySet { owner, file } => {
            apply_set(&url, token.as_ref(), &owner, &file, output).await
        }
        Command::Plan { file } => plan(&url, token.as_ref(), &file, output).await,
        Command::Get { resource } => match resource {
            GetResource::Streams => get_streams(&url, token.as_ref(), output).await,
            GetResource::Stream { name } => get_stream(&url, token.as_ref(), &name, output).await,
            GetResource::Status => get_status(&url, token.as_ref(), output).await,
            GetResource::Nodes => get_nodes(&url, token.as_ref(), output).await,
            GetResource::Endpoints { name } => {
                get_endpoints(&url, token.as_ref(), &name, output).await
            }
            GetResource::StreamSets => get_stream_sets(&url, token.as_ref(), output).await,
            GetResource::StreamSet { owner } => {
                get_stream_set(&url, token.as_ref(), &owner, output).await
            }
        },
        Command::Delete { resource } => match resource {
            DeleteResource::Stream { name } => {
                delete_stream(&url, token.as_ref(), &name, output).await
            }
        },
        Command::NodeToken { id, epoch } => {
            let key = std::env::var(auth::SOUTHBOUND_KEY_VAR).ok();
            node_token(key.as_deref(), &id, epoch)
        }
    }?;
    println!("{}", rendered.trim_end());
    Ok(())
}

fn node_token(key: Option<&str>, id: &str, epoch: u64) -> Result<String> {
    if let Err(error) = validate_resource_id(id) {
        bail!("invalid node id: {error}");
    }
    let key = match NodeKey::new(key.unwrap_or_default()) {
        Ok(key) => key,
        Err(NodeKeyError::Blank) => bail!("no southbound key: set {}", auth::SOUTHBOUND_KEY_VAR),
        Err(NodeKeyError::TooShort) => bail!(
            "the southbound key must be at least {MIN_NODE_KEY_LEN} characters, as southbound and the controller require"
        ),
    };
    Ok(key.token_for(id, epoch))
}

fn authorized(request: RequestBuilder, token: Option<&Token>) -> RequestBuilder {
    match token {
        Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
        None => request,
    }
}

fn unauthorized_hint(token: Option<&Token>) -> &'static str {
    if token.is_none() {
        "\nno token was sent: pass --token or set WEAVE_NORTHBOUND_TOKEN"
    } else {
        "\nthe token sent was rejected: check it matches the server's WEAVE_NORTHBOUND_TOKEN"
    }
}

fn error_detail(body: &str) -> String {
    let Ok(error) = serde_json::from_str::<read::ApiError>(body) else {
        return body.to_string();
    };
    let mut detail = format!("[{}] {}", error.code, error.message);
    for issue in error.details {
        detail.push_str(&format!(
            "\n{} [{}]: {}",
            issue.field, issue.code, issue.message
        ));
    }
    detail
}

fn refused(
    context: &str,
    status: reqwest::StatusCode,
    body: &str,
    token: Option<&Token>,
) -> anyhow::Error {
    let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
        unauthorized_hint(token)
    } else {
        ""
    };
    anyhow!("{context}: {status}: {}{hint}", error_detail(body))
}

async fn success_body(
    response: reqwest::Response,
    context: &str,
    token: Option<&Token>,
) -> Result<String> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(refused(context, status, &body, token));
    }
    Ok(body)
}

fn decode<T: DeserializeOwned>(body: &str, label: &str) -> Result<T> {
    serde_json::from_str(body).with_context(|| format!("decoding {label}"))
}

fn format_output(output: OutputFormat, body: &str, human: String) -> Result<String> {
    let value = || decode::<serde_json::Value>(body, "response");
    Ok(match output {
        OutputFormat::Human => human,
        OutputFormat::Yaml => serde_norway::to_string(&value()?).context("encoding YAML output")?,
        OutputFormat::Json => {
            serde_json::to_string_pretty(&value()?).context("encoding JSON output")?
        }
    })
}

fn render<T: DeserializeOwned>(
    output: OutputFormat,
    body: &str,
    label: &str,
    human: impl FnOnce(&T) -> String,
) -> Result<String> {
    let human = match output {
        OutputFormat::Human => human(&decode(body, label)?),
        OutputFormat::Json | OutputFormat::Yaml => String::new(),
    };
    format_output(output, body, human)
}

fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let render_row = |row: Vec<String>| {
        row.into_iter()
            .enumerate()
            .map(|(index, value)| {
                if index + 1 == widths.len() {
                    value
                } else {
                    format!("{value:<width$}", width = widths[index])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };

    let mut lines = vec![render_row(
        headers.iter().map(|header| (*header).to_string()).collect(),
    )];
    lines.extend(rows.into_iter().map(render_row));
    lines.join("\n")
}

fn render_streams(streams: &[read::StreamResource], statuses: &[read::StreamStatus]) -> String {
    let mut streams = streams.iter().collect::<Vec<_>>();
    streams.sort_by(|left, right| left.spec.name.cmp(&right.spec.name));
    let rows = streams
        .into_iter()
        .map(|stream| {
            vec![
                stream.spec.name.clone(),
                stream.generation.to_string(),
                statuses
                    .iter()
                    .find(|status| status.name == stream.spec.name)
                    .map_or_else(|| "-".to_string(), |status| status.status.to_string()),
                stream.owner.clone().unwrap_or_else(|| "-".to_string()),
                stream.spec.enabled.to_string(),
            ]
        })
        .collect();
    table(&["NAME", "GENERATION", "STATUS", "OWNER", "ENABLED"], rows)
}

fn render_stream(stream: &read::StreamResource, statuses: &[read::StreamStatus]) -> String {
    let mut sections = vec![render_streams(std::slice::from_ref(stream), statuses)];
    if let Some(status) = statuses
        .iter()
        .find(|status| status.name == stream.spec.name)
    {
        sections.push(table(
            &["CONDITION", "STATUS", "REASON", "DETAIL"],
            status
                .conditions
                .iter()
                .map(|condition| {
                    vec![
                        condition.condition_type.to_string(),
                        condition.status.to_string(),
                        condition.reason.to_string(),
                        condition.detail.clone(),
                    ]
                })
                .collect(),
        ));
        sections.push(table(
            &["DESTINATION", "STATUS", "ENDPOINT", "REASON"],
            status
                .destinations
                .iter()
                .map(|destination| {
                    vec![
                        destination.id.clone(),
                        destination.status.to_string(),
                        destination
                            .endpoint
                            .as_ref()
                            .map_or_else(|| "-".to_string(), |endpoint| endpoint.url.clone()),
                        failing_reasons(&destination.conditions),
                    ]
                })
                .collect(),
        ));
    }
    sections.join("\n\n")
}

fn failing_reasons(conditions: &[read::StreamCondition]) -> String {
    let reasons = conditions
        .iter()
        .filter(|condition| condition.status == StreamConditionStatus::False)
        .map(|condition| condition.reason.to_string())
        .collect::<Vec<_>>();
    if reasons.is_empty() {
        "-".to_string()
    } else {
        reasons.join(",")
    }
}

fn render_nodes(nodes: &[read::NodeDescriptor]) -> String {
    let mut nodes = nodes.iter().collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    let rows = nodes
        .into_iter()
        .map(|node| {
            let profiles = node
                .capabilities
                .hop_profiles
                .iter()
                .map(|profile| profile.id.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let networks = node
                .topology
                .attachments
                .iter()
                .map(|attachment| attachment.network.as_str())
                .collect::<Vec<_>>()
                .join(",");
            vec![
                node.id.clone(),
                node.status.to_string(),
                node.endpoint.clone(),
                profiles,
                networks,
            ]
        })
        .collect();
    table(&["ID", "STATUS", "ENDPOINT", "PROFILES", "NETWORKS"], rows)
}

fn render_status(status: &read::StatusResponse) -> String {
    match status {
        read::StatusResponse::Starting(_) => "STATUS\nstarting".to_string(),
        read::StatusResponse::Running(status) => {
            let mut streams = status.streams.iter().collect::<Vec<_>>();
            streams.sort_by(|left, right| left.name.cmp(&right.name));
            let rows = streams
                .into_iter()
                .map(|stream| {
                    vec![
                        stream.name.clone(),
                        stream.generation.to_string(),
                        stream
                            .observed_generation
                            .map_or_else(|| "-".to_string(), |value| value.to_string()),
                        stream.status.to_string(),
                        if stream.nodes.is_empty() {
                            "-".to_string()
                        } else {
                            stream.nodes.join(",")
                        },
                    ]
                })
                .collect();
            format!(
                "STATUS: {}\nSUMMARY: {}\n\n{}",
                status.status,
                status.summary,
                table(&["NAME", "GENERATION", "OBSERVED", "STATUS", "NODES"], rows,)
            )
        }
    }
}

fn render_endpoints(endpoints: &read::StreamEndpoints) -> String {
    let mut rows = Vec::with_capacity(endpoints.destinations.len() + 1);
    rows.push(match &endpoints.ingress {
        Some(endpoint) => vec![
            "ingress".to_string(),
            "-".to_string(),
            endpoint.node.clone(),
            endpoint.url.clone(),
        ],
        None => vec![
            "ingress".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ],
    });
    rows.extend(
        endpoints
            .destinations
            .iter()
            .map(|destination| match &destination.endpoint {
                Some(endpoint) => vec![
                    "destination".to_string(),
                    destination.id.clone(),
                    endpoint.node.clone(),
                    endpoint.url.clone(),
                ],
                None => vec![
                    "destination".to_string(),
                    destination.id.clone(),
                    "-".to_string(),
                    "-".to_string(),
                ],
            }),
    );
    table(&["ROLE", "ID", "NODE", "URL"], rows)
}

fn render_stream_sets(stream_sets: &[read::StreamSetResource]) -> String {
    let mut sets = stream_sets.iter().collect::<Vec<_>>();
    sets.sort_by(|left, right| left.owner.cmp(&right.owner));
    table(
        &["OWNER", "STREAMS"],
        sets.into_iter()
            .map(|set| vec![set.owner.clone(), set.streams.len().to_string()])
            .collect(),
    )
}

fn render_stream_set(
    stream_set: &read::StreamSetResource,
    statuses: &[read::StreamStatus],
) -> String {
    format!(
        "OWNER: {}\n\n{}",
        stream_set.owner,
        render_streams(&stream_set.streams, statuses)
    )
}

fn render_stream_set_accepted(accepted: &read::StreamSetAccepted) -> String {
    let mut rows = accepted
        .streams
        .iter()
        .map(|stream| {
            vec![
                stream.name.clone(),
                stream.action.to_string(),
                stream.generation.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    rows.extend(
        accepted
            .pruned
            .iter()
            .map(|name| vec![name.clone(), "pruned".to_string(), "-".to_string()]),
    );
    format!(
        "OWNER: {}\nCHANGED: {}\n\n{}",
        accepted.owner,
        accepted.changed,
        table(&["NAME", "ACTION", "GENERATION"], rows)
    )
}

fn render_plan(plan: &read::StreamPlan) -> String {
    table(
        &["NAME", "STATUS", "NODES", "REASON"],
        vec![vec![
            plan.name.clone(),
            plan.status.to_string(),
            if plan.nodes.is_empty() {
                "-".to_string()
            } else {
                plan.nodes.join(",")
            },
            plan.reason.clone().unwrap_or_else(|| "-".to_string()),
        ]],
    )
}

async fn get_body(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
    path: &str,
    label: &str,
) -> Result<String> {
    let response = authorized(client.get(api_url(url, path)), token)
        .send()
        .await
        .with_context(|| format!("fetching {label}"))?;
    success_body(
        response,
        &format!("northbound {label} request failed"),
        token,
    )
    .await
}

async fn fetch_status(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
) -> Result<read::StatusResponse> {
    decode(
        &get_body(client, url, token, "/status", "status").await?,
        "status",
    )
}

fn parse_stream(yaml: &str) -> Result<StreamDefinition> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream YAML")
}

fn parse_stream_set(yaml: &str) -> Result<StreamSetApply> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream-set YAML")
}

async fn apply(
    url: &str,
    token: Option<&Token>,
    file: &Path,
    output: OutputFormat,
) -> Result<String> {
    let stream = read_stream(file)?;
    apply_stream(url, token, &stream, output).await
}

async fn apply_stream(
    url: &str,
    token: Option<&Token>,
    stream: &StreamDefinition,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(&stream.name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let path = format!("/streams/{}", stream.name);
    let existing = lookup(&client, url, token, &path, "stream").await?;

    let request = authorized(client.post(api_url(url, "/streams")), token).json(&stream);
    let request = match existing {
        Some(existing) => request.header(IF_MATCH, existing.etag),
        None => request.header(IF_NONE_MATCH, "*"),
    };
    let response = request
        .send()
        .await
        .context("posting stream to northbound")?;
    let body = success_body(response, "northbound rejected stream", token).await?;
    render(
        output,
        &body,
        "accepted stream",
        |accepted: &read::StreamAccepted| {
            table(
                &["NAME", "GENERATION", "CHANGED"],
                vec![vec![
                    accepted.name.clone(),
                    accepted.generation.to_string(),
                    accepted.changed.to_string(),
                ]],
            )
        },
    )
}

fn read_stream(file: &Path) -> Result<StreamDefinition> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    parse_stream(&text).with_context(|| format!("parsing stream from {}", file.display()))
}

fn read_stream_set(file: &Path) -> Result<StreamSetApply> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    parse_stream_set(&text).with_context(|| format!("parsing stream set from {}", file.display()))
}

async fn apply_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    file: &Path,
    output: OutputFormat,
) -> Result<String> {
    let apply = read_stream_set(file)?;
    apply_stream_set(url, token, owner, &apply, output).await
}

async fn apply_stream_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    apply: &StreamSetApply,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(owner) {
        bail!("invalid stream-set owner: {error}");
    }
    let client = reqwest::Client::new();
    let path = format!("/stream-sets/{owner}");
    let existing = lookup(&client, url, token, &path, "stream set").await?;
    let request = authorized(client.put(api_url(url, &path)), token).json(apply);
    let request = match existing {
        Some(existing) => request.header(IF_MATCH, existing.etag),
        None => request.header(IF_NONE_MATCH, "*"),
    };
    let response = request
        .send()
        .await
        .context("putting stream set to northbound")?;
    let body = success_body(response, "northbound rejected stream set", token).await?;
    render(
        output,
        &body,
        "accepted stream set",
        render_stream_set_accepted,
    )
}

async fn plan(
    url: &str,
    token: Option<&Token>,
    file: &Path,
    output: OutputFormat,
) -> Result<String> {
    let stream = read_stream(file)?;
    plan_stream(url, token, &stream, output).await
}

async fn plan_stream(
    url: &str,
    token: Option<&Token>,
    stream: &StreamDefinition,
    output: OutputFormat,
) -> Result<String> {
    let response = authorized(
        reqwest::Client::new().post(api_url(url, "/stream-plans")),
        token,
    )
    .json(&stream)
    .send()
    .await
    .context("planning stream through northbound")?;
    let body = success_body(response, "northbound rejected stream plan", token).await?;
    render(output, &body, "stream plan", render_plan)
}

async fn get_streams(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<String> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/streams", "streams").await?;
    let human = match output {
        OutputFormat::Human => {
            let streams: Vec<read::StreamResource> = decode(&body, "streams")?;
            render_streams(&streams, fetch_status(&client, url, token).await?.streams())
        }
        OutputFormat::Json | OutputFormat::Yaml => String::new(),
    };
    format_output(output, &body, human)
}

async fn get_stream(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let Some(found) = lookup(&client, url, token, &format!("/streams/{name}"), "stream").await?
    else {
        bail!("no stream named {name}");
    };
    let human = match output {
        OutputFormat::Human => {
            let stream: read::StreamResource = decode(&found.body, "stream resource")?;
            render_stream(&stream, fetch_status(&client, url, token).await?.streams())
        }
        OutputFormat::Json | OutputFormat::Yaml => String::new(),
    };
    format_output(output, &found.body, human)
}

async fn get_status(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<String> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/status", "status").await?;
    render(output, &body, "status", render_status)
}

async fn get_nodes(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<String> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/nodes", "nodes").await?;
    render(
        output,
        &body,
        "nodes",
        |nodes: &Vec<read::NodeDescriptor>| render_nodes(nodes),
    )
}

async fn get_endpoints(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let body = get_body(
        &client,
        url,
        token,
        &format!("/streams/{name}/endpoints"),
        "stream endpoints",
    )
    .await?;
    render(output, &body, "stream endpoints", render_endpoints)
}

async fn get_stream_sets(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<String> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/stream-sets", "stream sets").await?;
    render(
        output,
        &body,
        "stream sets",
        |sets: &Vec<read::StreamSetResource>| render_stream_sets(sets),
    )
}

async fn get_stream_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(owner) {
        bail!("invalid stream-set owner: {error}");
    }
    let client = reqwest::Client::new();
    let body = get_body(
        &client,
        url,
        token,
        &format!("/stream-sets/{owner}"),
        "stream set",
    )
    .await?;
    let human = match output {
        OutputFormat::Human => {
            let stream_set: read::StreamSetResource = decode(&body, "stream set")?;
            render_stream_set(
                &stream_set,
                fetch_status(&client, url, token).await?.streams(),
            )
        }
        OutputFormat::Json | OutputFormat::Yaml => String::new(),
    };
    format_output(output, &body, human)
}

async fn delete_stream(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<String> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let path = format!("/streams/{name}");
    let Some(existing) = lookup(&client, url, token, &path, "stream").await? else {
        bail!("no stream named {name}");
    };
    let response = authorized(client.delete(api_url(url, &path)), token)
        .header(IF_MATCH, existing.etag)
        .send()
        .await
        .context("deleting stream on northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND {
        let detail = if body.trim().is_empty() {
            String::new()
        } else {
            format!(": {body}")
        };
        bail!("no stream named {name}{detail}");
    }
    if !status.is_success() {
        return Err(refused("northbound rejected delete", status, &body, token));
    }

    format_output(
        output,
        &serde_json::json!({ "deleted": name }).to_string(),
        format!("deleted stream {name}"),
    )
}

struct Found {
    etag: HeaderValue,
    body: String,
}

async fn lookup(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
    path: &str,
    label: &str,
) -> Result<Option<Found>> {
    let response = authorized(client.get(api_url(url, path)), token)
        .send()
        .await
        .with_context(|| format!("fetching {label}"))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let etag = response.headers().get(ETAG).cloned();
    let body = success_body(
        response,
        &format!("northbound {label} request failed"),
        token,
    )
    .await?;
    let etag = etag.with_context(|| format!("northbound {label} response is missing its ETag"))?;
    Ok(Some(Found { etag, body }))
}

fn api_url(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::{Body, Bytes};
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};
    use weave_core::{
        ApiError, ApiErrorCode, SrtEndpoint, StreamAccepted, StreamDestination, StreamPlan,
        StreamResource, StreamSetAccepted, StreamSetAction, StreamSetMemberResult,
        StreamSetResource, StreamTransport, validate_stream,
    };

    #[test]
    fn node_token_is_the_node_id_its_epoch_and_their_mac() {
        let key = Some("bench-southbound-key-for-local-use-only");
        assert_eq!(
            node_token(key, "strom-node-1", 0).unwrap(),
            "strom-node-1.0.dc18eb10e0f080536c55f9ff6569abb31d2da3ce4f782b294dd0d32fcbd8dbaf"
        );
        let second = node_token(key, "strom-node-1", 1).unwrap();
        assert!(second.starts_with("strom-node-1.1."), "{second}");
        assert!(node_token(Some("k"), "Not_An_Id", 0).is_err());
        let error = node_token(None, "strom-node-1", 0).unwrap_err().to_string();
        assert!(error.contains(auth::SOUTHBOUND_KEY_VAR), "{error}");
        assert!(node_token(Some("  "), "strom-node-1", 0).is_err());
        let error = node_token(Some("bench-southbound-key"), "strom-node-1", 0)
            .unwrap_err()
            .to_string();
        assert!(error.contains("32 characters"), "{error}");
    }

    #[test]
    fn node_token_takes_no_key_on_its_command_line() {
        let key = "bench-southbound-key-for-local-use-only";
        assert!(
            Cli::try_parse_from(["weave", "node-token", "strom-node-1", "--key", key]).is_err()
        );
        assert!(
            Cli::try_parse_from(["weave", "node-token", "strom-node-1", "--epoch", "2"]).is_ok()
        );
    }

    fn env_values_shown(command: &clap::Command) -> Vec<String> {
        command
            .get_arguments()
            .filter(|arg| arg.get_env().is_some() && !arg.is_hide_env_values_set())
            .map(|arg| arg.get_id().to_string())
            .chain(command.get_subcommands().flat_map(env_values_shown))
            .collect()
    }

    #[test]
    fn help_hides_every_env_value() {
        let shown = env_values_shown(&<Cli as clap::CommandFactory>::command());
        assert!(
            shown.is_empty(),
            "--help prints the environment value of {shown:?}"
        );
    }

    #[test]
    fn parses_fanout_yaml_with_defaults_and_srt_tag() {
        let yaml = r#"
name: cam1-to-studio
source:
  srt:
    node: strom-node-1
    latency: 200
destinations:
  - id: studio
    srt:
      node: strom-node-2
  - id: preview
    srt:
      node: strom-node-2
      network: wan
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");

        assert_eq!(stream.name, "cam1-to-studio");
        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(200),
                passphrase: None,
            })
        );
        assert_eq!(stream.destinations.len(), 2);
        assert_eq!(
            stream.destinations[1].endpoint,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: Some("wan".to_string()),
                latency: None,
                passphrase: None,
            })
        );
    }

    fn example(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples")
            .join(name)
    }

    #[test]
    fn example_manifests_parse_and_validate() {
        for name in ["stream.yaml", "contribution.yaml"] {
            let stream = read_stream(&example(name)).unwrap_or_else(|error| panic!("{error:#}"));
            assert_eq!(validate_stream(&stream), [], "examples/{name}");
        }

        let set = read_stream_set(&example("stream-set.yaml"))
            .unwrap_or_else(|error| panic!("{error:#}"));
        assert!(!set.streams.is_empty(), "examples/stream-set.yaml");
        for stream in &set.streams {
            assert_eq!(
                validate_stream(stream),
                [],
                "examples/stream-set.yaml: {}",
                stream.name
            );
        }
    }

    #[test]
    fn structured_errors_show_field_details() {
        let body = serde_json::json!({
            "code": "invalid_request",
            "message": "stream validation failed",
            "details": [{
                "field": "name",
                "code": "invalid_characters",
                "message": "stream name is invalid"
            }]
        })
        .to_string();

        assert_eq!(
            error_detail(&body),
            "[invalid_request] stream validation failed\nname [invalid_characters]: stream name is invalid"
        );
    }

    #[test]
    fn structured_precondition_errors_keep_their_stable_code() {
        let body = serde_json::json!({
            "code": "precondition_required",
            "message": "If-Match or If-None-Match is required"
        })
        .to_string();

        assert_eq!(
            error_detail(&body),
            "[precondition_required] If-Match or If-None-Match is required"
        );
    }

    #[test]
    fn api_url_joins_the_base_and_route_once() {
        assert_eq!(
            api_url("http://127.0.0.1:9080", "/streams"),
            "http://127.0.0.1:9080/streams"
        );
        assert_eq!(
            api_url("http://127.0.0.1:9080/", "/streams"),
            "http://127.0.0.1:9080/streams",
            "a trailing slash on the base does not double up"
        );
    }

    #[test]
    fn parses_read_commands_and_output_formats() {
        let cases = [
            (vec!["weave", "get", "nodes"], "nodes"),
            (vec!["weave", "get", "status"], "status"),
            (
                vec!["weave", "get", "endpoints", "cam1-to-studio"],
                "endpoints",
            ),
            (vec!["weave", "get", "stream-sets"], "stream-sets"),
            (vec!["weave", "get", "stream-set", "studio-a"], "stream-set"),
        ];

        for (args, expected) in cases {
            let cli = Cli::try_parse_from(args).unwrap();
            assert_eq!(cli.output, OutputFormat::Human);
            let Command::Get { resource } = cli.command else {
                panic!("expected get command");
            };
            let actual = match resource {
                GetResource::Nodes => "nodes",
                GetResource::Status => "status",
                GetResource::Endpoints { .. } => "endpoints",
                GetResource::StreamSets => "stream-sets",
                GetResource::StreamSet { .. } => "stream-set",
                GetResource::Streams | GetResource::Stream { .. } => "unexpected",
            };
            assert_eq!(actual, expected);
        }

        let cli = Cli::try_parse_from(["weave", "-o", "yaml", "get", "streams"]).unwrap();
        assert_eq!(cli.output, OutputFormat::Yaml);
        let cli = Cli::try_parse_from(["weave", "get", "streams", "--output", "json"]).unwrap();
        assert_eq!(cli.output, OutputFormat::Json);
        assert!(Cli::try_parse_from(["weave", "nodes"]).is_err());
    }

    #[test]
    fn parses_apply_set_command_and_strict_yaml() {
        let cli = Cli::try_parse_from([
            "weave",
            "apply-set",
            "studio-a",
            "-f",
            "streams.yaml",
            "--output",
            "json",
        ])
        .unwrap();
        assert_eq!(cli.output, OutputFormat::Json);
        let Command::ApplySet { owner, file } = cli.command else {
            panic!("expected apply-set command");
        };
        assert_eq!(owner, "studio-a");
        assert_eq!(file, PathBuf::from("streams.yaml"));

        let yaml = r#"
streams:
  - name: cam1-to-studio
    source:
      srt: { node: strom-node-1 }
    destinations:
      - id: studio
        srt: { node: strom-node-2 }
"#;
        let apply = parse_stream_set(yaml).unwrap();
        assert!(!apply.prune);
        assert_eq!(apply.streams, vec![sample_stream()]);

        let invalid = format!("{yaml}unknown: true\n");
        assert!(parse_stream_set(&invalid).is_err());
    }

    #[test]
    fn json_and_yaml_outputs_preserve_the_body() {
        let value = serde_json::json!({
            "generation": 3,
            "owner": "studio-a",
            "spec": { "name": "cam1-to-studio" }
        });
        let body = value.to_string();

        let json = format_output(OutputFormat::Json, &body, String::new()).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&json).unwrap(), value);

        let yaml = format_output(OutputFormat::Yaml, &body, String::new()).unwrap();
        assert_eq!(serde_norway::from_str::<Value>(&yaml).unwrap(), value);
    }

    fn listed(generation: u64, owner: Option<&str>) -> read::StreamResource {
        read::StreamResource {
            generation,
            owner: owner.map(str::to_string),
            spec: read::StreamSpec {
                name: "cam1-to-studio".to_string(),
                enabled: true,
            },
        }
    }

    #[test]
    fn human_stream_and_endpoint_tables_are_stable() {
        let streams = vec![listed(3, Some("studio-a"))];
        assert_eq!(
            render_streams(&streams, &[]),
            "NAME            GENERATION  STATUS  OWNER     ENABLED\ncam1-to-studio  3           -       studio-a  true"
        );

        let endpoints: read::StreamEndpoints = serde_json::from_value(serde_json::json!({
            "ingress": {
                "node": "strom-node-1",
                "host": "172.26.0.10",
                "port": 20000,
                "url": "srt://172.26.0.10:20000"
            },
            "destinations": [{ "id": "studio", "endpoint": null }]
        }))
        .unwrap();
        assert_eq!(
            render_endpoints(&endpoints),
            "ROLE         ID      NODE          URL\ningress      -       strom-node-1  srt://172.26.0.10:20000\ndestination  studio  -             -"
        );
    }

    #[test]
    fn human_tables_name_states_as_the_api_does() {
        let status: read::StatusResponse = serde_json::from_value(serde_json::json!({
            "status": "converging",
            "summary": "0/1 stream(s) flowing across 2 node(s)",
            "streams": [{
                "name": "cam1-to-studio",
                "generation": 2,
                "observed_generation": 1,
                "status": "awaiting_input",
                "nodes": ["strom-node-1", "strom-node-2"],
                "conditions": [],
                "ingress": null,
                "destinations": []
            }]
        }))
        .unwrap();
        let rendered = render_status(&status);
        assert_eq!(rendered.lines().next(), Some("STATUS: converging"));
        assert_eq!(
            rendered
                .lines()
                .last()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>(),
            [
                "cam1-to-studio",
                "2",
                "1",
                "awaiting_input",
                "strom-node-1,strom-node-2"
            ]
        );

        let plan: read::StreamPlan = serde_json::from_value(serde_json::json!({
            "name": "cam1-to-studio",
            "status": "unplaced",
            "nodes": [],
            "hops": [],
            "reason": "no node named strom-node-9"
        }))
        .unwrap();
        assert_eq!(
            render_plan(&plan)
                .lines()
                .last()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>(),
            [
                "cam1-to-studio",
                "unplaced",
                "-",
                "no",
                "node",
                "named",
                "strom-node-9"
            ]
        );

        let nodes: Vec<read::NodeDescriptor> = serde_json::from_value(serde_json::json!([{
            "id": "strom-node-1",
            "endpoint": "http://strom-node-1:8091",
            "status": "offline",
            "capabilities": { "adapters": [], "hop_profiles": [] },
            "topology": { "attachments": [] }
        }]))
        .unwrap();
        assert_eq!(
            render_nodes(&nodes)
                .lines()
                .last()
                .unwrap()
                .split_whitespace()
                .collect::<Vec<_>>(),
            ["strom-node-1", "offline", "http://strom-node-1:8091"]
        );
    }

    fn degraded_status() -> Value {
        let condition = |kind: &str, status: &str, reason: &str, detail: &str| {
            serde_json::json!({
                "type": kind,
                "status": status,
                "reason": reason,
                "detail": detail,
                "last_transition_time": "2026-10-15T18:00:00Z"
            })
        };
        let flowing = condition("media_flowing", "true", "media_flowing", "media is flowing");
        let degraded = condition(
            "media_flowing",
            "false",
            "media_degraded",
            "media is not flowing across every branch",
        );
        serde_json::json!({
            "status": "degraded",
            "summary": "0/1 stream(s) flowing across 2 node(s)",
            "streams": [{
                "name": "cam1-to-studio",
                "generation": 3,
                "observed_generation": 3,
                "status": "degraded",
                "nodes": ["strom-node-1", "strom-node-2"],
                "conditions": [
                    condition("placement_ready", "true", "placed", "the stream has a complete path"),
                    degraded.clone()
                ],
                "ingress": null,
                "destinations": [
                    {
                        "id": "preview",
                        "status": "degraded",
                        "nodes": ["strom-node-1", "strom-node-2"],
                        "conditions": [degraded],
                        "endpoint": {
                            "node": "strom-node-2",
                            "host": "192.0.2.2",
                            "port": 21567,
                            "url": "srt://192.0.2.2:21567"
                        }
                    },
                    {
                        "id": "studio",
                        "status": "flowing",
                        "nodes": ["strom-node-1", "strom-node-2"],
                        "conditions": [flowing],
                        "endpoint": null
                    }
                ]
            }]
        })
    }

    fn words(line: &str) -> Vec<&str> {
        line.split_whitespace().collect()
    }

    #[test]
    fn human_stream_views_show_status_and_why() {
        let status: read::StatusResponse = serde_json::from_value(degraded_status()).unwrap();
        let stream = listed(3, None);

        let list = render_streams(std::slice::from_ref(&stream), status.streams());
        assert_eq!(
            words(list.lines().last().unwrap()),
            ["cam1-to-studio", "3", "degraded", "-", "true"]
        );

        let view = render_stream(&stream, status.streams());
        let lines = view.lines().map(words).collect::<Vec<_>>();
        assert!(lines.contains(&vec![
            "media_flowing",
            "false",
            "media_degraded",
            "media",
            "is",
            "not",
            "flowing",
            "across",
            "every",
            "branch"
        ]));
        assert!(lines.contains(&vec![
            "preview",
            "degraded",
            "srt://192.0.2.2:21567",
            "media_degraded"
        ]));
        assert!(lines.contains(&vec!["studio", "flowing", "-", "-"]));
    }

    #[test]
    fn a_stream_the_controller_has_not_reported_reads_no_status() {
        let stream = listed(1, None);
        let starting: read::StatusResponse =
            serde_json::from_value(serde_json::json!({ "status": "starting" })).unwrap();
        let view = render_stream(&stream, starting.streams());
        assert_eq!(
            view.lines().map(words).collect::<Vec<_>>(),
            [
                vec!["NAME", "GENERATION", "STATUS", "OWNER", "ENABLED"],
                vec!["cam1-to-studio", "1", "-", "-", "true"]
            ]
        );
    }

    #[test]
    fn parsed_yaml_serializes_to_northbound_json_shape() {
        let yaml = r#"
name: paused
enabled: false
source:
  srt:
    node: strom-node-1
destinations:
  - id: studio
    srt:
      node: strom-node-2
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");
        assert!(!stream.enabled);

        let json = serde_json::to_value(&stream).unwrap();
        assert_eq!(json["source"]["srt"]["node"], "strom-node-1");
        assert_eq!(json["destinations"][0]["srt"]["node"], "strom-node-2");
    }

    fn sample_stream() -> StreamDefinition {
        StreamDefinition {
            name: "cam1-to-studio".to_string(),
            enabled: true,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                passphrase: None,
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("strom-node-2".to_string()),
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

    #[derive(Clone)]
    struct ResourceStub {
        seen: Arc<Mutex<Vec<String>>>,
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
        include_etag: bool,
    }

    async fn stub_resource(
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        start_resource_stub(resource, mutation_status, true).await
    }

    async fn start_resource_stub(
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
        include_etag: bool,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        async fn record(State(state): State<ResourceStub>, request: Request<Body>) -> Response {
            let authorization = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let if_match = request
                .headers()
                .get(axum::http::header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let if_none_match = request
                .headers()
                .get(axum::http::header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            state.seen.lock().unwrap().push(format!(
                "{} {} {authorization} {if_match} {if_none_match}",
                request.method(),
                request.uri().path()
            ));

            if request.method() == axum::http::Method::GET {
                return match &state.resource {
                    Some(resource) => {
                        let mut response = axum::Json(resource).into_response();
                        if state.include_etag {
                            response.headers_mut().insert(
                                axum::http::header::ETAG,
                                axum::http::HeaderValue::from_static("\"revision-7\""),
                            );
                        }
                        response
                    }
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(ApiError::new(
                            ApiErrorCode::StreamNotFound,
                            "stream not found",
                        )),
                    )
                        .into_response(),
                };
            }

            if state.mutation_status.is_success() {
                if request.method() == axum::http::Method::DELETE {
                    return StatusCode::NO_CONTENT.into_response();
                }
                return (
                    state.mutation_status,
                    axum::Json(StreamAccepted {
                        status: weave_core::AcceptedState::Accepted,
                        name: "cam1-to-studio".to_string(),
                        generation: state
                            .resource
                            .as_ref()
                            .map_or(1, |resource| resource.generation + 1),
                        changed: true,
                    }),
                )
                    .into_response();
            }

            let code = if state.mutation_status == StatusCode::PRECONDITION_REQUIRED {
                ApiErrorCode::PreconditionRequired
            } else {
                ApiErrorCode::PreconditionFailed
            };
            (
                state.mutation_status,
                axum::Json(ApiError::new(code, "stream revision precondition failed")),
            )
                .into_response()
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(record).with_state(ResourceStub {
            seen: Arc::clone(&seen),
            resource,
            mutation_status,
            include_etag,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[derive(Clone)]
    struct StreamSetStub {
        seen: Arc<Mutex<Vec<String>>>,
        existing: bool,
        mutation_status: StatusCode,
    }

    async fn stub_stream_set(
        existing: bool,
        mutation_status: StatusCode,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        async fn record(
            State(state): State<StreamSetStub>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            body: Bytes,
        ) -> Response {
            let if_match = headers
                .get(axum::http::header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let if_none_match = headers
                .get(axum::http::header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let body_summary = if method == axum::http::Method::PUT {
                let apply: StreamSetApply = serde_json::from_slice(&body).unwrap();
                format!("{} {}", apply.streams.len(), apply.prune)
            } else {
                "- -".to_string()
            };
            state.seen.lock().unwrap().push(format!(
                "{method} {} {if_match} {if_none_match} {body_summary}",
                uri.path()
            ));

            if method == axum::http::Method::GET {
                if !state.existing {
                    return (
                        StatusCode::NOT_FOUND,
                        axum::Json(ApiError::new(
                            ApiErrorCode::StreamSetNotFound,
                            "stream set not found",
                        )),
                    )
                        .into_response();
                }
                let mut response = axum::Json(StreamSetResource {
                    owner: "studio-a".to_string(),
                    streams: vec![StreamResource {
                        generation: 7,
                        owner: Some("studio-a".to_string()),
                        spec: sample_stream(),
                    }],
                })
                .into_response();
                response.headers_mut().insert(
                    axum::http::header::ETAG,
                    axum::http::HeaderValue::from_static("\"set-revision-7\""),
                );
                return response;
            }

            if state.mutation_status.is_success() {
                return (
                    state.mutation_status,
                    axum::Json(StreamSetAccepted {
                        status: weave_core::AcceptedState::Accepted,
                        owner: "studio-a".to_string(),
                        changed: true,
                        streams: vec![StreamSetMemberResult {
                            name: "cam1-to-studio".to_string(),
                            generation: if state.existing { 8 } else { 1 },
                            action: if state.existing {
                                StreamSetAction::Updated
                            } else {
                                StreamSetAction::Created
                            },
                        }],
                        pruned: Vec::new(),
                    }),
                )
                    .into_response();
            }

            let code = if state.mutation_status == StatusCode::CONFLICT {
                ApiErrorCode::OwnershipConflict
            } else {
                ApiErrorCode::PreconditionFailed
            };
            (
                state.mutation_status,
                axum::Json(ApiError::new(code, "stream-set mutation failed")),
            )
                .into_response()
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(record).with_state(StreamSetStub {
            seen: Arc::clone(&seen),
            existing,
            mutation_status,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    async fn stub_plan() -> (String, Arc<Mutex<Option<String>>>) {
        async fn record(
            State(seen): State<Arc<Mutex<Option<String>>>>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            axum::Json(stream): axum::Json<StreamDefinition>,
        ) -> axum::Json<StreamPlan> {
            let authorization = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            *seen.lock().unwrap() = Some(format!(
                "{method} {} {authorization} {}",
                uri.path(),
                stream.name
            ));
            axum::Json(StreamPlan {
                name: stream.name,
                status: weave_core::PlanStatus::Unplaced,
                nodes: Vec::new(),
                hops: Vec::new(),
                endpoints: None,
                reason: Some("node missing is not registered".to_string()),
            })
        }

        let seen = Arc::new(Mutex::new(None));
        let app = Router::new().fallback(record).with_state(Arc::clone(&seen));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[derive(Clone)]
    struct GetStub {
        seen: Arc<Mutex<Option<String>>>,
        status: StatusCode,
        body: Value,
    }

    async fn stub_get(status: StatusCode, body: Value) -> (String, Arc<Mutex<Option<String>>>) {
        async fn record(
            State(state): State<GetStub>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
        ) -> Response {
            let authorization = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            *state.seen.lock().unwrap() = Some(format!("{method} {} {authorization}", uri.path()));
            (state.status, axum::Json(state.body)).into_response()
        }

        let seen = Arc::new(Mutex::new(None));
        let app = Router::new().fallback(record).with_state(GetStub {
            seen: Arc::clone(&seen),
            status,
            body,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn read_commands_call_their_routes_with_auth() {
        let token = Token::new("cli-test-token").unwrap();
        let cases = [
            (
                "/nodes",
                serde_json::json!([{
                    "id": "strom-node-1",
                    "endpoint": "http://strom-node-1:8091",
                    "status": "ready",
                    "topology": { "attachments": [] }
                }]),
                "nodes",
            ),
            (
                "/status",
                serde_json::json!({ "status": "starting" }),
                "status",
            ),
            (
                "/streams/cam1-to-studio/endpoints",
                serde_json::json!({
                    "ingress": null,
                    "destinations": [{ "id": "studio", "endpoint": null }]
                }),
                "endpoints",
            ),
            ("/stream-sets", serde_json::json!([]), "stream-sets"),
            (
                "/stream-sets/studio-a",
                serde_json::json!({ "owner": "studio-a", "streams": [] }),
                "stream-set",
            ),
        ];

        for (path, body, command) in cases {
            let (url, seen) = stub_get(StatusCode::OK, body.clone()).await;
            let printed = match command {
                "nodes" => get_nodes(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "status" => get_status(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "endpoints" => {
                    get_endpoints(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
                        .await
                        .unwrap()
                }
                "stream-sets" => get_stream_sets(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "stream-set" => get_stream_set(&url, Some(&token), "studio-a", OutputFormat::Json)
                    .await
                    .unwrap(),
                _ => unreachable!(),
            };
            assert_eq!(
                seen.lock().unwrap().as_deref(),
                Some(format!("GET {path} Bearer cli-test-token").as_str())
            );
            assert_eq!(
                serde_json::from_str::<Value>(&printed).unwrap(),
                body,
                "-o json prints the body as returned"
            );
        }
    }

    #[tokio::test]
    async fn human_stream_views_read_the_status_and_json_does_not() {
        async fn route(
            State(seen): State<Arc<Mutex<Vec<String>>>>,
            uri: axum::http::Uri,
        ) -> Response {
            seen.lock().unwrap().push(uri.path().to_string());
            let stream = StreamResource {
                generation: 3,
                owner: None,
                spec: sample_stream(),
            };
            match uri.path() {
                "/streams" => axum::Json(vec![stream]).into_response(),
                "/streams/cam1-to-studio" => {
                    let mut response = axum::Json(stream).into_response();
                    response.headers_mut().insert(
                        axum::http::header::ETAG,
                        axum::http::HeaderValue::from_static("\"revision-3\""),
                    );
                    response
                }
                "/status" => axum::Json(degraded_status()).into_response(),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(route).with_state(Arc::clone(&seen));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let token = Token::new("cli-test-token").unwrap();

        get_streams(&url, Some(&token), OutputFormat::Human)
            .await
            .unwrap();
        get_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Human)
            .await
            .unwrap();
        assert_eq!(
            std::mem::take(&mut *seen.lock().unwrap()),
            ["/streams", "/status", "/streams/cam1-to-studio", "/status"]
        );

        get_streams(&url, Some(&token), OutputFormat::Json)
            .await
            .unwrap();
        get_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
            .await
            .unwrap();
        assert_eq!(
            std::mem::take(&mut *seen.lock().unwrap()),
            ["/streams", "/streams/cam1-to-studio"]
        );
    }

    fn newer_stream() -> Value {
        serde_json::json!({
            "generation": 4,
            "labels": { "site": "north" },
            "spec": {
                "name": "cam1-to-studio",
                "enabled": true,
                "priority": "high",
                "source": { "srt": { "node": "strom-node-1" } },
                "destinations": [{ "id": "studio", "rist": { "node": "strom-node-2" } }]
            }
        })
    }

    fn newer_status() -> Value {
        let preempted = serde_json::json!({
            "type": "media_flowing",
            "status": "false",
            "reason": "source_preempted",
            "detail": "another source holds the slot",
            "last_transition_time": "2026-10-16T10:00:00Z",
            "severity": "warning"
        });
        serde_json::json!({
            "status": "converging",
            "summary": "0/1 stream(s) flowing across 2 node(s)",
            "uptime_secs": 30,
            "streams": [{
                "name": "cam1-to-studio",
                "generation": 4,
                "observed_generation": 4,
                "status": "preempted",
                "nodes": ["strom-node-1", "strom-node-2"],
                "conditions": [preempted.clone()],
                "ingress": null,
                "destinations": [{
                    "id": "studio",
                    "status": "preempted",
                    "nodes": ["strom-node-1", "strom-node-2"],
                    "conditions": [preempted],
                    "endpoint": null
                }]
            }]
        })
    }

    async fn stub_newer_server() -> String {
        async fn route(method: axum::http::Method, uri: axum::http::Uri) -> Response {
            match (method.as_str(), uri.path()) {
                ("GET", "/streams") => axum::Json(vec![newer_stream()]).into_response(),
                ("GET", "/streams/cam1-to-studio") => (
                    [(axum::http::header::ETAG, "\"revision-4\"")],
                    axum::Json(newer_stream()),
                )
                    .into_response(),
                ("GET", "/status") => axum::Json(newer_status()).into_response(),
                ("POST", "/streams") => (
                    StatusCode::ACCEPTED,
                    [(axum::http::header::ETAG, "\"revision-5\"")],
                    axum::Json(serde_json::json!({
                        "status": "accepted",
                        "name": "cam1-to-studio",
                        "generation": 5,
                        "changed": true,
                        "warnings": []
                    })),
                )
                    .into_response(),
                ("DELETE", "/streams/cam1-to-studio") => StatusCode::NO_CONTENT.into_response(),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(route))
                .await
                .unwrap()
        });
        url
    }

    #[tokio::test]
    async fn a_newer_servers_stream_is_read_printed_applied_and_deleted() {
        let url = stub_newer_server().await;
        let name = "cam1-to-studio";

        let view = get_stream(&url, None, name, OutputFormat::Human)
            .await
            .unwrap();
        let lines = view.lines().map(words).collect::<Vec<_>>();
        assert!(
            lines.contains(&vec!["cam1-to-studio", "4", "preempted", "-", "true"]),
            "{view}"
        );
        assert!(
            lines.contains(&vec![
                "media_flowing",
                "false",
                "source_preempted",
                "another",
                "source",
                "holds",
                "the",
                "slot"
            ]),
            "{view}"
        );
        assert!(
            lines.contains(&vec!["studio", "preempted", "-", "source_preempted"]),
            "{view}"
        );

        let list = get_streams(&url, None, OutputFormat::Human).await.unwrap();
        assert_eq!(
            words(list.lines().last().unwrap()),
            ["cam1-to-studio", "4", "preempted", "-", "true"]
        );

        let json = get_stream(&url, None, name, OutputFormat::Json)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json).unwrap(),
            newer_stream()
        );
        let yaml = get_stream(&url, None, name, OutputFormat::Yaml)
            .await
            .unwrap();
        assert_eq!(
            serde_norway::from_str::<Value>(&yaml).unwrap(),
            newer_stream()
        );

        let applied = apply_stream(&url, None, &sample_stream(), OutputFormat::Human)
            .await
            .unwrap();
        assert_eq!(
            words(applied.lines().last().unwrap()),
            ["cam1-to-studio", "5", "true"]
        );
        let deleted = delete_stream(&url, None, name, OutputFormat::Human)
            .await
            .unwrap();
        assert_eq!(deleted, "deleted stream cam1-to-studio");
    }

    #[tokio::test]
    async fn get_commands_surface_structured_errors() {
        let error = serde_json::json!({
            "code": "controller_unreachable",
            "message": "controller unreachable",
            "details": [{
                "field": "controller",
                "code": "unavailable",
                "message": "try again later"
            }]
        });
        let (url, _seen) = stub_get(StatusCode::BAD_GATEWAY, error).await;

        let error = get_nodes(&url, None, OutputFormat::Human)
            .await
            .expect_err("a failed GET must preserve the structured error");
        assert!(
            error
                .to_string()
                .contains("[controller_unreachable] controller unreachable")
        );
        assert!(
            error
                .to_string()
                .contains("controller [unavailable]: try again later")
        );
    }

    #[tokio::test]
    async fn endpoint_and_stream_set_ids_are_validated_locally() {
        let endpoint_error = get_endpoints("not a URL", None, "foo?ignored", OutputFormat::Human)
            .await
            .expect_err("unsafe stream name must be rejected locally");
        assert!(
            endpoint_error
                .to_string()
                .starts_with("invalid stream name:")
        );

        let owner_error = get_stream_set("not a URL", None, "foo?ignored", OutputFormat::Human)
            .await
            .expect_err("unsafe owner must be rejected locally");
        assert!(
            owner_error
                .to_string()
                .starts_with("invalid stream-set owner:")
        );
    }

    #[tokio::test]
    async fn plan_calls_the_route_with_auth_and_body() {
        let token = Token::new("cli-test-token").unwrap();
        let stream = StreamDefinition {
            name: "preview".to_string(),
            enabled: true,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("missing".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                passphrase: None,
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamDestination {
                id: "preview".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("also-missing".to_string()),
                    remote: None,
                    via: Vec::new(),
                    network: None,
                    latency: None,
                    passphrase: None,
                    format: None,
                    accepts: None,
                }),
            }],
        };
        let (url, seen) = stub_plan().await;

        plan_stream(&url, Some(&token), &stream, OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("POST /stream-plans Bearer cli-test-token preview")
        );
    }

    #[tokio::test]
    async fn get_stream_calls_the_route_with_auth() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        get_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["GET /streams/cam1-to-studio Bearer cli-test-token - -"]
        );
    }

    #[tokio::test]
    async fn apply_set_creates_with_one_conditional_put() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: true,
        };
        let (url, seen) = stub_stream_set(false, StatusCode::ACCEPTED).await;

        apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /stream-sets/studio-a - - - -",
                "PUT /stream-sets/studio-a - * 1 true"
            ]
        );
    }

    #[tokio::test]
    async fn apply_set_updates_with_the_set_etag() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: false,
        };
        let (url, seen) = stub_stream_set(true, StatusCode::ACCEPTED).await;

        apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Human)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /stream-sets/studio-a - - - -",
                "PUT /stream-sets/studio-a \"set-revision-7\" - 1 false"
            ]
        );
    }

    #[tokio::test]
    async fn apply_set_surfaces_conflicts_without_retrying() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: false,
        };

        for (status, code) in [
            (StatusCode::CONFLICT, "ownership_conflict"),
            (StatusCode::PRECONDITION_FAILED, "precondition_failed"),
        ] {
            let (url, seen) = stub_stream_set(true, status).await;
            let error = apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Json)
                .await
                .expect_err("conflicting set apply must fail");
            assert!(error.to_string().contains(&format!("[{code}]")), "{error}");
            assert_eq!(seen.lock().unwrap().len(), 2, "apply-set must not retry");
        }
    }

    #[tokio::test]
    async fn apply_set_rejects_an_unsafe_owner_locally() {
        let apply = StreamSetApply {
            streams: Vec::new(),
            prune: true,
        };
        let error = apply_stream_set(
            "not a URL",
            None,
            "foo?ignored",
            &apply,
            OutputFormat::Human,
        )
        .await
        .expect_err("unsafe owner must be rejected locally");
        assert!(error.to_string().starts_with("invalid stream-set owner:"));
    }

    #[test]
    fn human_apply_set_output_lists_member_actions_and_prunes() {
        let accepted = read::StreamSetAccepted {
            owner: "studio-a".to_string(),
            changed: true,
            streams: vec![read::StreamSetMemberResult {
                name: "cam1-to-studio".to_string(),
                generation: 2,
                action: read::Open::Known(StreamSetAction::Updated),
            }],
            pruned: vec!["old-feed".to_string()],
        };

        assert_eq!(
            render_stream_set_accepted(&accepted),
            "OWNER: studio-a\nCHANGED: true\n\nNAME            ACTION   GENERATION\ncam1-to-studio  updated  2\nold-feed        pruned   -"
        );
    }

    #[tokio::test]
    async fn apply_creates_with_if_none_match_after_a_missing_lookup() {
        let token = Token::new("cli-test-token").unwrap();
        let (url, seen) = stub_resource(None, StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream(), OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /streams Bearer cli-test-token - *"
            ]
        );
    }

    #[tokio::test]
    async fn apply_updates_once_with_the_revision_it_read() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream(), OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /streams Bearer cli-test-token \"revision-7\" -"
            ]
        );
    }

    #[tokio::test]
    async fn apply_surfaces_a_structured_conflict_without_retrying() {
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::PRECONDITION_FAILED).await;

        let error = apply_stream(&url, None, &sample_stream(), OutputFormat::Json)
            .await
            .expect_err("a stale revision must fail");

        assert!(
            error.to_string().contains("[precondition_failed]"),
            "{error}"
        );
        assert_eq!(seen.lock().unwrap().len(), 2, "apply must not retry 412");
    }

    #[tokio::test]
    async fn apply_refuses_to_update_without_the_get_etag() {
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = start_resource_stub(Some(resource), StatusCode::ACCEPTED, false).await;

        let error = apply_stream(&url, None, &sample_stream(), OutputFormat::Json)
            .await
            .expect_err("an update needs the revision ETag");

        assert!(error.to_string().contains("missing its ETag"), "{error}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lookup_returns_the_etag_and_the_body_as_sent() {
        let resource = StreamResource {
            generation: 23,
            owner: None,
            spec: sample_stream(),
        };
        let (url, _seen) = stub_resource(Some(resource.clone()), StatusCode::ACCEPTED).await;

        let found = lookup(
            &reqwest::Client::new(),
            &url,
            None,
            "/streams/cam1-to-studio",
            "stream",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(found.etag, "\"revision-7\"");
        assert_eq!(
            serde_json::from_str::<Value>(&found.body).unwrap(),
            serde_json::to_value(resource).unwrap()
        );
    }

    #[tokio::test]
    async fn get_stream_rejects_an_unsafe_name_locally() {
        let error = get_stream("not a URL", None, "foo?ignored", OutputFormat::Json)
            .await
            .expect_err("unsafe name must be rejected locally");
        assert!(error.to_string().starts_with("invalid stream name:"));
    }

    #[tokio::test]
    async fn delete_calls_the_route_and_reports_an_unknown_stream() {
        let token = Token::new("cli-test-token").unwrap();

        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::NO_CONTENT).await;
        delete_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
            .await
            .expect("204 deletes the stream");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /streams/cam1-to-studio Bearer cli-test-token - -",
                "DELETE /streams/cam1-to-studio Bearer cli-test-token \"revision-7\" -"
            ]
        );

        let (url, _seen) = stub_resource(None, StatusCode::NO_CONTENT).await;
        let err = delete_stream(&url, Some(&token), "missing", OutputFormat::Json)
            .await
            .expect_err("404 is an error");
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[tokio::test]
    async fn delete_rejects_an_unsafe_name_before_building_a_request() {
        let error = delete_stream("not a URL", None, "foo?ignored", OutputFormat::Json)
            .await
            .expect_err("unsafe name must be rejected locally");
        assert_eq!(
            error.to_string(),
            "invalid stream name: must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
        );
    }
}
