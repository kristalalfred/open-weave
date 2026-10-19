use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "weave-node",
    version,
    about = "Native SRT media node with fixed or automatically calibrated delivery latency"
)]
struct Args {
    #[arg(
        long,
        env = "WEAVE_NODE_CONFIG",
        hide_env_values = true,
        help = "Path to the native node YAML configuration"
    )]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weave_core::logging::init();
    let args = Args::parse();
    let text = tokio::fs::read_to_string(&args.config)
        .await
        .context("reading native node config")?;
    let config: weave_media::node::Config =
        serde_norway::from_str(&text).context("parsing native node config")?;
    weave_media::node::run(config).await
}
