mod media;
mod output;
mod report;
mod runtime;
mod scenario;

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use serde_json::json;

use output::{CaseOutput, ProgressMode};
use report::{Report, write_json};
use runtime::Bench;
use scenario::Scenario;

#[derive(Parser)]
#[command(about = "Run media scenarios against the local open-weave bench")]
struct Cli {
    #[arg(long, default_value_os_t = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench"), global = true)]
    bench: PathBuf,
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_URL",
        hide_env_values = true,
        default_value = "https://localhost:29443",
        global = true
    )]
    url: String,
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_TOKEN",
        default_value = "bench-northbound-token",
        hide_env_values = true,
        global = true
    )]
    token: String,
    #[arg(
        long,
        env = "WEAVE_STROM_TOKEN",
        default_value = "bench-strom-key-for-local-use-only",
        hide_env_values = true,
        global = true
    )]
    strom_token: String,
    #[arg(long, default_value = media::IMAGE, global = true)]
    media_image: String,
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    #[command(about = "List the checked scenario corpus")]
    List,
    #[command(about = "Print the JSON Schema for scenario actions and assertions")]
    Schema,
    #[command(about = "Validate scenarios without starting media or changing the bench")]
    Check { cases: Vec<PathBuf> },
    #[command(
        about = "Run scenarios serially on an empty bench, restoring faults and removing test streams"
    )]
    Run {
        cases: Vec<PathBuf>,
        #[arg(long)]
        results: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = ProgressMode::Auto, help = "Live terminal progress, plain log progress, or result lines only")]
        progress: ProgressMode,
    },
}

struct Lock(PathBuf);

impl Lock {
    fn acquire(root: &Path) -> Result<Self> {
        let path = root.join(".e2e.lock");
        let mut file = OpenOptions::new().write(true).create_new(true).open(&path)
            .context("bench is locked by another test run; remove bench/.e2e.lock only after checking its process")?;
        use std::io::Write;
        writeln!(file, "{}", std::process::id())?;
        Ok(Self(path))
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn paths(root: &Path, requested: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if !requested.is_empty() {
        return requested
            .iter()
            .map(|path| {
                if path.is_file() {
                    Ok(path.clone())
                } else {
                    let path = root.join("cases").join(path).with_extension("yaml");
                    ensure!(path.is_file(), "scenario {} missing", path.display());
                    Ok(path)
                }
            })
            .collect();
    }
    let mut paths = std::fs::read_dir(root.join("cases"))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "yaml"));
    paths.sort();
    ensure!(!paths.is_empty(), "no scenarios found");
    Ok(paths)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if matches!(cli.command, Action::Schema) {
        println!(
            "{}",
            serde_json::to_string_pretty(&schemars::schema_for!(Scenario))?
        );
        return Ok(());
    }
    let requested = match &cli.command {
        Action::List | Action::Schema => &[][..],
        Action::Check { cases } | Action::Run { cases, .. } => cases,
    };
    let cases = paths(&cli.bench, requested)?
        .into_iter()
        .map(|path| {
            let scenario = Scenario::read(&path)?;
            Ok((path, scenario))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut names = std::collections::BTreeSet::new();
    for (_, scenario) in &cases {
        ensure!(
            names.insert(&scenario.name),
            "duplicate scenario name {}",
            scenario.name
        );
    }
    if !matches!(cli.command, Action::Run { .. }) {
        for (path, scenario) in cases {
            println!(
                "{}: {} ({})",
                scenario.name,
                scenario.description,
                path.display()
            );
        }
        return Ok(());
    }
    let Action::Run {
        results, progress, ..
    } = cli.command
    else {
        unreachable!()
    };
    let _lock = Lock::acquire(&cli.bench)?;
    let run_id = format!(
        "{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        std::process::id()
    );
    let directory = results.unwrap_or_else(|| cli.bench.join("results").join(&run_id));
    std::fs::create_dir_all(&directory)?;
    let mut outcomes = Vec::new();
    let mut interrupted = false;
    let case_count = cases.len();
    let run_started = Instant::now();
    for (index, (path, scenario)) in cases.into_iter().enumerate() {
        let started = Instant::now();
        let case_dir = directory.join(&scenario.name);
        let prefix = format!("e2e-{}-{index}", std::process::id());
        let mut output = CaseOutput::new(
            &scenario.name,
            &prefix,
            index + 1,
            case_count,
            &scenario.steps,
            progress,
        )?;
        let display = output.progress();
        let report = Report::new(&case_dir)?.with_observer(display.clone());
        let mut bench = Bench::new(
            cli.bench.clone(),
            cli.url.clone(),
            cli.token.clone(),
            cli.strom_token.clone(),
            cli.media_image.clone(),
            prefix,
            report.clone(),
        )?;
        std::fs::copy(&path, case_dir.join("scenario.yaml"))?;
        let preflight = tokio::select! {
            result = bench.preflight() => result,
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                Err(anyhow::anyhow!("interrupted during preflight"))
            }
            _ = terminate() => {
                interrupted = true;
                Err(anyhow::anyhow!("terminated during preflight"))
            }
        };
        if let Err(error) = preflight {
            let outcome = json!({"name": scenario.name, "passed": false, "preflight_error": format!("{error:#}")});
            report.event("result", &outcome)?;
            write_json(&case_dir.join("result.json"), &outcome)?;
            output.finish(&outcome)?;
            outcomes.push(outcome);
            break;
        }
        let sampler = runtime::telemetry(&bench);
        let result = tokio::select! {
            result = bench.steps(&scenario.steps, path.parent().unwrap()) => result,
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                Err(anyhow::anyhow!("interrupted"))
            }
            _ = terminate() => {
                interrupted = true;
                Err(anyhow::anyhow!("terminated"))
            }
        };
        let media = bench.media_summary();
        display.phase("collecting diagnostics", "");
        if let Err(error) = &result {
            display.failure(&format!("{error:#}"));
        }
        let mut diagnostic_errors = Vec::new();
        for service in [
            "controller",
            "controller-2",
            "southbound",
            "adapter-1",
            "adapter-2",
            "adapter-3",
            "adapter-4",
        ] {
            display.detail(&format!("capturing {service} logs"));
            let capture = async {
                let logs = runtime::logs(&format!("ow-{service}")).await?;
                std::fs::write(
                    case_dir.join(format!("{service}.log")),
                    report::redact(&logs),
                )?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = capture {
                diagnostic_errors.push(format!("{service}: {error:#}"));
            }
        }
        let cleanup = bench.cleanup().await;
        sampler.abort();
        let error = result.err().map(|error| format!("{error:#}"));
        let cleanup_error = cleanup.err().map(|error| format!("{error:#}"));
        let passed = error.is_none() && cleanup_error.is_none() && diagnostic_errors.is_empty();
        let outcome = json!({
            "name": scenario.name, "passed": passed, "elapsed_ms": started.elapsed().as_millis(),
            "error": error, "cleanup_error": cleanup_error, "media": media, "diagnostic_errors": diagnostic_errors,
        });
        report.event("result", &outcome)?;
        write_json(&case_dir.join("result.json"), &outcome)?;
        output.finish(&outcome)?;
        outcomes.push(outcome);
        if interrupted || cleanup_error.is_some() {
            break;
        }
    }
    let passed = outcomes.iter().all(|outcome| outcome["passed"] == true) && !interrupted;
    write_json(
        &directory.join("summary.json"),
        &json!({"run": run_id, "passed": passed, "cases": outcomes}),
    )?;
    let passing = outcomes
        .iter()
        .filter(|outcome| outcome["passed"] == true)
        .count();
    println!(
        "{} passed, {} failed, {} not run ({:.1}s)",
        passing,
        outcomes.len() - passing,
        case_count - outcomes.len(),
        run_started.elapsed().as_secs_f64()
    );
    println!("Reports: {}", directory.display());
    if !passed {
        bail!("bench scenarios failed");
    }
    Ok(())
}

async fn terminate() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    }
    #[cfg(not(unix))]
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

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
        let shown = env_values_shown(&Cli::command());
        assert!(
            shown.is_empty(),
            "--help prints the environment value of {shown:?}"
        );
    }
}
