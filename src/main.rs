use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_knowledge::memory::{self, Claim, Kind, Memory};
use agent_knowledge::plug::{self, Agent};
use agent_knowledge::sync::{self, Options, Presence, Ready};
use anyhow::{Context, bail};
use clap::{Parser, Subcommand};

/// Share a folder with live two-way sync over habilis-network.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Share a folder. Prints the hash that `mount` takes, then syncs until Ctrl-C.
    Share {
        folder: PathBuf,
        /// This peer's nickname in the mesh. Default: a random one.
        #[arg(long)]
        nickname: Option<String>,
    },
    /// Sync <target> with the folder that `share` printed <hash> for, until Ctrl-C.
    Mount {
        hash: String,
        target: PathBuf,
        /// This peer's nickname in the mesh. Default: a random one.
        #[arg(long)]
        nickname: Option<String>,
    },
    /// Copy the knowledge skills into each agent's skills folder.
    Plug {
        /// Agent to plug into. Repeat for more than one. Default: every detected agent.
        #[arg(long = "agent", value_enum)]
        agents: Vec<Agent>,
    },
    /// Print the folder of this agent session's memory, or fail if it runs none.
    Status,
    /// Stop this agent session's memory: it leaves the mesh, and its files stay as they are.
    Unmount,
}

/// The project emoji. Every line printed for the chat starts with it.
const EMOJI: &str = "📚";

/// How long `unmount` waits for the memory to leave the mesh and exit.
const UNMOUNT_WAIT: Duration = Duration::from_secs(15);

/// The chat line for another peer joining or leaving.
fn notice(presence: &Presence) -> String {
    match presence {
        Presence::Joined(nick) => format!("{EMOJI} {nick} joined"),
        Presence::Left(nick) => format!("{EMOJI} {nick} left"),
    }
}

/// Stdout, not stderr: the skills relay every `📚` line on stdout into the chat.
fn print_nickname(nick: &str) {
    println!("{EMOJI} joined as {nick}");
}

/// Print another peer's join or leave notice on stdout, for the skills to relay.
fn print_presence(presence: Presence) {
    println!("{}", notice(&presence));
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Claim `folder` as this session's one memory. Fails if the session already runs one.
fn claim(kind: Kind, folder: &Path) -> anyhow::Result<Claim> {
    let folder = std::path::absolute(folder).with_context(|| format!("bad folder {}", folder.display()))?;
    let entry = Memory { pid: std::process::id(), kind, folder };
    Claim::acquire(&memory::registry_dir(env)?, &memory::session_id(env)?, entry)
}

/// The registry, this session, and the memory it runs. Fails if it runs none.
fn running() -> anyhow::Result<(PathBuf, String, Memory)> {
    let registry = memory::registry_dir(env)?;
    let session = memory::session_id(env)?;
    let Some(running) = memory::current(&registry, &session)? else {
        bail!("no memory bank in this session ({session}); start one with knowledge-share or knowledge-mount");
    };
    Ok((registry, session, running))
}

fn status() -> anyhow::Result<()> {
    let (_, _, running) = running()?;
    println!("{}", running.folder.display());
    Ok(())
}

fn unmount() -> anyhow::Result<()> {
    let (registry, session, running) = running()?;
    let (kind, folder) = (running.kind.as_str(), running.folder.display());
    if !memory::stop(&registry, &session, &running, UNMOUNT_WAIT)? {
        bail!(
            "the {kind} at {folder} (pid {}) did not stop within {}s; `kill -9 {}` forces it, without a clean leave",
            running.pid,
            UNMOUNT_WAIT.as_secs(),
            running.pid
        );
    }
    println!("{EMOJI} left the {kind} at {folder}; files kept");
    Ok(())
}

/// Resolves on Ctrl-C (SIGINT) or SIGTERM, so `kill <pid>` and a stopped background task also
/// leave the mesh cleanly. The listeners register here, before the mesh join, so a signal that
/// lands while the join is still running is kept rather than killing the process.
#[cfg(unix)]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
    })
}

fn run_plug(explicit: Vec<Agent>) -> anyhow::Result<()> {
    let env = |key: &str| std::env::var(key).ok();
    let agents = plug::selected(&explicit, |agent| plug::detected(agent, env));
    if agents.is_empty() {
        println!("no agent detected · use --agent claude-code|codex|pi");
    }
    for agent in agents {
        if !plug::detected(agent, env) {
            println!("skipped\t{}\tnot detected", agent.label());
            continue;
        }
        let root = plug::skills_dir(agent, env)?;
        plug::install(&root)?;
        println!("installed\t{}\t{}", agent.label(), root.display());
    }
    Ok(())
}

/// Parse the command line and run the chosen command; `share` and `mount` sync until Ctrl-C.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "error".into()))
        .init();
    match Cli::parse().command {
        Command::Share { folder, nickname } => {
            let _claim = claim(Kind::Share, &folder)?;
            let shutdown = shutdown_signal()?;
            let on_ready = |ready: &Ready| {
                println!("{}", ready.mesh);
                print_nickname(&ready.nick);
            };
            let options = Options { root: folder, mesh: None, nickname };
            sync::run(options, on_ready, print_presence, shutdown).await
        }
        Command::Mount { hash, target, nickname } => {
            let _claim = claim(Kind::Mount, &target)?;
            let shutdown = shutdown_signal()?;
            std::fs::create_dir_all(&target)?;
            let on_ready = |ready: &Ready| {
                eprintln!("mounted at {}", target.display());
                print_nickname(&ready.nick);
            };
            let options = Options { root: target.clone(), mesh: Some(hash), nickname };
            sync::run(options, on_ready, print_presence, shutdown).await
        }
        Command::Plug { agents } => run_plug(agents),
        Command::Status => status(),
        Command::Unmount => unmount(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_start_with_the_project_emoji() {
        assert_eq!(notice(&Presence::Joined("b".to_owned())), "📚 b joined");
        assert_eq!(notice(&Presence::Left("b".to_owned())), "📚 b left");
    }
}
