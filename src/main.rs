use std::path::{Path, PathBuf};

use agent_knowledge::memory::{self, Claim, Kind, Memory};
use agent_knowledge::plug::{self, Agent};
use agent_knowledge::status::{self, Status};
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
    /// Copy the knowledge-share and knowledge-mount skills into each agent's skills folder.
    Plug {
        /// Agent to plug into. Repeat for more than one. Default: every detected agent.
        #[arg(long = "agent", value_enum)]
        agents: Vec<Agent>,
    },
    /// Print the folder of this agent session's memory, or fail if it runs none.
    Status,
}

/// The project emoji. Every line printed for the chat starts with it.
const EMOJI: &str = "📚";

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

/// Print this session's memory folder on the first line, so the skills can read it there, then
/// the `📚` status block once the running process has written one.
fn status() -> anyhow::Result<()> {
    let session = memory::session_id(env)?;
    let registry = memory::registry_dir(env)?;
    let Some(running) = memory::current(&registry, &session)? else {
        bail!("no memory bank in this session ({session}); start one with knowledge-share or knowledge-mount");
    };
    println!("{}", running.folder.display());
    if let Some(state) = Status::read(&memory::status_path(&registry, &session))? {
        println!("\n{}", status::render(&state, running.kind.as_str(), status::now()));
    }
    Ok(())
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
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match Cli::parse().command {
        Command::Share { folder, nickname } => {
            let claim = claim(Kind::Share, &folder)?;
            let on_ready = |ready: &Ready| {
                println!("{}", ready.mesh);
                print_nickname(&ready.nick);
            };
            let status_file = Some(claim.status_path().to_path_buf());
            let options = Options { root: folder, mesh: None, nickname, status_file };
            sync::run(options, on_ready, print_presence, shutdown).await
        }
        Command::Mount { hash, target, nickname } => {
            let claim = claim(Kind::Mount, &target)?;
            std::fs::create_dir_all(&target)?;
            let on_ready = |ready: &Ready| {
                eprintln!("mounted at {}", target.display());
                print_nickname(&ready.nick);
            };
            let status_file = Some(claim.status_path().to_path_buf());
            let options = Options { root: target.clone(), mesh: Some(hash), nickname, status_file };
            sync::run(options, on_ready, print_presence, shutdown).await
        }
        Command::Plug { agents } => run_plug(agents),
        Command::Status => status(),
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
