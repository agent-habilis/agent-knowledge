use std::path::PathBuf;

use agent_knowledge::plug::{self, Agent};
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
    Share { folder: PathBuf },
    /// Sync <target> with the folder that `share` printed <hash> for, until Ctrl-C.
    Mount { hash: String, target: PathBuf },
    /// Copy the knowledge-share and knowledge-mount skills into each agent's skills folder.
    Plug {
        /// Agent to plug into. Repeat for more than one. Default: every agent.
        #[arg(long = "agent", value_enum)]
        agents: Vec<Agent>,
    },
}

fn run_plug(agents: Vec<Agent>) -> anyhow::Result<()> {
    let agents = if agents.is_empty() { Agent::ALL.to_vec() } else { agents };
    for agent in agents {
        let root = plug::skills_dir(agent, |key| std::env::var(key).ok())?;
        for path in plug::install(&root)? {
            println!("{}", path.display());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "error".into()),
        )
        .init();
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match Cli::parse().command {
        Command::Share { folder } => {
            let print_hash = |hash: &str| println!("{hash}");
            agent_knowledge::sync::run(folder, None, print_hash, shutdown).await
        }
        Command::Mount { hash, target } => {
            std::fs::create_dir_all(&target)?;
            let announce = |_: &str| eprintln!("mounted at {}", target.display());
            agent_knowledge::sync::run(target.clone(), Some(hash), announce, shutdown).await
        }
        Command::Plug { agents } => run_plug(agents),
    }
}
