//! `plug`: copy the skills embedded in this binary into an agent's skills folder.

use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

/// One skill, embedded from `src/skills/<name>/SKILL.md` at build time.
pub struct Skill {
    pub name: &'static str,
    pub body: &'static str,
}

pub const SKILLS: &[Skill] = &[
    Skill { name: "knowledge-share", body: include_str!("skills/knowledge-share/SKILL.md") },
    Skill { name: "knowledge-mount", body: include_str!("skills/knowledge-mount/SKILL.md") },
    Skill { name: "knowledge-write", body: include_str!("skills/knowledge-write/SKILL.md") },
    Skill { name: "knowledge-read", body: include_str!("skills/knowledge-read/SKILL.md") },
    Skill { name: "knowledge-status", body: include_str!("skills/knowledge-status/SKILL.md") },
];

/// A coding agent that loads `SKILL.md` skills from a user-level folder.
/// The names match the `plug` of the sibling projects (agent-gossip, agent-graph).
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Agent {
    #[value(name = "claude-code", alias = "claude")]
    ClaudeCode,
    Codex,
    Pi,
}

impl Agent {
    pub const ALL: [Agent; 3] = [Agent::ClaudeCode, Agent::Codex, Agent::Pi];

    pub fn label(self) -> &'static str {
        match self {
            Agent::ClaudeCode => "claude-code",
            Agent::Codex => "codex",
            Agent::Pi => "pi",
        }
    }

    /// The env var that moves the agent's config folder, and the default under `$HOME`.
    fn config_dir(self) -> (&'static str, &'static str) {
        match self {
            Agent::ClaudeCode => ("CLAUDE_CONFIG_DIR", ".claude"),
            Agent::Codex => ("CODEX_HOME", ".codex"),
            Agent::Pi => ("PI_CODING_AGENT_DIR", ".pi/agent"),
        }
    }
}

/// The config folder of `agent`. `env` reads an environment variable,
/// so tests can resolve paths without touching the process environment.
fn agent_dir(agent: Agent, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    let (override_var, default_dir) = agent.config_dir();
    if let Some(dir) = env(override_var).filter(|d| !d.is_empty()) {
        return Ok(PathBuf::from(dir));
    }
    let Some(home) = env("HOME").filter(|h| !h.is_empty()) else {
        bail!("cannot find the {} skills folder: HOME is not set", agent.label());
    };
    Ok(PathBuf::from(home).join(default_dir))
}

/// The user-level skills folder of `agent`.
pub fn skills_dir(agent: Agent, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    Ok(agent_dir(agent, env)?.join("skills"))
}

/// Whether `agent` is on this machine: its config folder exists.
pub fn detected(agent: Agent, env: impl Fn(&str) -> Option<String>) -> bool {
    agent_dir(agent, env).is_ok_and(|dir| dir.is_dir())
}

/// The agents to plug into: the explicit ones without duplicates, else every detected agent.
pub fn selected(explicit: &[Agent], detected: impl Fn(Agent) -> bool) -> Vec<Agent> {
    if explicit.is_empty() {
        return Agent::ALL.into_iter().filter(|&agent| detected(agent)).collect();
    }
    let mut agents = Vec::new();
    for &agent in explicit {
        if !agents.contains(&agent) {
            agents.push(agent);
        }
    }
    agents
}

/// Write every embedded skill to `<root>/<name>/SKILL.md`, replacing older copies.
/// Returns the paths written, in `SKILLS` order.
pub fn install(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    SKILLS.iter().map(|skill| write_skill(root, skill)).collect()
}

fn write_skill(root: &Path, skill: &Skill) -> anyhow::Result<PathBuf> {
    let dir = root.join(skill.name);
    let path = dir.join("SKILL.md");
    let write = || -> io::Result<()> {
        std::fs::create_dir_all(&dir)?;
        std::fs::write(&path, skill.body)
    };
    write().with_context(|| format!("cannot write {}", path.display()))?;
    Ok(path)
}
