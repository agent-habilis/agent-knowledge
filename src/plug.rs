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
];

/// A coding agent that loads `SKILL.md` skills from a user-level folder.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Agent {
    Claude,
    Codex,
}

impl Agent {
    pub const ALL: [Agent; 2] = [Agent::Claude, Agent::Codex];

    /// The env var that moves the agent's config folder, and the default under `$HOME`.
    fn config_dir(self) -> (&'static str, &'static str) {
        match self {
            Agent::Claude => ("CLAUDE_CONFIG_DIR", ".claude"),
            Agent::Codex => ("CODEX_HOME", ".codex"),
        }
    }
}

/// The user-level skills folder of `agent`. `env` reads an environment variable,
/// so tests can resolve paths without touching the process environment.
pub fn skills_dir(agent: Agent, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    let (override_var, default_dir) = agent.config_dir();
    if let Some(dir) = env(override_var).filter(|d| !d.is_empty()) {
        return Ok(PathBuf::from(dir).join("skills"));
    }
    let Some(home) = env("HOME").filter(|h| !h.is_empty()) else {
        bail!("cannot find the {agent:?} skills folder: HOME is not set");
    };
    Ok(PathBuf::from(home).join(default_dir).join("skills"))
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
