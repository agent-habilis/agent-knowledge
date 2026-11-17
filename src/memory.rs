//! One memory per agent session: a registry of the share or mount each session runs.
//!
//! An entry is `<registry>/<session>.json` holding `{pid, kind, folder}`. The
//! entry counts only while its process is alive, so a memory killed without a
//! clean shutdown never blocks the session.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, bail};
use serde_json::{Value, json};

/// Env vars that name the agent session, most specific first.
const SESSION_VARS: [&str; 3] = ["CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "AGENT_KNOWLEDGE_SESSION"];
/// The session of an agent that exposes no session id, and of a plain terminal.
const DEFAULT_SESSION: &str = "default";
const MAX_SESSION_ID_LEN: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Share,
    Mount,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Share => "share",
            Kind::Mount => "mount",
        }
    }

    fn parse(text: &str) -> Option<Kind> {
        match text {
            "share" => Some(Kind::Share),
            "mount" => Some(Kind::Mount),
            _ => None,
        }
    }
}

/// A running share or mount: the memory bank of one session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Memory {
    pub pid: u32,
    pub kind: Kind,
    pub folder: PathBuf,
}

/// The current agent session. The id becomes a file name, so it is checked.
pub fn session_id(env: impl Fn(&str) -> Option<String>) -> anyhow::Result<String> {
    let id = SESSION_VARS
        .iter()
        .find_map(|var| env(var).filter(|value| !value.is_empty()))
        .unwrap_or_else(|| DEFAULT_SESSION.to_owned());
    let safe = id.len() <= MAX_SESSION_ID_LEN && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !safe {
        bail!("unsafe session id {id:?}: use letters, digits, '-' and '_' only");
    }
    Ok(id)
}

/// The folder that holds one entry per session.
pub fn registry_dir(env: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    if let Some(home) = env("AGENT_KNOWLEDGE_HOME").filter(|dir| !dir.is_empty()) {
        return Ok(PathBuf::from(home).join("sessions"));
    }
    let Some(home) = env("HOME").filter(|dir| !dir.is_empty()) else {
        bail!("cannot find the session registry: HOME is not set");
    };
    Ok(PathBuf::from(home).join(".agent-knowledge").join("sessions"))
}

/// The memory that `session` runs, if its process is still alive.
pub fn current(dir: &Path, session: &str) -> anyhow::Result<Option<Memory>> {
    Ok(read_entry(&entry_path(dir, session))?.filter(|memory| is_alive(memory.pid)))
}

/// The status file of `session`'s memory, beside its entry: the running process writes it, and
/// `agent-knowledge status` reads it.
pub fn status_path(dir: &Path, session: &str) -> PathBuf {
    dir.join(format!("{session}.status.json"))
}

/// The registry entry of a running memory. Dropping it removes the entry and its status file.
#[derive(Debug)]
pub struct Claim {
    path: PathBuf,
    status: PathBuf,
    pid: u32,
}

impl Claim {
    /// Record `memory` as the memory of `session`. Fails if the session
    /// already runs a live memory.
    pub fn acquire(dir: &Path, session: &str, memory: Memory) -> anyhow::Result<Claim> {
        if let Some(running) = current(dir, session)? {
            bail!(
                "this session already runs a {} memory at {} (pid {}); stop it before starting another",
                running.kind.as_str(),
                running.folder.display(),
                running.pid
            );
        }
        let path = entry_path(dir, session);
        let body = json!({
            "pid": memory.pid,
            "kind": memory.kind.as_str(),
            "folder": memory.folder.to_string_lossy(),
        });
        let write = || -> io::Result<()> {
            std::fs::create_dir_all(dir)?;
            std::fs::write(&path, body.to_string())
        };
        write().with_context(|| format!("cannot write {}", path.display()))?;
        Ok(Claim { path, status: status_path(dir, session), pid: memory.pid })
    }

    /// Where this memory writes its status.
    pub fn status_path(&self) -> &Path {
        &self.status
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        // A newer memory may have replaced this entry after this process died
        // and came back, so remove the entry only while it is still ours.
        let ours = matches!(read_entry(&self.path), Ok(Some(memory)) if memory.pid == self.pid);
        if !ours {
            return;
        }
        for path in [&self.status, &self.path] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => eprintln!("warning: cannot remove {}: {err}", path.display()),
            }
        }
    }
}

fn entry_path(dir: &Path, session: &str) -> PathBuf {
    dir.join(format!("{session}.json"))
}

fn read_entry(path: &Path) -> anyhow::Result<Option<Memory>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("cannot read {}", path.display())),
    };
    parse_entry(&text).with_context(|| format!("bad session entry {}; delete it to continue", path.display())).map(Some)
}

fn parse_entry(text: &str) -> anyhow::Result<Memory> {
    let value: Value = serde_json::from_str(text)?;
    let pid = value["pid"].as_u64().and_then(|pid| u32::try_from(pid).ok()).context("no pid")?;
    let kind = value["kind"].as_str().and_then(Kind::parse).context("no kind")?;
    let folder = value["folder"].as_str().context("no folder")?;
    Ok(Memory { pid, kind, folder: PathBuf::from(folder) })
}

/// Whether a process with `pid` exists. `kill -0` sends no signal; it only checks.
fn is_alive(pid: u32) -> bool {
    // pid 0 would address the whole process group, not one process.
    pid != 0
        && Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
}
