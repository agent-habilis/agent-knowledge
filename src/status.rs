//! The live state of a running share or mount, for `agent-knowledge status`.
//!
//! The running process writes it to `<registry>/<session>.status.json` (see
//! [`crate::memory::status_path`]) after each sync step and peer event. `status` reads the file
//! and prints it as one `📚` block. Times are Unix seconds, so a reader can age them.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};

/// How many recent changes the block lists.
pub const RECENT_CHANGES: usize = 5;

/// The state of one running memory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub mesh: String,
    pub nick: String,
    pub folder: PathBuf,
    pub peers: Vec<Peer>,
    pub files: usize,
    pub bytes: u64,
    /// When the shared file list was last applied to the folder.
    pub synced_at: Option<u64>,
    /// Uploads and downloads that failed since the process started.
    pub failed: u64,
    /// The newest change last, at most [`RECENT_CHANGES`].
    pub changes: VecDeque<Change>,
    /// When this was written. Peer ages are as of this time.
    pub written_at: u64,
}

/// Another member of the mesh, as this peer last saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub nick: String,
    /// The lane a message to it takes: `unicast`, `multihop`, `relay-only`, and so on.
    pub path: String,
    pub last_seen_secs: Option<u64>,
    /// Silent past the mesh's alive timeout.
    pub quiet: bool,
}

/// One file that synced: `↑` sent, `↓` received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub arrow: String,
    pub path: String,
    pub deleted: bool,
    pub at: u64,
}

/// The peers in the mesh's roster JSON (`Inspect::Peers`): `{"peers": [{"nickname",
/// "last_seen_secs_ago", "quiet", "transport"}], "count"}`. This peer is not in it.
pub fn peers_from_roster(roster: &Value) -> Vec<Peer> {
    let rows = roster["peers"].as_array().map(Vec::as_slice).unwrap_or_default();
    rows.iter()
        .map(|row| Peer {
            nick: row["nickname"].as_str().unwrap_or("?").to_owned(),
            path: row["transport"].as_str().unwrap_or("?").to_owned(),
            last_seen_secs: row["last_seen_secs_ago"].as_u64(),
            quiet: row["quiet"].as_bool().unwrap_or(false),
        })
        .collect()
}

/// Now, in Unix seconds.
pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_secs())
}

impl Status {
    /// Record a change, dropping the oldest past [`RECENT_CHANGES`].
    pub fn record(&mut self, arrow: &str, path: &str, deleted: bool) {
        let change = Change { arrow: arrow.to_owned(), path: path.to_owned(), deleted, at: now() };
        self.changes.push_back(change);
        while self.changes.len() > RECENT_CHANGES {
            self.changes.pop_front();
        }
    }

    /// Write to `path` through a temp file and a rename, so a reader never sees half of it.
    pub fn write(&self, path: &Path) -> Result<()> {
        let temp = path.with_extension("json.tmp");
        std::fs::write(&temp, to_json(self).to_string()).with_context(|| format!("cannot write {}", temp.display()))?;
        std::fs::rename(&temp, path).with_context(|| format!("cannot write {}", path.display()))
    }

    /// Read `path`, or `None` when there is no such file.
    pub fn read(path: &Path) -> Result<Option<Status>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("cannot read {}", path.display())),
        };
        let value: Value =
            serde_json::from_str(&text).with_context(|| format!("bad status file {}", path.display()))?;
        Ok(Some(from_json(&value)))
    }
}

fn to_json(status: &Status) -> Value {
    let peers: Vec<Value> = status
        .peers
        .iter()
        .map(|peer| json!({ "nick": peer.nick, "path": peer.path, "last_seen_secs": peer.last_seen_secs, "quiet": peer.quiet }))
        .collect();
    let changes: Vec<Value> = status
        .changes
        .iter()
        .map(|change| json!({ "arrow": change.arrow, "path": change.path, "deleted": change.deleted, "at": change.at }))
        .collect();
    json!({
        "mesh": status.mesh,
        "nick": status.nick,
        "folder": status.folder.to_string_lossy(),
        "peers": peers,
        "files": status.files,
        "bytes": status.bytes,
        "synced_at": status.synced_at,
        "failed": status.failed,
        "changes": changes,
        "written_at": status.written_at,
    })
}

fn from_json(value: &Value) -> Status {
    let text = |value: &Value| value.as_str().unwrap_or_default().to_owned();
    let list = |key: &str| value[key].as_array().cloned().unwrap_or_default();
    let peers = list("peers")
        .iter()
        .map(|peer| Peer {
            nick: text(&peer["nick"]),
            path: text(&peer["path"]),
            last_seen_secs: peer["last_seen_secs"].as_u64(),
            quiet: peer["quiet"].as_bool().unwrap_or(false),
        })
        .collect();
    let changes = list("changes")
        .iter()
        .map(|change| Change {
            arrow: text(&change["arrow"]),
            path: text(&change["path"]),
            deleted: change["deleted"].as_bool().unwrap_or(false),
            at: change["at"].as_u64().unwrap_or(0),
        })
        .collect();
    Status {
        mesh: text(&value["mesh"]),
        nick: text(&value["nick"]),
        folder: PathBuf::from(text(&value["folder"])),
        peers,
        files: value["files"].as_u64().unwrap_or(0) as usize,
        bytes: value["bytes"].as_u64().unwrap_or(0),
        synced_at: value["synced_at"].as_u64(),
        failed: value["failed"].as_u64().unwrap_or(0),
        changes,
        written_at: value["written_at"].as_u64().unwrap_or(0),
    }
}

/// The `📚` block for a memory of `kind` (`share` or `mount`), with ages as of `now`.
pub fn render(status: &Status, kind: &str, now: u64) -> String {
    format!("{}\n\n{}", render_mesh(status, kind, now), render_docs(status, now))
}

/// The header leads with what a user asks first: how many others are connected, the hash to give
/// a peer, and the local folder. `connected` counts the other peers, not this one.
fn render_mesh(status: &Status, kind: &str, now: u64) -> String {
    let since_write = now.saturating_sub(status.written_at);
    let mut out = format!(
        "📚 {kind} · {} connected · hash `{}` · path `{}` · you {}\n\n",
        status.peers.len(),
        status.mesh,
        status.folder.display(),
        status.nick
    );
    out.push_str("| peer | path | last seen |\n| ---- | ---- | --------- |\n");
    out.push_str(&format!("| {} (you) | — | — |\n", status.nick));
    for peer in &status.peers {
        let seen = peer.last_seen_secs.map_or("—".to_owned(), |secs| ago(secs + since_write));
        let seen = if peer.quiet { format!("quiet · {seen}") } else { seen };
        out.push_str(&format!("| {} | {} | {seen} |\n", peer.nick, peer.path));
    }
    out.trim_end().to_owned()
}

fn render_docs(status: &Status, now: u64) -> String {
    let synced = status.synced_at.map_or("never".to_owned(), |at| format!("synced {}", ago(now.saturating_sub(at))));
    let mut out = format!(
        "📚 docs · {} · {} · {synced} · {} failed",
        plural(status.files, "file"),
        size(status.bytes),
        status.failed
    );
    if status.changes.is_empty() {
        return out;
    }
    out.push_str("\n\n| change | file | when |\n| ------ | ---- | ---- |\n");
    for change in status.changes.iter().rev() {
        let arrow = if change.deleted { format!("{} deleted", change.arrow) } else { change.arrow.clone() };
        out.push_str(&format!("| {arrow} | {} | {} |\n", change.path, ago(now.saturating_sub(change.at))));
    }
    out.trim_end().to_owned()
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 { format!("1 {noun}") } else { format!("{count} {noun}s") }
}

/// A time span in its largest whole unit: `95s ago`, `3m ago`, `2h ago`, `5d ago`. Seconds last
/// up to 2 minutes, where `1m` would hide most of the span.
fn ago(secs: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    match secs {
        s if s < 2 * MINUTE => format!("{s}s ago"),
        s if s < HOUR => format!("{}m ago", s / MINUTE),
        s if s < DAY => format!("{}h ago", s / HOUR),
        s => format!("{}d ago", s / DAY),
    }
}

/// A byte count in decimal units with one decimal: `512 B`, `1.3 MB`.
fn size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    const STEP: f64 = 1000.0;
    if bytes < 1000 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / STEP;
    let mut unit = 0;
    while value >= STEP && unit < UNITS.len() - 1 {
        value /= STEP;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Status {
        let mut status = Status {
            mesh: "HASH".to_owned(),
            nick: "alice".to_owned(),
            folder: PathBuf::from("/abs/folder"),
            peers: vec![
                Peer { nick: "bob".to_owned(), path: "unicast".to_owned(), last_seen_secs: Some(2), quiet: false },
                Peer { nick: "carol".to_owned(), path: "relay-only".to_owned(), last_seen_secs: Some(95), quiet: true },
            ],
            files: 42,
            bytes: 1_300_000,
            synced_at: Some(996),
            failed: 0,
            changes: VecDeque::new(),
            written_at: 1000,
        };
        status
            .changes
            .push_back(Change { arrow: "↑".to_owned(), path: "log.md".to_owned(), deleted: false, at: 980 });
        status.changes.push_back(Change {
            arrow: "↓".to_owned(),
            path: "concepts/peer.md".to_owned(),
            deleted: false,
            at: 996,
        });
        status
    }

    #[test]
    fn renders_the_mesh_and_the_docs_newest_change_first() {
        let block = render(&sample(), "share", 1000);

        assert_eq!(
            block,
            "📚 share · 2 connected · hash `HASH` · path `/abs/folder` · you alice\n\n\
             | peer | path | last seen |\n| ---- | ---- | --------- |\n\
             | alice (you) | — | — |\n| bob | unicast | 2s ago |\n| carol | relay-only | quiet · 95s ago |\n\n\
             📚 docs · 42 files · 1.3 MB · synced 4s ago · 0 failed\n\n\
             | change | file | when |\n| ------ | ---- | ---- |\n\
             | ↓ | concepts/peer.md | 4s ago |\n| ↑ | log.md | 20s ago |"
        );
    }

    #[test]
    fn ages_peers_by_the_time_since_the_file_was_written() {
        let block = render(&sample(), "mount", 1180);

        assert!(block.contains("| bob | unicast | 3m ago |"), "{block}");
    }

    #[test]
    fn an_alone_peer_with_nothing_synced_says_so() {
        let status =
            Status { mesh: "H".to_owned(), nick: "a".to_owned(), folder: PathBuf::from("/f"), ..Status::default() };

        let block = render(&status, "share", 5);

        assert!(block.starts_with("📚 share · 0 connected · hash `H` · path `/f` · you a"), "{block}");
        assert!(block.ends_with("📚 docs · 0 files · 0 B · never · 0 failed"), "{block}");
    }

    #[test]
    fn record_keeps_only_the_newest_changes() {
        let mut status = Status::default();
        for index in 0..RECENT_CHANGES + 2 {
            status.record("↑", &format!("{index}.md"), false);
        }

        assert_eq!(status.changes.len(), RECENT_CHANGES);
        assert_eq!(status.changes.front().unwrap().path, "2.md");
    }

    #[test]
    fn a_written_status_reads_back_the_same() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.status.json");

        sample().write(&path).unwrap();

        assert_eq!(Status::read(&path).unwrap(), Some(sample()));
        assert_eq!(Status::read(&dir.path().join("missing.json")).unwrap(), None);
    }

    #[test]
    fn reads_peers_from_the_mesh_roster() {
        let roster = json!({ "count": 3, "peers": [
            { "key": "k1", "nickname": "bob", "last_seen_secs_ago": 2, "quiet": false, "reach": "direct", "transport": "unicast" },
            { "key": "k2", "nickname": "carol", "last_seen_secs_ago": null, "quiet": true, "reach": "gossip", "transport": "relay-only" },
        ] });

        let peers = peers_from_roster(&roster);

        assert_eq!(
            peers,
            [
                Peer { nick: "bob".to_owned(), path: "unicast".to_owned(), last_seen_secs: Some(2), quiet: false },
                Peer { nick: "carol".to_owned(), path: "relay-only".to_owned(), last_seen_secs: None, quiet: true },
            ]
        );
        assert!(peers_from_roster(&json!({})).is_empty());
    }

    #[test]
    fn sizes_and_ages_use_their_largest_unit() {
        assert_eq!(size(999), "999 B");
        assert_eq!(size(1_300_000), "1.3 MB");
        assert_eq!(ago(95), "95s ago");
        assert_eq!(ago(120), "2m ago");
        assert_eq!(ago(3 * 3600), "3h ago");
    }
}
