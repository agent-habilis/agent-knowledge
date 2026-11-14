//! `agent-knowledge research` refuses to run without a live share or mount in the session.

use std::path::Path;
use std::process::{Command, Output};

const NOT_RUNNING: &str = "no agent-knowledge share or mount is running";

/// Run `research` as session `session`, with `home` as the registry home and `key` as the API key.
fn research(home: &Path, session: &str, key: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-knowledge"));
    command
        .args(["research", "rust async"])
        .env_remove("CLAUDE_CODE_SESSION_ID")
        .env_remove("CODEX_THREAD_ID")
        .env_remove("AGENT_KNOWLEDGE_YOUCOM_KEY")
        .env("AGENT_KNOWLEDGE_SESSION", session)
        .env("AGENT_KNOWLEDGE_HOME", home);
    if let Some(key) = key {
        command.env("AGENT_KNOWLEDGE_YOUCOM_KEY", key);
    }
    command.output().unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn fails_when_no_share_or_mount_runs() {
    let home = tempfile::tempdir().unwrap();

    let output = research(home.path(), "no-process", Some("x"));

    assert!(!output.status.success());
    assert!(stderr(&output).contains(NOT_RUNNING), "{}", stderr(&output));
}

#[test]
fn fails_when_the_recorded_share_has_exited() {
    let home = tempfile::tempdir().unwrap();
    let bank = tempfile::tempdir().unwrap();
    let mut child = Command::new("true").spawn().unwrap();
    let dead = child.id();
    child.wait().unwrap();
    let sessions = home.path().join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let entry = serde_json::json!({ "pid": dead, "kind": "share", "folder": bank.path() });
    std::fs::write(sessions.join("dead-share.json"), entry.to_string()).unwrap();

    let output = research(home.path(), "dead-share", Some("x"));

    assert!(!output.status.success());
    assert!(stderr(&output).contains(NOT_RUNNING), "{}", stderr(&output));
    assert!(!bank.path().join("research").exists());
}

#[test]
fn the_missing_key_is_reported_first() {
    let home = tempfile::tempdir().unwrap();

    let output = research(home.path(), "no-key", None);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("AGENT_KNOWLEDGE_YOUCOM_KEY is not set"), "{}", stderr(&output));
}
