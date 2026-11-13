//! `unmount`, and a plain `kill`, stop a running memory gracefully: the process
//! leaves the mesh, exits 0, removes its registry entry, and keeps the files.
//! Drives the real binary, because the stop is a signal sent to another process.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

const SESSION: &str = "unmount-test";

/// A command for the binary, isolated in its own registry and session.
fn agent_knowledge(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-knowledge"));
    // These outrank AGENT_KNOWLEDGE_SESSION, and the test may run inside an agent that sets them.
    command.env_remove("CLAUDE_CODE_SESSION_ID").env_remove("CODEX_THREAD_ID");
    command.env("AGENT_KNOWLEDGE_HOME", home).env("AGENT_KNOWLEDGE_SESSION", SESSION);
    command
}

fn entry(home: &Path) -> PathBuf {
    home.join("sessions").join(format!("{SESSION}.json"))
}

/// Start `share <folder>` and return once it has joined the mesh.
fn start_share(home: &Path, folder: &Path) -> Child {
    let mut share = agent_knowledge(home)
        .arg("share")
        .arg(folder)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start share");
    let mut lines = BufReader::new(share.stdout.take().unwrap()).lines();
    let joined = lines.by_ref().map_while(Result::ok).any(|line| line.starts_with("📚 joined as "));
    assert!(joined, "share exited before it joined the mesh");
    // Keep draining stdout, so a notice never blocks the process on a full pipe.
    std::thread::spawn(move || lines.for_each(drop));
    share
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn unmount_leaves_the_mesh_and_keeps_the_files() {
    let home = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    std::fs::write(folder.path().join("note.md"), "kept").unwrap();
    let mut share = start_share(home.path(), folder.path());
    assert!(entry(home.path()).exists(), "share wrote no registry entry");

    let unmount = agent_knowledge(home.path()).arg("unmount").output().unwrap();

    assert!(unmount.status.success(), "unmount failed: {}", String::from_utf8_lossy(&unmount.stderr));
    let folder_abs = std::path::absolute(folder.path()).unwrap();
    assert_eq!(stdout(&unmount), format!("📚 left the share at {}; files kept\n", folder_abs.display()));
    assert!(share.wait().unwrap().success(), "share did not exit 0");
    assert!(!entry(home.path()).exists(), "registry entry left behind");
    assert_eq!(std::fs::read_to_string(folder.path().join("note.md")).unwrap(), "kept");
}

#[test]
fn sigterm_also_leaves_cleanly() {
    let home = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    let mut share = start_share(home.path(), folder.path());

    let kill = Command::new("kill").args(["-TERM", &share.id().to_string()]).status().unwrap();

    assert!(kill.success());
    assert!(share.wait().unwrap().success(), "share did not exit 0 on SIGTERM");
    assert!(!entry(home.path()).exists(), "registry entry left behind");
}

#[test]
fn unmount_without_a_memory_fails_clearly() {
    let home = tempfile::tempdir().unwrap();

    let unmount = agent_knowledge(home.path()).arg("unmount").output().unwrap();

    assert!(!unmount.status.success());
    let stderr = String::from_utf8_lossy(&unmount.stderr);
    assert!(stderr.contains("no memory bank in this session"), "{stderr}");
}
