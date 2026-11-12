use std::path::PathBuf;

use agent_knowledge::memory::{self, Claim, Kind, Memory};

fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let pairs: Vec<(String, String)> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    move |key| pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

fn live(kind: Kind, folder: &str) -> Memory {
    Memory { pid: std::process::id(), kind, folder: PathBuf::from(folder) }
}

/// A pid that just exited, so no process holds it.
fn dead_pid() -> u32 {
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

#[test]
fn session_id_comes_from_the_agent_env() {
    let all =
        [("CLAUDE_CODE_SESSION_ID", "claude-1"), ("CODEX_THREAD_ID", "codex-1"), ("AGENT_KNOWLEDGE_SESSION", "own-1")];

    assert_eq!(memory::session_id(env_from(&all)).unwrap(), "claude-1");
    assert_eq!(memory::session_id(env_from(&all[1..])).unwrap(), "codex-1");
    assert_eq!(memory::session_id(env_from(&all[2..])).unwrap(), "own-1");
    assert_eq!(memory::session_id(env_from(&[])).unwrap(), "default");
}

#[test]
fn session_id_rejects_unsafe_values() {
    let err = memory::session_id(env_from(&[("AGENT_KNOWLEDGE_SESSION", "../etc")])).unwrap_err();

    assert!(err.to_string().contains("session id"), "{err}");
}

#[test]
fn registry_dir_defaults_under_home_and_honors_override() {
    assert_eq!(
        memory::registry_dir(env_from(&[("HOME", "/h")])).unwrap(),
        PathBuf::from("/h/.agent-knowledge/sessions")
    );
    assert_eq!(
        memory::registry_dir(env_from(&[("HOME", "/h"), ("AGENT_KNOWLEDGE_HOME", "/a")])).unwrap(),
        PathBuf::from("/a/sessions")
    );
}

#[test]
fn claim_makes_the_memory_current_and_drop_releases_it() {
    let dir = tempfile::tempdir().unwrap();
    let memory = live(Kind::Mount, "/bank");

    let claim = Claim::acquire(dir.path(), "s1", memory.clone()).unwrap();
    assert_eq!(memory::current(dir.path(), "s1").unwrap(), Some(memory));

    drop(claim);
    assert_eq!(memory::current(dir.path(), "s1").unwrap(), None);
}

#[test]
fn second_live_memory_in_one_session_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _first = Claim::acquire(dir.path(), "s1", live(Kind::Share, "/first")).unwrap();

    let err = Claim::acquire(dir.path(), "s1", live(Kind::Mount, "/second")).unwrap_err();

    let msg = err.to_string();
    assert!(msg.contains("already") && msg.contains("/first"), "{msg}");
    assert_eq!(memory::current(dir.path(), "s1").unwrap().unwrap().folder, PathBuf::from("/first"));
}

#[test]
fn sessions_do_not_block_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let _first = Claim::acquire(dir.path(), "s1", live(Kind::Share, "/first")).unwrap();

    let second = Claim::acquire(dir.path(), "s2", live(Kind::Mount, "/second"));

    assert!(second.is_ok());
}

#[test]
fn memory_of_a_dead_process_is_not_current_and_can_be_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let stale = Memory { pid: dead_pid(), kind: Kind::Mount, folder: PathBuf::from("/old") };
    let old = Claim::acquire(dir.path(), "s1", stale).unwrap();
    assert_eq!(memory::current(dir.path(), "s1").unwrap(), None);

    let fresh = live(Kind::Mount, "/new");
    let _new = Claim::acquire(dir.path(), "s1", fresh.clone()).unwrap();
    drop(old);

    assert_eq!(memory::current(dir.path(), "s1").unwrap(), Some(fresh));
}
