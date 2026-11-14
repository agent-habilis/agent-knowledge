mod common;

use common::{Peer, wait_for};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// P8. A peer whose clock ran 1 h ahead saved an own edit. Its clock is now correct. After a restart, the user's
/// edit must not be overwritten on its own disk by an older version from another peer.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_keeps_its_own_edit_after_its_clock_is_set_back() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "old").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("old")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    mount.stop().await;

    // The state the mount saved when its clock was 1 h ahead and the user wrote "mine".
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    std::fs::write(mount_dir.path().join("a.md"), "mine").unwrap();
    let sha = agent_knowledge::scan::sha256_bytes(b"mine");
    let saved =
        serde_json::json!({ "mesh": mesh, "own": { "a.md": { "sha256": sha, "ticket": "x", "ts": now + 3_600_000 } } });
    std::fs::write(mount_dir.path().join(".agent-knowledge-index.json"), saved.to_string()).unwrap();

    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let mount_copy = std::fs::read_to_string(mount_dir.path().join("a.md")).ok();
    let share_copy = std::fs::read_to_string(share_dir.path().join("a.md")).ok();
    mount.stop().await;
    share.stop().await;
    eprintln!("mount: {mount_copy:?}, share: {share_copy:?}");
    assert_eq!(mount_copy.as_deref(), Some("mine"), "the user's own edit was overwritten by an older version");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_own_newer_saved_edit_is_not_overwritten_at_restart() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "old").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("old")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    mount.stop().await;

    // The state the mount saved when its clock was 1 h ahead and the user wrote "mine".
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    std::fs::write(mount_dir.path().join("a.md"), "mine").unwrap();
    let sha = agent_knowledge::scan::sha256_bytes(b"mine");
    let saved = serde_json::json!({ "mesh": mesh, "own": { "a.md": { "sha256": sha, "ticket": "x", "ts": now } } });
    std::fs::write(mount_dir.path().join(".agent-knowledge-index.json"), saved.to_string()).unwrap();

    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let mount_copy = std::fs::read_to_string(mount_dir.path().join("a.md")).ok();
    let share_copy = std::fs::read_to_string(share_dir.path().join("a.md")).ok();
    mount.stop().await;
    share.stop().await;
    eprintln!("mount: {mount_copy:?}, share: {share_copy:?}");
    assert_eq!(mount_copy.as_deref(), Some("mine"), "the user's own edit was overwritten by an older version");
}
