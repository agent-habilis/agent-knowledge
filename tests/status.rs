mod common;

use std::path::Path;
use std::time::Duration;

use agent_knowledge::status::Status;
use agent_knowledge::sync;
use common::{Peer, WAIT, options, wait_for};

/// Poll the status file at `path` until `done` holds for it; fail after [`WAIT`].
async fn wait_status(path: &Path, what: &str, done: impl Fn(&Status) -> bool) -> Status {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if let Ok(Some(status)) = Status::read(path)
            && done(&status)
        {
            return status;
        }
        assert!(tokio::time::Instant::now() < deadline, "{} never showed {what}", path.display());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Each side's status file names the other peer, counts the synced file, and lists the change.
#[tokio::test(flavor = "multi_thread")]
async fn each_side_sees_the_other_peer_and_the_synced_file() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let share_status = registry.path().join("share.status.json");
    let mount_status = registry.path().join("mount.status.json");
    std::fs::write(share_dir.path().join("note.md"), "hello").unwrap();

    let share_options = sync::Options { status_file: Some(share_status.clone()), ..options(share_dir.path(), None, Some("alice")) };
    let share = Peer::start_with(share_options, |_| {}).await;
    let mount_options =
        sync::Options { status_file: Some(mount_status.clone()), ..options(mount_dir.path(), Some(share.ready.mesh.clone()), Some("bob")) };
    let mount = Peer::start_with(mount_options, |_| {}).await;
    wait_for(&mount_dir.path().join("note.md"), Some("hello")).await;

    let seen = wait_status(&share_status, "bob", |status| status.peers.iter().any(|peer| peer.nick == "bob")).await;
    assert_eq!((seen.nick.as_str(), seen.files), ("alice", 1));
    assert!(seen.changes.iter().any(|change| change.arrow == "↑" && change.path == "note.md"));

    let got = wait_status(&mount_status, "alice", |status| status.peers.iter().any(|peer| peer.nick == "alice")).await;
    assert_eq!((got.nick.as_str(), got.mesh.as_str(), got.files, got.bytes), ("bob", share.ready.mesh.as_str(), 1, 5));
    assert!(got.changes.iter().any(|change| change.arrow == "↓" && change.path == "note.md"));
    assert!(got.synced_at.is_some());

    mount.stop().await;
    share.stop().await;
}
