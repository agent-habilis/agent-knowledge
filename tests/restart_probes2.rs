mod common;

use common::{Peer, wait_for};
use std::time::Duration;

/// P6. After the share restarts, a new mount still gets the files. The saved entries must not name the old run's tickets.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_mount_gets_a_file_after_the_share_restarts() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    tokio::time::sleep(Duration::from_secs(4)).await;
    share.stop().await;
    let share = Peer::start(share_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    let got = tokio::time::timeout(Duration::from_secs(40), wait_for(&mount_dir.path().join("a.md"), Some("v1"))).await;
    mount.stop().await;
    share.stop().await;
    assert!(got.is_ok(), "the new mount never got a.md after the share restarted");
}

/// P6b. A mount that restarts after the share left still serves its files to a late mount.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_mount_still_serves_its_files() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let late_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v1")).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    share.stop().await;
    mount.stop().await;
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let late = Peer::start(late_dir.path(), Some(mesh), None, |_| {}).await;
    wait_for(&late_dir.path().join("a.md"), Some("v1")).await;
    late.stop().await;
    mount.stop().await;
}

/// P7. After a restart, the own entry names a ticket of the new run, not of the old run.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_publishes_a_ticket_of_the_new_run() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), "v1").unwrap();
    let ticket = |dir: &std::path::Path| {
        let text = std::fs::read_to_string(dir.join(".agent-knowledge-index.json")).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let own = value.get("own").unwrap_or(&value);
        own["a.md"]["ticket"].as_str().unwrap().to_owned()
    };
    let share = Peer::start(dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    tokio::time::sleep(Duration::from_secs(4)).await;
    share.stop().await;
    let first = ticket(dir.path());
    let share = Peer::start(dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    share.stop().await;
    let second = ticket(dir.path());
    assert_ne!(first, second, "the restart kept the old run's ticket");
}
