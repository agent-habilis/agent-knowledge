mod common;

use std::path::Path;
use std::time::Duration;

use common::{Peer, wait_for};

const SETTLE: Duration = Duration::from_secs(7);

/// A peer stops with `p.md` = v1 on disk, another edits it to v2, and the first starts again.
/// Its old copy is not an edit: v2 must win on both sides.
async fn restart_with_a_stale_copy(forget_history: impl FnOnce(&Path)) {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let (share_path, mount_path) = (share_dir.path(), mount_dir.path());
    std::fs::write(share_path.join("p.md"), "v1").unwrap();

    let share = Peer::start(share_path, None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_path, Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_path.join("p.md"), Some("v1")).await;

    mount.stop().await;
    std::fs::write(share_path.join("p.md"), "v2").unwrap();
    tokio::time::sleep(SETTLE).await;
    forget_history(mount_path);

    let mount = Peer::start(mount_path, Some(mesh), None, |_| {}).await;
    wait_for(&mount_path.join("p.md"), Some("v2")).await;
    tokio::time::sleep(SETTLE).await;
    wait_for(&share_path.join("p.md"), Some("v2")).await;
    wait_for(&mount_path.join("p.md"), Some("v2")).await;

    mount.stop().await;
    share.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_peer_with_a_stale_copy_takes_the_newer_version() {
    restart_with_a_stale_copy(|_| {}).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_copy_with_no_history_does_not_beat_the_mesh() {
    restart_with_a_stale_copy(|root| {
        let _ = std::fs::remove_file(root.join(".agent-knowledge-known.json"));
    })
    .await;
}
