mod common;

use std::time::Duration;

use common::{Peer, wait_for};

const SETTLE: Duration = Duration::from_secs(7);

/// A peer that restarts holds a file that another peer deleted while it was away. The restart
/// knows nothing of what it had synced, so the delete must still win: the file goes, and it does
/// not come back to the peer that deleted it.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_peer_drops_a_file_that_was_deleted_while_it_was_away() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let (share_path, mount_path) = (share_dir.path(), mount_dir.path());
    std::fs::write(share_path.join("gone.md"), "soon gone").unwrap();
    std::fs::write(share_path.join("stays.md"), "stays").unwrap();

    let share = Peer::start(share_path, None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_path, Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_path.join("gone.md"), Some("soon gone")).await;
    wait_for(&mount_path.join("stays.md"), Some("stays")).await;

    mount.stop().await;
    std::fs::remove_file(share_path.join("gone.md")).unwrap();
    tokio::time::sleep(SETTLE).await;

    let mount = Peer::start(mount_path, Some(mesh), None, |_| {}).await;
    wait_for(&mount_path.join("gone.md"), None).await;
    tokio::time::sleep(SETTLE).await;
    wait_for(&share_path.join("gone.md"), None).await;
    wait_for(&mount_path.join("stays.md"), Some("stays")).await;

    mount.stop().await;
    share.stop().await;
}
