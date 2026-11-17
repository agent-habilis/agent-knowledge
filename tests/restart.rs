mod common;

use common::{Peer, wait_for};
use std::time::Duration;

/// A file that the mount deletes while it is stopped stays deleted when it restarts. The share still lists the file,
/// so without the mount's tombstone the share would keep it, and the mount would download it back.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_a_delete_made_while_stopped() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "from share").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("from share")).await;

    mount.stop().await;
    std::fs::remove_file(mount_dir.path().join("a.md")).unwrap();
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;

    wait_for(&share_dir.path().join("a.md"), None).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(!mount_dir.path().join("a.md").exists(), "the deleted file came back on the mount");

    mount.stop().await;
    share.stop().await;
}
