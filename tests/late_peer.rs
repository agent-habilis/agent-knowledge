mod common;

use std::time::Duration;

use common::{Peer, wait_for};

/// A mount starts with a stale copy and no history while the only other peer is away. The other peer comes back after
/// a few seconds with the newer version. The mount holds its copy until it has seen the mesh, so the stale copy does
/// not win.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_copy_does_not_win_when_the_other_peer_comes_back_later() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(share_dir.path().join("p.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("p.md"), Some("v1")).await;
    let history = mount_dir.path().join(".agent-knowledge-index.json");
    while !std::fs::read_to_string(&history).is_ok_and(|saved| saved.contains("p.md")) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    mount.stop().await;
    std::fs::write(share_dir.path().join("p.md"), "v2").unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    share.stop().await;
    std::fs::remove_file(&history).unwrap();

    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let share = Peer::start(share_dir.path(), Some(mesh), None, |_| {}).await;

    wait_for(&share_dir.path().join("p.md"), Some("v2")).await;
    wait_for(&mount_dir.path().join("p.md"), Some("v2")).await;

    mount.stop().await;
    share.stop().await;
}
