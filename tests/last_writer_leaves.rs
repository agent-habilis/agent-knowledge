mod common;

use std::time::Duration;

use common::{Peer, wait_for};

const SETTLE: Duration = Duration::from_secs(7);

/// A writes `p.md`, B edits it, B leaves. The edit must stay: neither A nor a peer that joins
/// later may go back to the content that A wrote first.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_writer_leaves_and_the_file_keeps_its_new_content() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let late_dir = tempfile::tempdir().unwrap();
    let (share_path, mount_path, late_path) = (share_dir.path(), mount_dir.path(), late_dir.path());
    std::fs::write(share_path.join("p.md"), "v1").unwrap();

    let share = Peer::start(share_path, None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_path, Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_path.join("p.md"), Some("v1")).await;

    std::fs::write(mount_path.join("p.md"), "v2").unwrap();
    wait_for(&share_path.join("p.md"), Some("v2")).await;

    mount.stop().await;
    tokio::time::sleep(SETTLE).await;
    wait_for(&share_path.join("p.md"), Some("v2")).await;

    // The blob of the departed peer is gone: the new content must come from the share.
    let late = Peer::start(late_path, Some(mesh), None, |_| {}).await;
    wait_for(&late_path.join("p.md"), Some("v2")).await;

    late.stop().await;
    share.stop().await;
}
