mod common;

use std::time::Duration;

use agent_knowledge::sync;
use common::{Peer, WAIT, options, wait_for};

/// The end of a download and the fetch of an index are both slow, so the mount reads the author's new pointer while
/// its download of the old version is still in flight.
const SLOW: sync::Delays = sync::Delays { finished: Duration::from_secs(3), index: Duration::from_secs(4) };

/// A share and a mount that has fetched `p.md` as v1, with its download not yet reported.
async fn mount_with_a_late_download(share_dir: &std::path::Path, mount_dir: &std::path::Path) -> (Peer, Peer) {
    std::fs::write(share_dir.join("p.md"), "v1").unwrap();
    let share = Peer::start(share_dir, None, None, |_| {}).await;
    let slow = sync::Options { delays: SLOW, ..options(mount_dir, Some(share.ready.mesh.clone()), None) };
    let mount = Peer::start_with(slow, |_| {}).await;
    wait_for(&mount_dir.join("p.md"), Some("v1")).await;
    (share, mount)
}

/// The mount serves v1 with the ts of the entry it came from, so the author's newer v2 still wins.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_download_of_the_old_version_does_not_beat_the_newer_edit() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (share, mount) = mount_with_a_late_download(share_dir.path(), mount_dir.path()).await;

    std::fs::write(share_dir.path().join("p.md"), "v2").unwrap();
    wait_for(&mount_dir.path().join("p.md"), Some("v2")).await;
    tokio::time::sleep(WAIT / 3).await;
    wait_for(&share_dir.path().join("p.md"), Some("v2")).await;
    wait_for(&mount_dir.path().join("p.md"), Some("v2")).await;

    mount.stop().await;
    share.stop().await;
}

/// The same for a delete: the author's tombstone is newer than the copy that the mount is still serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_download_of_the_old_version_does_not_beat_a_newer_delete() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (share, mount) = mount_with_a_late_download(share_dir.path(), mount_dir.path()).await;

    std::fs::remove_file(share_dir.path().join("p.md")).unwrap();
    wait_for(&mount_dir.path().join("p.md"), None).await;
    tokio::time::sleep(WAIT / 3).await;
    wait_for(&share_dir.path().join("p.md"), None).await;
    wait_for(&mount_dir.path().join("p.md"), None).await;

    mount.stop().await;
    share.stop().await;
}
