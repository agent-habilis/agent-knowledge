mod common;

use common::{Peer, wait_for};

/// A mount edits a file while it is stopped. The edit has a saved entry to differ from, so it is an edit and not a stale
/// copy: it must reach the share, and the share's old version must not replace it.
#[tokio::test(flavor = "multi_thread")]
async fn an_edit_made_while_a_mount_was_stopped_reaches_the_share() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(share_dir.path().join("p.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("p.md"), Some("v1")).await;
    // The mount saves its history once its download is published.
    let history = mount_dir.path().join(".agent-knowledge-index.json");
    while !std::fs::read_to_string(&history).is_ok_and(|saved| saved.contains("p.md")) {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    mount.stop().await;

    std::fs::write(mount_dir.path().join("p.md"), "offline edit").unwrap();
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;

    wait_for(&share_dir.path().join("p.md"), Some("offline edit")).await;
    wait_for(&mount_dir.path().join("p.md"), Some("offline edit")).await;

    mount.stop().await;
    share.stop().await;
}
