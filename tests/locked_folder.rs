#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use common::{Peer, wait_for};

/// A mount starts with a folder inside it that it cannot list. A scan that gives up there must not read the whole
/// folder as empty: that would delete every file on every peer.
#[tokio::test(flavor = "multi_thread")]
async fn a_folder_that_cannot_be_listed_at_start_deletes_nothing() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(share_dir.path().join("p.md"), "hello").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("p.md"), Some("hello")).await;
    let history = mount_dir.path().join(".agent-knowledge-index.json");
    while !std::fs::read_to_string(&history).is_ok_and(|saved| saved.contains("p.md")) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    mount.stop().await;

    let locked = mount_dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&locked).is_ok() {
        return; // a user that can list any folder, such as root, cannot make it unreadable
    }
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let on_the_share = std::fs::read_to_string(share_dir.path().join("p.md")).ok();
    let on_the_mount = std::fs::read_to_string(mount_dir.path().join("p.md")).ok();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    mount.stop().await;
    share.stop().await;

    assert_eq!(on_the_share.as_deref(), Some("hello"), "the share lost a file after its peer could not list a folder");
    assert_eq!(on_the_mount.as_deref(), Some("hello"), "the mount lost its file after it could not list a folder");
}
