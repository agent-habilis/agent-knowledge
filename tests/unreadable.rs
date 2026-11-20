#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use common::{Pair, wait_for};

/// A file that exists but cannot be read is not a deleted file. It must not be deleted on the other peers.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_cannot_be_read_is_not_deleted_on_the_other_peer() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(share_dir.path().join("p.md"), "hello").unwrap();
    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;
    wait_for(&mount_dir.path().join("p.md"), Some("hello")).await;

    let mounted = mount_dir.path().join("p.md");
    std::fs::set_permissions(&mounted, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&mounted).is_ok() {
        return; // a user that can read any file, such as root, cannot make it unreadable
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    let on_the_share = std::fs::read_to_string(share_dir.path().join("p.md")).ok();
    std::fs::set_permissions(&mounted, std::fs::Permissions::from_mode(0o644)).unwrap();

    assert_eq!(on_the_share.as_deref(), Some("hello"), "the share lost a file that its peer could not read");
    pair.stop().await;
}
