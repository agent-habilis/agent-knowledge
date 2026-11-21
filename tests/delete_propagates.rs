mod common;

use std::time::Duration;

use common::{Pair, wait_for};

/// Long enough for several reconcile ticks, so a file that comes back would have come back.
const SETTLE: Duration = Duration::from_secs(7);

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_reaches_the_peer_whoever_wrote_the_file_and_stays_deleted() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let (share, mount) = (share_dir.path(), mount_dir.path());
    std::fs::write(share.join("a.md"), "a").unwrap();
    std::fs::write(share.join("b.md"), "b").unwrap();

    let pair = Pair::start(share, mount).await;
    wait_for(&mount.join("a.md"), Some("a")).await;
    wait_for(&mount.join("b.md"), Some("b")).await;

    // The author deletes its own file.
    std::fs::remove_file(share.join("a.md")).unwrap();
    wait_for(&mount.join("a.md"), None).await;

    // A peer deletes a file that another peer wrote: only a tombstone of its own can say so.
    std::fs::remove_file(mount.join("b.md")).unwrap();
    wait_for(&share.join("b.md"), None).await;
    tokio::time::sleep(SETTLE).await;
    wait_for(&share.join("b.md"), None).await;
    wait_for(&mount.join("b.md"), None).await;

    // A file written again after the delete beats the tombstone.
    std::fs::write(share.join("b.md"), "b again").unwrap();
    wait_for(&mount.join("b.md"), Some("b again")).await;

    pair.stop().await;
}
