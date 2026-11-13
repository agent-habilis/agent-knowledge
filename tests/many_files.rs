mod common;

use common::{Pair, wait_for};

/// A share with more files than one slice holds (a slice is one frame) syncs every file to the mount.
#[tokio::test(flavor = "multi_thread")]
async fn a_share_with_100_files_syncs() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    for n in 0..100 {
        std::fs::write(share_dir.path().join(format!("file-{n:03}.md")), format!("body {n}")).unwrap();
    }

    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;

    for n in 0..100 {
        wait_for(&mount_dir.path().join(format!("file-{n:03}.md")), Some(&format!("body {n}"))).await;
    }
    pair.stop().await;
}
