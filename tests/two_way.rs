mod common;

use common::{Pair, wait_for};

#[tokio::test(flavor = "multi_thread")]
async fn edits_flow_both_ways() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(share_dir.path().join("sub")).unwrap();
    std::fs::write(share_dir.path().join("sub/start.md"), "from share").unwrap();

    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;

    wait_for(&mount_dir.path().join("sub/start.md"), Some("from share")).await;

    std::fs::write(mount_dir.path().join("back.md"), "from mount").unwrap();
    wait_for(&share_dir.path().join("back.md"), Some("from mount")).await;

    std::fs::write(share_dir.path().join("back.md"), "edited by share").unwrap();
    wait_for(&mount_dir.path().join("back.md"), Some("edited by share")).await;

    std::fs::remove_file(mount_dir.path().join("sub/start.md")).unwrap();
    wait_for(&share_dir.path().join("sub/start.md"), None).await;

    pair.stop().await;
}
