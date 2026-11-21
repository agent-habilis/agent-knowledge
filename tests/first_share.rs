mod common;

use common::{Pair, wait_for};

/// The engine allows a burst of 5 slice writes, then 1 each second. A first share of more files
/// than that must still arrive whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_share_of_many_files_arrives_whole() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let names: Vec<String> = (1..=12).map(|n| format!("note-{n}.md")).collect();
    for name in &names {
        std::fs::write(share_dir.path().join(name), format!("body of {name}")).unwrap();
    }

    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;

    for name in &names {
        wait_for(&mount_dir.path().join(name), Some(&format!("body of {name}"))).await;
    }

    pair.stop().await;
}
