mod common;

use std::time::Duration;

use common::{Pair, wait_for};

/// A file that is not synced changes every 100 ms, so the folder is never quiet and the watcher sends no tick. A new
/// note must still be published, by the timer, and not only when the changes stop.
#[tokio::test(flavor = "multi_thread")]
async fn a_busy_hidden_file_does_not_hold_back_a_new_note() {
    let (share_dir, mount_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    std::fs::write(share_dir.path().join("seed.md"), "seed").unwrap();
    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;
    wait_for(&mount_dir.path().join("seed.md"), Some("seed")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let busy = mount_dir.path().join(".busy.log");
    let writer = tokio::spawn(async move {
        for n in 0..400u32 {
            std::fs::write(&busy, n.to_string()).unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    std::fs::write(mount_dir.path().join("a.md"), "x").unwrap();

    // The writer runs for 40 s, so a note that arrives sooner did not wait for it to stop.
    wait_for(&share_dir.path().join("a.md"), Some("x")).await;

    writer.abort();
    pair.stop().await;
}
