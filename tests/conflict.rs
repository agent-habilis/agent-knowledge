mod common;

use std::path::Path;
use std::time::Duration;

use common::Pair;

const SETTLE: Duration = Duration::from_secs(7);
/// Locally the 2 edits converge in about 2 s. A longer cap would not help a lost edit, which does not heal with time, so
/// this stays at the 30 s of every other wait. The failure message shows how long it waited and what each side holds.
const CONVERGE: Duration = common::WAIT;

fn read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Wait until both folders hold the same content for `name`, and return it.
async fn converged(left: &Path, right: &Path, name: &str) -> String {
    let started = tokio::time::Instant::now();
    loop {
        let (a, b) = (read(&left.join(name)), read(&right.join(name)));
        if let Some(content) = a.as_ref().filter(|_| a == b) {
            eprintln!("{name} converged after {:?}", started.elapsed());
            return content.clone();
        }
        assert!(started.elapsed() < CONVERGE, "{name} never converged in {CONVERGE:?}: {a:?} vs {b:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_peers_that_write_one_path_at_once_converge_on_one_content() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let (share, mount) = (share_dir.path(), mount_dir.path());

    std::fs::write(share.join("c.md"), "start").unwrap();
    let pair = Pair::start(share, mount).await;
    // Both edits must be made against the same synced version of the file.
    common::wait_for(&mount.join("c.md"), Some("start")).await;
    std::fs::write(share.join("c.md"), "from share").unwrap();
    std::fs::write(mount.join("c.md"), "from mount").unwrap();

    let winner = converged(share, mount, "c.md").await;
    assert!(winner == "from share" || winner == "from mount", "unexpected content {winner:?}");

    // The winner stays: the loser does not take its copy back.
    tokio::time::sleep(SETTLE).await;
    assert_eq!(read(&share.join("c.md")).as_deref(), Some(winner.as_str()));
    assert_eq!(read(&mount.join("c.md")).as_deref(), Some(winner.as_str()));

    pair.stop().await;
}
