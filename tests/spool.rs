mod common;

use common::{Pair, wait_for};
use std::path::Path;

/// The spool directories of this process, and the number of files in them.
fn spool_of_this_process() -> (usize, usize) {
    let prefix = format!("agent-knowledge-{}-", std::process::id());
    let (mut dirs, mut files) = (0, 0);
    for entry in std::fs::read_dir(std::env::temp_dir()).unwrap().flatten() {
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            dirs += 1;
            files += count_files(&entry.path());
        }
    }
    (dirs, files)
}

fn count_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries.flatten().map(|entry| if entry.path().is_dir() { count_files(&entry.path()) } else { 1 }).sum()
        })
        .unwrap_or(0)
}

/// Each edit writes a file blob in the share, and the mount serves the copy it downloads, so each edit adds one file
/// blob per peer. The index blob of the edit before is evicted, so no index blob stays behind: an index that was not
/// evicted would add two more per edit, and the bound would fail.
#[tokio::test(flavor = "multi_thread")]
async fn the_spool_does_not_grow_with_each_index() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v0").unwrap();
    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v0")).await;
    let (dirs_before, files_before) = spool_of_this_process();

    for n in 1..=20 {
        std::fs::write(share_dir.path().join("a.md"), format!("v{n}")).unwrap();
        wait_for(&mount_dir.path().join("a.md"), Some(&format!("v{n}"))).await;
    }
    let (dirs_after, files_after) = spool_of_this_process();
    pair.stop().await;

    assert!(dirs_after <= dirs_before + 2, "spool directories grew from {dirs_before} to {dirs_after}");
    assert!(files_after <= files_before + 2 * 20 + 4, "spool files grew from {files_before} to {files_after}");
}
