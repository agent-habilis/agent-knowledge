mod common;

use common::{Peer, wait_for};
use std::time::{Duration, SystemTime};

const INDEX_FILE: &str = ".agent-knowledge-index.json";

/// P1. A peer that took a remote delete keeps a live entry in its own index. When it restarts, that entry
/// becomes a tombstone with a new ts, and it deletes a newer copy that another peer made in the meantime.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_does_not_delete_a_file_recreated_while_stopped() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v1")).await;

    std::fs::remove_file(share_dir.path().join("a.md")).unwrap();
    wait_for(&mount_dir.path().join("a.md"), None).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    mount.stop().await;
    let saved = std::fs::read_to_string(mount_dir.path().join(INDEX_FILE)).unwrap_or_default();
    eprintln!("mount own index after the remote delete: {saved}");

    std::fs::write(share_dir.path().join("a.md"), "v2").unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let share_copy = std::fs::read_to_string(share_dir.path().join("a.md")).ok();
    let mount_copy = std::fs::read_to_string(mount_dir.path().join("a.md")).ok();
    mount.stop().await;
    share.stop().await;
    assert_eq!(share_copy.as_deref(), Some("v2"), "the restart deleted the share's newer copy");
    assert_eq!(mount_copy.as_deref(), Some("v2"), "the mount did not get the newer copy");
}

/// P2. An idle peer must not rewrite its own index file again and again. The file is in the watched root.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_peer_does_not_rewrite_its_index_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), "x").unwrap();
    let share = Peer::start(dir.path(), None, None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut seen: Vec<SystemTime> = Vec::new();
    for _ in 0..100 {
        if let Ok(meta) = std::fs::metadata(dir.path().join(INDEX_FILE))
            && let Ok(modified) = meta.modified()
            && seen.last() != Some(&modified)
        {
            seen.push(modified);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    share.stop().await;
    eprintln!("distinct index file mtimes in 10 s of idle: {}", seen.len());
    assert!(seen.len() <= 2, "the idle peer rewrote its index file {} times in 10 s", seen.len());
}

/// P3. A delete that another peer made while this peer was stopped must stay a delete after this peer restarts.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_a_remote_delete_made_while_stopped() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v1")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    mount.stop().await;

    std::fs::remove_file(share_dir.path().join("a.md")).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let share_has = share_dir.path().join("a.md").exists();
    let mount_has = mount_dir.path().join("a.md").exists();
    mount.stop().await;
    share.stop().await;
    assert!(!share_has, "the mount's restart brought the deleted file back on the share");
    assert!(!mount_has, "the deleted file stayed on the mount");
}

/// P4. An edit that another peer made while this peer was stopped must survive this peer's restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_keeps_a_remote_edit_made_while_stopped() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v1")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    mount.stop().await;

    std::fs::write(share_dir.path().join("a.md"), "v2").unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mount = Peer::start(mount_dir.path(), Some(mesh), None, |_| {}).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let share_copy = std::fs::read_to_string(share_dir.path().join("a.md")).ok();
    let mount_copy = std::fs::read_to_string(mount_dir.path().join("a.md")).ok();
    mount.stop().await;
    share.stop().await;
    assert_eq!(share_copy.as_deref(), Some("v2"), "the mount's restart replaced the share's newer edit");
    assert_eq!(mount_copy.as_deref(), Some("v2"), "the mount kept its stale copy");
}

/// P5. A delete stays a delete after its author leaves. A third peer that joins later must not get the file back.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_outlives_its_author() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let late_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("a.md"), "v1").unwrap();
    std::fs::write(share_dir.path().join("keep.md"), "k").unwrap();
    let share = Peer::start(share_dir.path(), None, None, |_| {}).await;
    let mesh = share.ready.mesh.clone();
    let mount = Peer::start(mount_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&mount_dir.path().join("a.md"), Some("v1")).await;
    std::fs::remove_file(share_dir.path().join("a.md")).unwrap();
    wait_for(&mount_dir.path().join("a.md"), None).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    share.stop().await;
    let late = Peer::start(late_dir.path(), Some(mesh), None, |_| {}).await;
    wait_for(&late_dir.path().join("keep.md"), Some("k")).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let late_has = late_dir.path().join("a.md").exists();
    let mount_has = mount_dir.path().join("a.md").exists();
    late.stop().await;
    mount.stop().await;
    assert!(!mount_has, "the deleted file came back on the mount after its deleter left");
    assert!(!late_has, "a late peer got the deleted file from the mount's stale entry");
}
