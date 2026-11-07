use std::path::Path;
use std::time::Duration;

use agent_knowledge::sync;
use tokio::sync::{oneshot, watch};

async fn wait_for(path: &Path, want: Option<&str>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let got = std::fs::read_to_string(path).ok();
        if got.as_deref() == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{} is {got:?}, want {want:?}", path.display());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn edits_flow_both_ways() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(share_dir.path().join("sub")).unwrap();
    std::fs::write(share_dir.path().join("sub/start.md"), "from share").unwrap();

    let (stop_tx, stop_rx) = watch::channel(());
    let stopped = |mut stop: watch::Receiver<()>| async move {
        let _ = stop.changed().await;
    };
    let (hash_tx, hash_rx) = oneshot::channel();
    let share = tokio::spawn(sync::run(
        share_dir.path().to_path_buf(),
        None,
        move |hash: &str| hash_tx.send(hash.to_owned()).unwrap(),
        stopped(stop_rx.clone()),
    ));
    let hash = hash_rx.await.unwrap();
    let mount = tokio::spawn(sync::run(mount_dir.path().to_path_buf(), Some(hash), |_: &str| {}, stopped(stop_rx)));

    wait_for(&mount_dir.path().join("sub/start.md"), Some("from share")).await;

    std::fs::write(mount_dir.path().join("back.md"), "from mount").unwrap();
    wait_for(&share_dir.path().join("back.md"), Some("from mount")).await;

    std::fs::write(share_dir.path().join("back.md"), "edited by share").unwrap();
    wait_for(&mount_dir.path().join("back.md"), Some("edited by share")).await;

    std::fs::remove_file(mount_dir.path().join("sub/start.md")).unwrap();
    wait_for(&share_dir.path().join("sub/start.md"), None).await;

    stop_tx.send(()).unwrap();
    share.await.unwrap().unwrap();
    mount.await.unwrap().unwrap();
}
