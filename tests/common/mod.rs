use std::path::Path;
use std::time::Duration;

use agent_knowledge::sync;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub async fn wait_for(path: &Path, want: Option<&str>) {
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

/// A `share` loop on `share_dir` and a `mount` loop on `mount_dir`, joined by the share's hash.
pub struct Pair {
    stop: watch::Sender<()>,
    loops: [JoinHandle<anyhow::Result<()>>; 2],
}

impl Pair {
    pub async fn start(share_dir: &Path, mount_dir: &Path) -> Self {
        let (stop, stop_rx) = watch::channel(());
        let stopped = |mut stop: watch::Receiver<()>| async move {
            let _ = stop.changed().await;
        };
        let (hash_tx, hash_rx) = oneshot::channel();
        let share = tokio::spawn(sync::run(
            share_dir.to_path_buf(),
            None,
            move |hash: &str| hash_tx.send(hash.to_owned()).unwrap(),
            stopped(stop_rx.clone()),
        ));
        let hash = hash_rx.await.unwrap();
        let mount = tokio::spawn(sync::run(mount_dir.to_path_buf(), Some(hash), |_: &str| {}, stopped(stop_rx)));
        Self { stop, loops: [share, mount] }
    }

    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        for side in self.loops {
            side.await.unwrap().unwrap();
        }
    }
}
