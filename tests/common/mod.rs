#![allow(dead_code)] // Each test crate uses a different subset of these helpers.

use std::path::Path;
use std::time::Duration;

use agent_knowledge::sync::{self, Presence, Ready};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// How long a test waits for the mesh to deliver something.
pub const WAIT: Duration = Duration::from_secs(30);

/// Poll `path` until its contents equal `want` (`None`: the file is absent); fail after [`WAIT`].
pub async fn wait_for(path: &Path, want: Option<&str>) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let got = std::fs::read_to_string(path).ok();
        if got.as_deref() == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{} is {got:?}, want {want:?}", path.display());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The `sync::Options` of a test peer on `root`.
pub fn options(root: &Path, mesh: Option<String>, nickname: Option<&str>) -> sync::Options {
    sync::Options { root: root.to_path_buf(), mesh, nickname: nickname.map(str::to_owned) }
}

/// One running `sync::run` loop.
pub struct Peer {
    pub ready: Ready,
    stop: watch::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Peer {
    /// Start a loop on `root` and wait until its mesh is up.
    pub async fn start(
        root: &Path,
        mesh: Option<String>,
        nickname: Option<&str>,
        on_presence: impl FnMut(Presence) + Send + 'static,
    ) -> Self {
        let (stop, mut stop_rx) = watch::channel(());
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(sync::run(
            options(root, mesh, nickname),
            move |ready: &Ready| ready_tx.send(ready.clone()).unwrap(),
            on_presence,
            async move {
                let _ = stop_rx.changed().await;
            },
        ));
        let ready = ready_rx.await.unwrap();
        Self { ready, stop, task }
    }

    /// Signal the loop to stop and wait until it exits cleanly.
    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

/// A `share` loop on `share_dir` and a `mount` loop on `mount_dir`, joined by the share's hash.
pub struct Pair {
    peers: [Peer; 2],
}

impl Pair {
    /// Start the share, then a mount joined by the share's hash.
    pub async fn start(share_dir: &Path, mount_dir: &Path) -> Self {
        let share = Peer::start(share_dir, None, None, |_| {}).await;
        let mount = Peer::start(mount_dir, Some(share.ready.mesh.clone()), None, |_| {}).await;
        Self { peers: [share, mount] }
    }

    /// Stop both peers.
    pub async fn stop(self) {
        for peer in self.peers {
            peer.stop().await;
        }
    }
}
