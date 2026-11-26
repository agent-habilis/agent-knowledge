#![allow(dead_code)] // Each test crate uses a different subset of these helpers.

use std::path::Path;
use std::time::Duration;

use agent_knowledge::sync::{self, Presence, Ready};
use agent_knowledge::writers::Writers;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// How long a test waits for the mesh to deliver something.
pub const WAIT: Duration = Duration::from_secs(30);

/// How long [`Peer::stop`] waits for a loop to exit.
pub const STOP_LIMIT: Duration = Duration::from_secs(30);

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
    sync::Options {
        root: root.to_path_buf(),
        mesh,
        nickname: nickname.map(str::to_owned),
        status_file: None,
        writers: Writers::Everybody,
    }
}

/// How long a peer's runtime waits for its blocking threads when the peer is dropped.
const RUNTIME_GRACE: Duration = Duration::from_secs(5);

/// One running `sync::run` loop. It runs on a runtime of its own. A test that panics drops its runtime, and that drop
/// waits for every blocking thread of the runtime, so a stuck lookup of a peer would keep a failed test alive. A peer
/// that is dropped gives up its blocking threads after [`RUNTIME_GRACE`].
pub struct Peer {
    pub ready: Ready,
    stop: watch::Sender<()>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
    runtime: Option<Runtime>,
}

impl Peer {
    /// Start a loop on `root` and wait until its mesh is up.
    pub async fn start(
        root: &Path,
        mesh: Option<String>,
        nickname: Option<&str>,
        on_presence: impl FnMut(Presence) + Send + 'static,
    ) -> Self {
        Self::start_with(options(root, mesh, nickname), on_presence).await
    }

    /// Start a share on `root` under the writer rule `writers`. `ready.mesh` is its hash.
    pub async fn share_as(root: &Path, writers: Writers) -> Self {
        Self::start_with(sync::Options { writers, ..options(root, None, None) }, |_| {}).await
    }

    /// [`Peer::start`] with every option spelled out.
    pub async fn start_with(options: sync::Options, on_presence: impl FnMut(Presence) + Send + 'static) -> Self {
        let (stop, mut stop_rx) = watch::channel(());
        let (ready_tx, ready_rx) = oneshot::channel();
        let runtime = Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let task = runtime.spawn(sync::run(
            options,
            move |ready: &Ready| ready_tx.send(ready.clone()).unwrap(),
            on_presence,
            async move {
                let _ = stop_rx.changed().await;
            },
        ));
        let ready = ready_rx.await.unwrap();
        Self { ready, stop, task: Some(task), runtime: Some(runtime) }
    }

    /// Signal the loop to stop and wait until it exits cleanly. Fails after [`STOP_LIMIT`].
    pub async fn stop(mut self) {
        self.stop.send(()).unwrap();
        let task = self.task.take().expect("a peer stops once");
        let exited = tokio::time::timeout(STOP_LIMIT, task).await;
        exited.expect("the peer did not stop within the limit").unwrap().unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // On a thread of its own: a runtime must not be shut down from async code, and a stuck blocking thread must
        // not hold up the test that dropped the peer.
        if let Some(runtime) = self.runtime.take() {
            std::thread::spawn(move || runtime.shutdown_timeout(RUNTIME_GRACE));
        }
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
