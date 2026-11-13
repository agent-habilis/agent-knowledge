//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` document holds one slice per author. Each slice is a pointer to that author's
//! index blob, `{"index": {"ticket", "sha256", "ts"}}`. An index is a map from path to an entry:
//! `{"sha256", "ticket", "ts"}` for a file, or `{"deleted": true, "ts"}` for a delete. The entry with
//! the larger `ts` wins a path, and a winning tombstone removes the local copy. File bytes do not fit
//! in a gossip frame, so each entry holds a blob ticket that a peer redeems against the author's
//! blob server.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::iroh::Endpoint;
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::build_peer_endpoint;
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{BlobError, BlobServer, BlobTicket, ContentId, fetch_hedged};
use habilis_network::protocol::{Lookup, LookupOpts, Transport};
use serde_json::{Value, json};
use tokio::io::AsyncWrite;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::AbortHandle;

use crate::scan::{self, resolve, sha256_bytes};
use crate::status::{self, Status};
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);

/// What to sync, and as whom.
pub struct Options {
    /// The folder to keep in sync.
    pub root: PathBuf,
    /// The mesh to join, or `None` to create a new one.
    pub mesh: Option<String>,
    /// This peer's nickname, or `None` for the mesh to mint a random one.
    pub nickname: Option<String>,
    /// Where to keep this memory's [`Status`] for `agent-knowledge status`, or `None` for nowhere.
    pub status_file: Option<PathBuf>,
}

/// The mesh is up.
#[derive(Clone, Debug)]
pub struct Ready {
    /// The mesh id, which is the hash that `mount` takes.
    pub mesh: String,
    /// This peer's nickname: the one asked for, or the one the mesh minted.
    pub nick: String,
}

/// Another peer joined or left the mesh, by nickname.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Presence {
    Joined(String),
    Left(String),
}

/// Sync `options.root` until `shutdown` resolves. With `options.mesh` `None`, create a new mesh;
/// else join it. `on_ready` gets the mesh id and this peer's nickname; `on_presence` gets each
/// other peer that joins or leaves.
pub async fn run(
    options: Options,
    on_ready: impl FnOnce(&Ready),
    mut on_presence: impl FnMut(Presence),
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let Options { root, mesh, nickname, status_file } = options;
    let root = std::fs::canonicalize(&root).with_context(|| format!("no folder at {}", root.display()))?;
    let (sink, mut events) = membership::json_sink();
    let options = JoinOptions::default()
        .mesh(mesh)
        .nick(nickname)
        .name(Some(MESH_NAME.to_owned()))
        .lookup([Lookup::Mdns, Lookup::Dht, Lookup::Relay])
        // The engine default plus `Relay`. Without it, 2 peers whose hole punch fails find each
        // other through the relay but never link, because the relay is kept for lookup alone.
        .transport([Transport::Udp, Transport::WebRtc, Transport::Multihop, Transport::Gossip, Transport::Relay]);
    let member = membership::join(&options, sink).await.map_err(|error| anyhow!("{error}"))?;
    let ready = wait_ready(&mut events).await?;
    on_ready(&ready);

    let spool = spool_dir();
    let blobs = BlobServer::start(blob_lookups(), spool.clone(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let (done, mut finished) = mpsc::unbounded_channel();
    let (index_tx, mut index_done) = mpsc::unbounded_channel();
    let mesh = ready.mesh.clone();
    let saved = std::fs::read(root.join(INDEX_FILE)).ok().map(|bytes| load_saved(&bytes, &mesh)).unwrap_or_default();
    let disk = scan::scan(&root).unwrap_or_default();
    let own = own_after_restart(saved, &disk, now_ms());
    // A disk file whose sha is its saved entry is not an edit made while stopped, so it is not published again.
    let known: HashMap<String, String> = own
        .iter()
        .filter_map(|(path, entry)| {
            let sha = entry.get("sha256")?.as_str()?;
            (disk.get(path).map(String::as_str) == Some(sha)).then(|| (path.clone(), sha.to_owned()))
        })
        .collect();
    // A kept file has no ticket of this run, so it is registered below. Its ts stays, so a restart is not a new edit.
    let kept: Vec<(String, String, u64)> = known
        .iter()
        .map(|(path, sha)| {
            let ts = own.get(path).and_then(|entry| entry.get("ts")).and_then(Value::as_u64).unwrap_or(0);
            (path.clone(), sha.clone(), ts)
        })
        .collect();
    let status = Status { mesh: ready.mesh, nick: ready.nick, folder: root.clone(), ..Status::default() };
    let mut folder = Folder {
        root: root.clone(),
        member,
        blobs,
        known,
        downloads: Downloads::new(root, done),
        consumer: Arc::new(Consumer::new()),
        status,
        status_file,
        mesh,
        pointers: HashMap::new(),
        own_saved: Vec::new(),
        index_refused_at: None,
        own_version: 0,
        indexes: Indexes::default(),
        index_pending: HashSet::new(),
        index_tasks: HashMap::new(),
        index_tx,
        own,
        index_sha: None,
        // A restart may have turned files into tombstones, so the first pass publishes the index.
        index_dirty: true,
        index_version: 0,
        winner_ts: HashMap::new(),
        index_written: None,
        spool,
    };

    for (path, sha, ts) in kept {
        if let Err(error) = folder.publish(&path, &sha, ts).await {
            folder.failed(&format!("kept file {path} failed to register: {error:#}"));
        }
    }

    // Open item: this publishes before the document has arrived, so a stale copy of an untracked file can replace a
    // newer remote version. It is pre-existing, and it is not fixed here.
    folder.reconcile().await;
    folder.publish_local().await;
    folder.write_status().await;

    let mut every = tokio::time::interval(RECONCILE_EVERY);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(()) = ticks.recv() => folder.publish_local().await,
            Some(event) = events.recv() => {
                let Ok(event) = serde_json::from_str::<Value>(&event) else { continue };
                if let Some(presence) = presence(&event) {
                    on_presence(presence);
                }
                // Not the event's own document: events queue up while a download runs, and an
                // older snapshot that lacks a path would delete a file that a newer one has.
                if is_remote_state_change(&event) {
                    folder.reconcile().await;
                }
            }
            Some(done) = finished.recv() => folder.finish(done).await,
            Some(done) = index_done.recv() => folder.index_done(done).await,
            _ = every.tick() => folder.reconcile().await,
        }
        folder.publish_index().await;
        folder.write_status().await;
    }
    for (_, (_, handle)) in folder.index_tasks.drain() {
        handle.abort();
    }
    folder.save_own();
    folder.downloads.abort_all();
    folder.consumer.shutdown().await;
    folder.blobs.shutdown().await;
    membership::depart(folder.member.node).await.map_err(|error| anyhow!("{error}"))
}

struct Folder {
    root: PathBuf,
    member: Membership,
    blobs: BlobServer,
    /// The sha256 of each path as both the disk and the document last agreed on it.
    /// A watcher tick or a document entry that matches it is our own echo, and is skipped.
    known: HashMap<String, String>,
    downloads: Downloads,
    consumer: Arc<Consumer<Endpoint>>,
    /// What `agent-knowledge status` shows, kept up to date as the folder syncs.
    status: Status,
    status_file: Option<PathBuf>,
    /// The entries of each author's fetched index.
    indexes: Indexes,
    /// The index fetches under way, by author and sha256, so each one starts once.
    index_pending: HashSet<(String, String)>,
    /// The index fetch under way for each author, with its handle to cancel it.
    index_tasks: HashMap<String, (String, AbortHandle)>,
    index_tx: mpsc::UnboundedSender<IndexDone>,
    /// This peer's own entries: one per file it publishes, and a tombstone per file it deleted.
    own: BTreeMap<String, Value>,
    /// The sha256 of the own index now published, and whether the own index changed since.
    index_sha: Option<String>,
    index_dirty: bool,
    index_version: u64,
    /// The mesh this peer is in, which its saved index is bound to.
    mesh: String,
    /// The pointer that each author's slice had when the document was last read.
    pointers: HashMap<String, String>,
    /// The saved index as last written, so an idle peer writes nothing.
    own_saved: Vec<u8>,
    /// The `own_version` at which the own index was refused for the cap. It is not serialized again until own changes.
    index_refused_at: Option<u64>,
    /// Counts each change to `own`, so a refused index is retried only after a change.
    own_version: u64,
    /// The ts of the winning entry of each path in the view, which a served copy keeps.
    winner_ts: HashMap<String, u64>,
    /// When the own index was last written. A pointer write waits for `INDEX_INTERVAL` after it.
    index_written: Option<tokio::time::Instant>,
    /// The directory that the blob store keeps its spool in, the one the index is written into.
    spool: PathBuf,
}

/// One file the document names: its path, its sha256, and its blob ticket.
type Entry = (String, String, String);

/// Why a download failed. A permanent failure is not retried for the same entry.
#[derive(Debug)]
enum Failure {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
    /// A write to the folder failed, such as a full disk. It is retried after a long, growing wait.
    LocalWrite(anyhow::Error),
}

/// At most this many blob fetches run at once.
const FETCH_SLOTS: usize = 4;
/// A transfer that gets no byte this long after it starts is cut off, and the retry loop starts a new attempt. The
/// dial is capped at 90 s.
const ATTEMPT_DEADLINE: Duration = Duration::from_secs(180);

/// How long a transfer may stay quiet after its last byte before it is cut.
const IDLE_LIMIT: Duration = Duration::from_secs(60);

/// One blob transfer's progress: its bytes, when it started, and when its last byte came.
#[derive(Clone)]
struct Progress(Arc<std::sync::Mutex<Transfer>>);

struct Transfer {
    bytes: u64,
    started: Option<tokio::time::Instant>,
    last: tokio::time::Instant,
}

impl Default for Progress {
    fn default() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Transfer { bytes: 0, started: None, last: tokio::time::Instant::now() })))
    }
}

impl Progress {
    /// Starts the clock. Until then the transfer is not cut, so a wait for a fetch slot does not count as a stall.
    fn start(&self) {
        let now = tokio::time::Instant::now();
        let mut transfer = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        transfer.started = Some(now);
        transfer.last = now;
    }

    fn record(&self, bytes: usize) {
        let mut transfer = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        transfer.bytes += bytes as u64;
        transfer.last = tokio::time::Instant::now();
    }

    /// Before the first byte, the setup deadline applies. After it, the transfer is cut only when it has been quiet
    /// for `IDLE_LIMIT`, so a slow transfer that keeps moving can finish. `None` until `start`.
    fn deadline(&self) -> Option<tokio::time::Instant> {
        let transfer = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let started = transfer.started?;
        Some(if transfer.bytes == 0 { started + ATTEMPT_DEADLINE } else { transfer.last + IDLE_LIMIT })
    }
}

/// How often a transfer's deadline is checked. A byte moves the deadline, so the check must run again to see it.
const DEADLINE_CHECK: Duration = Duration::from_secs(1);

/// Runs `attempt` until it ends, or returns `None` once the transfer is past its deadline.
async fn guarded<T>(attempt: impl Future<Output = T>, progress: &Progress) -> Option<T> {
    tokio::pin!(attempt);
    loop {
        tokio::select! {
            out = &mut attempt => return Some(out),
            () = tokio::time::sleep(DEADLINE_CHECK) => {}
        }
        if progress.deadline().is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return None;
        }
    }
}

/// Collects a blob transfer, and records each write in its progress.
struct Collected {
    bytes: Vec<u8>,
    progress: Progress,
}

impl AsyncWrite for Collected {
    fn poll_write(self: Pin<&mut Self>, _: &mut TaskContext<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.bytes.extend_from_slice(buf);
        this.progress.record(buf.len());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Cancels the task it holds when dropped, so an aborted outer task takes its inner task with it.
struct CancelOnDrop(AbortHandle);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A path whose write failed, and when its download may run again.
struct LocalRetry {
    sha: String,
    at: tokio::time::Instant,
    attempts: u32,
}

/// The wait after a failed write: 1 min, doubling up to 30 min.
fn local_backoff(attempts: u32) -> Duration {
    Duration::from_secs(60).saturating_mul(2u32.saturating_pow(attempts)).min(Duration::from_secs(30 * 60))
}

/// The downloads of one folder. The loop owns it. Each download runs in its own task.
struct Downloads {
    root: PathBuf,
    gate: Gate,
    /// The sha and ticket of each entry whose download failed for good.
    failed: HashMap<String, (String, String)>,
    /// When a path whose write failed may be tried again.
    local_retry: HashMap<String, LocalRetry>,
    /// The sha of each download in flight, and the handle that cancels it.
    running: HashMap<(String, String), (String, AbortHandle)>,
    slots: Arc<Semaphore>,
    done: mpsc::UnboundedSender<Finished>,
}

/// How one download ended. On `Ok` the file is on disk, and the caller must publish it.
struct Finished {
    path: String,
    sha: String,
    ticket: String,
    result: Result<(), Failure>,
}

impl Downloads {
    fn new(root: PathBuf, done: mpsc::UnboundedSender<Finished>) -> Self {
        Self {
            root,
            gate: Gate::default(),
            failed: HashMap::new(),
            local_retry: HashMap::new(),
            running: HashMap::new(),
            slots: Arc::new(Semaphore::new(FETCH_SLOTS)),
            done,
        }
    }

    /// Start a download for each entry that `known` does not hold and no download is already running for.
    /// Each one reports back on the `done` channel. A download for a sha that the document no longer names is cancelled.
    fn reconcile<F, Fut>(&mut self, entries: &[Entry], known: &HashMap<String, String>, fetch: F)
    where
        F: Fn(String, Progress) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send,
    {
        for (path, sha, ticket) in entries {
            self.gate.set_wanted(path, sha, ticket);
            // A landed copy that is not `sha` is a stale download, so `sha` is fetched again even when `known` holds it.
            let synced = known.get(path) == Some(sha) && self.gate.landed_sha(path).is_none_or(|landed| &landed == sha);
            if synced
                || self.failed.get(path) == Some(&(sha.clone(), ticket.clone()))
                || self.waiting_to_retry(path, sha)
            {
                continue;
            }
            let key = (path.clone(), ticket.clone());
            if self.running.get(&key).is_some_and(|(running_sha, _)| running_sha == sha) {
                continue;
            }
            // Another sha for this path is obsolete. A ticket change for this sha is not: its transfer may be half done.
            self.running.retain(|(running_path, _), (running_sha, handle)| {
                let obsolete = running_path == path && running_sha != sha;
                if obsolete {
                    handle.abort();
                }
                !obsolete
            });
            let Some(local) = resolve(&self.root, path) else {
                self.failed.insert(path.clone(), (sha.clone(), ticket.clone()));
                eprintln!("download {path} failed: unsafe path {path:?}");
                continue;
            };
            // A fetch takes a slot only while it runs, so a download that waits to retry holds none.
            let (slots, fetch_again) = (self.slots.clone(), fetch.clone());
            let attempt = move |ticket: String, progress: Progress| {
                let (slots, fetch) = (slots.clone(), fetch_again.clone());
                async move {
                    let _slot = slots.acquire_owned().await;
                    progress.start();
                    fetch(ticket, progress).await
                }
            };
            let done = self.done.clone();
            let (path_owned, sha_owned, ticket_owned) = (path.clone(), sha.clone(), ticket.clone());
            let (path_for_fetch, sha_for_fetch, ticket_for_fetch) =
                (path.clone(), sha_owned.clone(), ticket_owned.clone());
            let gate = self.gate.clone();
            let task = tokio::spawn(async move {
                // The download runs in an inner task, so a panic in it still reports back as a failure.
                let inner = tokio::spawn(async move {
                    download_file(&local, &path_for_fetch, &sha_for_fetch, &ticket_for_fetch, &gate, attempt).await
                });
                let _cancel = CancelOnDrop(inner.abort_handle());
                let result = match inner.await {
                    Ok(result) => result,
                    // A panic is a bug in this code, so a retry would repeat it. It stays failed until the entry changes.
                    Err(error) => Err(Failure::Permanent(anyhow!("download task ended: {error}"))),
                };
                let _ = done.send(Finished { path: path_owned, sha: sha_owned, ticket: ticket_owned, result });
            });
            self.running.insert(key, (sha.clone(), task.abort_handle()));
        }
    }

    /// Record how a download ended, so a permanent failure is not started again.
    fn settle(&mut self, done: &Finished) -> bool {
        // A result from an entry that was replaced or cancelled must not change the current entry's state.
        let key = (done.path.clone(), done.ticket.clone());
        let current = self.running.get(&key).is_some_and(|(sha, _)| sha == &done.sha);
        if !current {
            return false;
        }
        self.running.remove(&key);
        match &done.result {
            // A superseded attempt stops with a transient failure. It has nothing to record or publish.
            Err(Failure::Transient(_)) => return false,
            Ok(()) => {
                self.failed.remove(&done.path);
                self.local_retry.remove(&done.path);
                // The file landed, so the other tickets for this sha need not finish.
                self.running.retain(|(running_path, _), (running_sha, handle)| {
                    let same = running_path == &done.path && running_sha == &done.sha;
                    if same {
                        handle.abort();
                    }
                    !same
                });
            }
            Err(Failure::Permanent(_)) => {
                self.failed.insert(done.path.clone(), (done.sha.clone(), done.ticket.clone()));
            }
            Err(Failure::LocalWrite(_)) => {
                let attempts = self.local_retry.get(&done.path).map_or(0, |retry| retry.attempts + 1);
                let at = tokio::time::Instant::now() + local_backoff(attempts);
                let retry = LocalRetry { sha: done.sha.clone(), at, attempts };
                self.local_retry.insert(done.path.clone(), retry);
            }
        }
        true
    }

    /// Whether the write for this entry failed, and its wait is not over yet.
    fn waiting_to_retry(&self, path: &str, sha: &str) -> bool {
        self.local_retry.get(path).is_some_and(|retry| retry.sha == sha && tokio::time::Instant::now() < retry.at)
    }

    /// Scans the folder. A file that differs from its known baseline is a local edit, and its download is forgotten, so
    /// an older download cannot overwrite it. A file with no baseline is not an edit: after a restart, the document's
    /// copy replaces it, as before. Keeping such an edit needs a persisted baseline. A write that a download made is
    /// not an edit either. The write turn is held across the scan, so no download write runs in between.
    fn claim_edits(&mut self, known: &HashMap<String, String>) -> io::Result<BTreeMap<String, String>> {
        let turn = self.gate.turn.clone();
        let _turn = turn.lock().unwrap_or_else(PoisonError::into_inner);
        let disk = scan::scan(&self.root)?;
        self.gate.keep_landed(&disk);
        for (path, sha) in &disk {
            if self.gate.landed_sha(path).as_deref() == Some(sha.as_str()) {
                continue;
            }
            if known.get(path).is_some_and(|base| base != sha) {
                self.forget(path);
            }
        }
        Ok(disk)
    }

    /// The disk files that `publish_local` publishes. A download's own write waits for its result. An untracked file
    /// waits while the document names a copy for its path. Its download publishes the file when it lands, so the file
    /// is not published twice, and it does not replace the document's copy.
    /// Open item: a local file at a path that the document names with other content is replaced by the download, with
    /// no conflict copy. That is part of the last-write-wins design, not this change.
    fn publishable(&self, disk: &BTreeMap<String, String>, known: &HashMap<String, String>) -> Vec<(String, String)> {
        disk.iter()
            .filter(|&(path, sha)| {
                if known.get(path) == Some(sha) || self.gate.landed_sha(path).as_deref() == Some(sha.as_str()) {
                    return false;
                }
                let held = !known.contains_key(path) && self.gate.wanted_sha(path).is_some();
                if held {
                    tracing::debug!(path = %path, "holding a local file: the document names a copy for its path");
                }
                !held
            })
            .map(|(path, sha)| (path.clone(), sha.clone()))
            .collect()
    }

    /// Forgets each path that the document does not name. Returns the local copy of each such path that holds content
    /// this peer got from the mesh, so the caller removes it. A local edit that no peer has seen stays on disk.
    fn drop_unnamed(
        &self,
        files: &serde_json::Map<String, Value>,
        known: &HashMap<String, String>,
    ) -> Vec<(String, Option<PathBuf>)> {
        let tracked: BTreeSet<String> = known.keys().cloned().chain(self.gate.landed_paths()).collect();
        unnamed(tracked.into_iter().collect(), files)
            .into_iter()
            .map(|path| {
                let synced = known.get(&path).cloned();
                let landed = self.gate.landed_sha(&path);
                let copy = resolve(&self.root, &path).filter(|local| {
                    let on_disk = file_sha(local);
                    on_disk.is_some() && (on_disk == synced || on_disk == landed)
                });
                (path, copy)
            })
            .collect()
    }

    /// Removes the local copy of each path that the document no longer names. A copy whose removal fails keeps its
    /// record, so the next call retries it, and the file is not published back as a local edit.
    fn remove_dropped(
        &mut self,
        files: &serde_json::Map<String, Value>,
        known: &mut HashMap<String, String>,
        remove: impl Fn(&Path) -> io::Result<()>,
    ) -> Vec<String> {
        let mut deleted = Vec::new();
        for (path, copy) in self.drop_unnamed(files, known) {
            if let Some(local) = &copy {
                // A file that is already gone counts as removed.
                let removed = remove(local).or_else(|error| match error.kind() {
                    io::ErrorKind::NotFound => Ok(()),
                    _ => Err(error),
                });
                if let Err(error) = removed {
                    tracing::debug!(path = %path, error = %error, "retrying a file removal on the next reconcile");
                    continue;
                }
                eprintln!("↓ deleted {path}");
                deleted.push(path.clone());
            }
            self.drop_record(&path, known);
        }
        deleted
    }

    /// Forgets a path that the document no longer names, once its local copy is gone or kept.
    fn drop_record(&self, path: &str, known: &mut HashMap<String, String>) {
        known.remove(path);
        self.gate.forget_landed(path);
    }

    /// Stop everything for a path that the document no longer names.
    fn forget(&mut self, path: &str) {
        self.gate.unwant(path);
        self.failed.remove(path);
        self.local_retry.remove(path);
        self.running.retain(|(running_path, _), (_, handle)| {
            let gone = running_path == path;
            if gone {
                handle.abort();
            }
            !gone
        });
    }

    /// Stop everything for each path that the document no longer names, including a retry that is waiting.
    fn prune(&mut self, files: &serde_json::Map<String, Value>) {
        for path in unnamed(self.gate.wanted_paths(), files) {
            self.forget(&path);
        }
    }

    /// Stop every download in flight, on shutdown.
    fn abort_all(&mut self) {
        for (_, (_, handle)) in self.running.drain() {
            handle.abort();
        }
    }
}

impl Folder {
    async fn publish_local(&mut self) {
        let disk = match self.downloads.claim_edits(&self.known) {
            Ok(disk) => disk,
            Err(error) => return eprintln!("scan failed: {error}"),
        };
        for (path, sha) in self.downloads.publishable(&disk, &self.known) {
            match self.publish(&path, &sha, now_ms()).await {
                Ok(()) => {
                    eprintln!("↑ {path}");
                    self.known.insert(path.clone(), sha.clone());
                    self.status.record("↑", &path, false);
                }
                Err(error) => self.failed(&format!("publish {path} failed: {error:#}")),
            }
        }
        let gone: Vec<String> = self.known.keys().filter(|path| !disk.contains_key(*path)).cloned().collect();
        for path in gone {
            eprintln!("↑ deleted {path}");
            self.own.insert(path.clone(), json!({ "deleted": true, "ts": now_ms() }));
            self.own_changed();
            self.known.remove(&path);
            self.status.record("↑", &path, true);
        }
        self.save_own();
        self.publish_index().await;
    }

    /// Log a failed transfer and count it for `status`.
    fn own_changed(&mut self) {
        self.index_dirty = true;
        self.own_version += 1;
    }

    fn failed(&mut self, message: &str) {
        eprintln!("{message}");
        self.status.failed += 1;
    }

    /// Refresh the peers and the file totals, then write the status file, if there is one.
    async fn write_status(&mut self) {
        let Some(path) = &self.status_file else { return };
        if let Ok(roster) = self.member.request(|reply| Request::Inspect(Inspect::Peers { reply })).await
            && let Ok(roster) = serde_json::from_str::<Value>(&roster)
        {
            self.status.peers = status::peers_from_roster(&roster);
        }
        self.status.files = self.known.len();
        self.status.bytes = self
            .known
            .keys()
            .filter_map(|rel| std::fs::metadata(self.root.join(rel)).ok())
            .map(|meta| meta.len())
            .sum();
        self.status.written_at = status::now();
        if let Err(error) = self.status.write(path) {
            eprintln!("{error:#}");
        }
    }

    async fn publish(&mut self, path: &str, sha: &str, ts: u64) -> Result<()> {
        let ticket = self
            .blobs
            .register(&self.root.join(path), ContentId::new(sha))
            .await
            .map_err(|error| anyhow!("{error}"))?;
        self.own.insert(path.to_owned(), json!({ "sha256": sha, "ticket": ticket.encode(), "ts": ts }));
        self.own_changed();
        Ok(())
    }

    /// Write the own index as one blob, point the own slice at it, and evict the index it replaces. A failure is logged,
    /// and the index stays dirty, so the next tick tries again.
    async fn publish_index(&mut self) {
        if !self.index_dirty
            || self.index_refused_at == Some(self.own_version)
            || self.index_written.is_some_and(|at| at.elapsed() < INDEX_INTERVAL)
        {
            return;
        }
        let bytes = match serde_json::to_vec(&self.own) {
            Ok(bytes) => bytes,
            Err(error) => return self.failed(&format!("own index failed to serialize: {error}")),
        };
        if bytes.len() > INDEX_MAX_BYTES {
            // Kept dirty and not truncated: the entries stay, and the log names the size once.
            if self.index_refused_at != Some(self.own_version) {
                self.index_refused_at = Some(self.own_version);
                eprintln!("own index refused: {} bytes is over the {INDEX_MAX_BYTES}-byte cap", bytes.len());
            }
            return;
        }
        let sha = sha256_bytes(&bytes);
        self.index_version += 1;
        // A unique name per version, never edited in place. The spool keeps its own link, so this copy goes.
        let spool = self.spool.clone();
        let temp = spool.join(format!("index-{}-{}.json", std::process::id(), self.index_version));
        let registered = match std::fs::create_dir_all(&spool).and_then(|()| std::fs::write(&temp, &bytes)) {
            Ok(()) => self.blobs.register(&temp, ContentId::new(&sha)).await.map_err(|error| anyhow!("{error}")),
            Err(error) => Err(error.into()),
        };
        let _ = std::fs::remove_file(&temp);
        let ticket = match registered {
            Ok(ticket) => ticket,
            Err(error) => return self.failed(&format!("own index blob failed, will retry: {error:#}")),
        };
        self.index_written = Some(tokio::time::Instant::now());
        match self.merge(json!({ "index": { "ticket": ticket.encode(), "sha256": sha, "ts": now_ms() } })).await {
            Ok(()) => {
                self.index_dirty = false;
                self.save_own();
                if let Some(previous) = self.index_sha.replace(sha.clone())
                    && previous != sha
                {
                    let _ = self.blobs.evict_content(&ContentId::new(&previous)).await;
                }
            }
            Err(error) => self.failed(&format!("own index pointer failed, will retry: {error:#}")),
        }
    }

    /// Keep the own entries in the hidden index file, so a restart keeps its tombstones. A failure is logged only.
    fn save_own(&mut self) {
        let bytes = match serde_json::to_vec(&json!({ "mesh": self.mesh, "own": self.own })) {
            Ok(bytes) => bytes,
            Err(error) => return eprintln!("own index not kept for a restart: {error}"),
        };
        if bytes == self.own_saved {
            return;
        }
        let temp = self.root.join(format!("{INDEX_FILE}.tmp"));
        let saved = std::fs::write(&temp, &bytes).and_then(|()| std::fs::rename(&temp, self.root.join(INDEX_FILE)));
        match saved {
            Ok(()) => self.own_saved = bytes,
            Err(error) => eprintln!("own index not kept for a restart: {error}"),
        }
    }

    /// Fetch the index of each author whose pointer is new. Until it lands, the author contributes no entries, so no
    /// file it lists is dropped.
    fn fetch_indexes(&mut self, document: &Value) {
        self.pointers = document
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(author, slice)| {
                Some((author.clone(), slice.get("index")?.get("sha256")?.as_str()?.to_owned()))
            })
            .collect();
        let running: HashMap<String, String> =
            self.index_tasks.iter().map(|(a, (sha, _))| (a.clone(), sha.clone())).collect();
        let (starts, cancels) = fetch_plan(document, &self.indexes, &running);
        for author in cancels {
            if let Some((sha, handle)) = self.index_tasks.remove(&author) {
                handle.abort();
                self.index_pending.remove(&(author.clone(), sha));
            }
        }
        for (author, ticket, sha) in starts {
            self.index_pending.insert((author.clone(), sha.clone()));
            let (consumer, tx) = (self.consumer.clone(), self.index_tx.clone());
            let (fetch_author, fetch_sha) = (author.clone(), sha.clone());
            let task = tokio::spawn(async move {
                let result = fetch_index(consumer, &ticket, &fetch_sha).await.map_err(|error| format!("{error:#}"));
                let _ = tx.send(IndexDone { author: fetch_author, sha: fetch_sha, result });
            });
            self.index_tasks.insert(author, (sha, task.abort_handle()));
        }
    }

    /// Take the end of one index fetch. A landed index is cached, and the view is read again. A failed one is logged.
    async fn index_done(&mut self, done: IndexDone) {
        self.index_pending.remove(&(done.author.clone(), done.sha.clone()));
        if self.index_tasks.get(&done.author).is_some_and(|(sha, _)| *sha == done.sha) {
            self.index_tasks.remove(&done.author);
        }
        match done.result {
            Ok(entries) if current_pointer(&self.pointers, &done.author, &done.sha) => {
                self.indexes.0.insert(done.author, (done.sha, entries));
                self.reconcile().await;
            }
            // The pointer moved on, and the fetch of the newer pointer is the one that lands.
            Ok(_) => {}
            Err(error) => self.failed(&format!("index of {} failed: {error}", done.author)),
        }
    }

    async fn merge(&self, merge: Value) -> Result<()> {
        self.member
            .request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply }))
            .await
            .and_then(|sent| sent.map_err(|error| error.to_string()))
            .map_err(|error| anyhow!("{error}"))
    }

    async fn reconcile(&mut self) {
        let Ok(text) = self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await else {
            return;
        };
        if let Ok(document) = serde_json::from_str::<Value>(&text) {
            self.fetch_indexes(&document);
            self.apply(&document);
            self.status.synced_at = Some(status::now());
        }
    }

    /// Make the disk match the document, for every path the document or `known` names.
    fn apply(&mut self, document: &Value) {
        // The own slice is this peer's own truth. Its fetched index can lag the own publishes, so the own entries
        // are read from `own` instead, and the older copy never wins over a newer own edit.
        if let Some(sha) = self.index_sha.clone()
            && let Some(author) = document.as_object().and_then(|slices| {
                slices.iter().find_map(|(author, slice)| {
                    let pointer = slice.get("index")?.get("sha256")?.as_str()?;
                    (pointer == sha).then(|| author.clone())
                })
            })
        {
            let entries = self.own.iter().map(|(path, entry)| (path.clone(), entry.clone())).collect();
            self.indexes.0.insert(author, (sha, entries));
        }
        let view = view_of(document, &self.indexes);
        self.winner_ts = view
            .iter()
            .map(|(path, entry)| (path.clone(), entry.get("ts").and_then(Value::as_u64).unwrap_or(0)))
            .collect();
        let tracked: Vec<String> = self
            .known
            .keys()
            .cloned()
            .chain(self.downloads.gate.wanted_paths())
            .chain(self.downloads.gate.landed_paths())
            .collect();
        let removed = removed_by_tombstone(document, &self.indexes, &tracked);
        let winners = winners_of(document, &self.indexes);
        for path in &removed {
            if let Some(tombstone) = winners.get(path)
                && self.own.get(path) != Some(tombstone)
            {
                self.own.insert(path.clone(), tombstone.clone());
                self.own_changed();
            }
        }
        // The paths that the view names, plus each tracked path that was not removed. The removal helpers read this map.
        let mut named = view.clone();
        for path in tracked.iter().filter(|path| !removed.contains(path)) {
            named.entry(path.clone()).or_insert(Value::Null);
        }
        let entries: Vec<Entry> = view
            .iter()
            .filter_map(|(path, entry)| {
                let (Some(sha), Some(ticket)) = (entry["sha256"].as_str(), entry["ticket"].as_str()) else {
                    return None;
                };
                Some((path.clone(), sha.to_owned(), ticket.to_owned()))
            })
            .collect();
        let consumer = self.consumer.clone();
        self.downloads
            .reconcile(&entries, &self.known, move |ticket, progress| fetch_blob(consumer.clone(), ticket, progress));
        self.downloads.prune(&named);
        for path in self.downloads.remove_dropped(&named, &mut self.known, |local| std::fs::remove_file(local)) {
            self.status.record("↓", &path, true);
        }
    }

    /// Take the end of one download. A sha that the document no longer names is dropped.
    async fn finish(&mut self, done: Finished) {
        if !self.downloads.settle(&done) {
            return;
        }
        let Finished { path, sha, result, .. } = done;
        if self.downloads.gate.wanted_sha(&path).as_deref() != Some(sha.as_str()) {
            return;
        }
        let published = match result {
            // Serve the file too, so a peer can still get it after its author left.
            // A served copy keeps the ts of the entry it came from, so an older copy never beats a newer edit.
            Ok(()) => {
                let ts = self.winner_ts.get(&path).copied().unwrap_or_else(now_ms);
                let published = self.publish(&path, &sha, ts).await;
                self.save_own();
                published
            }
            Err(Failure::Transient(error) | Failure::Permanent(error) | Failure::LocalWrite(error)) => Err(error),
        };
        if published.is_ok() {
            self.status.record("↓", &path, false);
        } else {
            self.status.failed += 1;
        }
        mark_landed(&mut self.known, path, sha, published);
    }
}

/// The paths in `tracked` that the document does not name.
fn unnamed(tracked: Vec<String>, files: &serde_json::Map<String, Value>) -> Vec<String> {
    tracked.into_iter().filter(|path| !files.contains_key(path)).collect()
}

/// Record a landed file in `known`, so the next reconcile does not fetch it again.
/// A file that failed to publish is not recorded, so the next reconcile tries again.
fn mark_landed(known: &mut HashMap<String, String>, path: String, sha: String, published: Result<()>) {
    match published {
        Ok(()) => {
            eprintln!("↓ {path}");
            known.insert(path, sha);
        }
        Err(error) => eprintln!("download {path} failed: {error:#}"),
    }
}

/// Fetch the blob that `ticket` names, check it against `sha`, and write it to `local`.
/// Returns `true` when the caller must publish the file, which is also when it already matched `sha`:
/// a peer that gets the file from its author would otherwise never serve it.
async fn download_file<Fut>(
    local: &Path,
    path: &str,
    sha: &str,
    ticket: &str,
    gate: &Gate,
    mut fetch: impl FnMut(String, Progress) -> Fut,
) -> Result<(), Failure>
where
    Fut: Future<Output = Result<Vec<u8>>>,
{
    if file_sha(local).as_deref() == Some(sha) {
        return Ok(());
    }
    let mut attempt = 0;
    let bytes = loop {
        let progress = Progress::default();
        let outcome = match guarded(fetch(ticket.to_owned(), progress.clone()), &progress).await {
            Some(outcome) => outcome,
            None => Err(anyhow!("the transfer went quiet")),
        };
        match outcome {
            Ok(bytes) => break bytes,
            Err(error) if error.is::<BadTicket>() => return Err(Failure::Permanent(error)),
            Err(error) => {
                let wait = backoff(attempt);
                tracing::debug!(path = %local.display(), attempt = attempt.saturating_add(1), wait = ?wait, error = %error_text(&error), "retrying a blob fetch");
                tokio::time::sleep(wait).await;
                // A newer ticket for this sha has its own download, so this one stops before its next attempt.
                if !gate.is_latest(path, sha, ticket) {
                    return Err(Failure::Transient(anyhow!("{path} names another ticket now")));
                }
                attempt = attempt.saturating_add(1);
            }
        }
    };
    if sha256_bytes(&bytes) != sha {
        return Err(Failure::Permanent(anyhow!("content does not match its sha256")));
    }
    // A local write error, such as a full disk or a folder where the file goes, repeats until the entry changes.
    if !gate.write(local, path, sha, bytes).await.map_err(Failure::LocalWrite)? {
        return Err(Failure::Transient(anyhow!("{path} was replaced before its write")));
    }
    Ok(())
}

/// A failed fetch, which keeps the cause chain of its blob error.
fn fetch_error(error: BlobError) -> anyhow::Error {
    anyhow::Error::new(error)
}

/// The own entries after a restart: the saved ones, with each file that is gone from disk turned into a tombstone.
/// A tombstone is kept, so a file deleted before the restart stays deleted for the mesh.
fn own_after_restart(
    saved: BTreeMap<String, Value>,
    disk: &BTreeMap<String, String>,
    now: u64,
) -> BTreeMap<String, Value> {
    saved
        .into_iter()
        .map(|(path, entry)| {
            if is_tombstone(&entry) || disk.contains_key(&path) {
                (path, entry)
            } else {
                (path, json!({ "deleted": true, "ts": now }))
            }
        })
        .collect()
}

/// The own entries in a saved index file, when it belongs to `mesh`. A file of another mesh is ignored.
fn load_saved(bytes: &[u8], mesh: &str) -> BTreeMap<String, Value> {
    let Ok(Value::Object(saved)) = serde_json::from_slice::<Value>(bytes) else {
        return BTreeMap::new();
    };
    if saved.get("mesh").and_then(Value::as_str) != Some(mesh) {
        return BTreeMap::new();
    }
    saved.get("own").cloned().and_then(|own| serde_json::from_value(own).ok()).unwrap_or_default()
}

/// How far a remote ts may run ahead of the local clock. An entry past it is ignored until the clock reaches it.
const TS_REACH: u64 = 10 * 60 * 1000;

/// Whether `sha` is the pointer that `author` has now.
fn current_pointer(pointers: &HashMap<String, String>, author: &str, sha: &str) -> bool {
    pointers.get(author).is_some_and(|current| current == sha)
}

/// The own index is kept here between runs, as a hidden file, which the scan skips.
const INDEX_FILE: &str = ".agent-knowledge-index.json";

/// The largest index blob that is fetched or published. A larger one is refused with an error, never truncated.
const INDEX_MAX_BYTES: usize = 16 * 1024 * 1024;
/// How many times an index fetch is retried before it is given up for now. The pointer stays, so it is tried again.
const INDEX_ATTEMPTS: u32 = 6;
/// The least time between two own index writes. Many edits in this time give one index, and one pointer write.
const INDEX_INTERVAL: Duration = Duration::from_secs(2);

/// The entries of each author's index, with the sha256 of the index they came from.
#[derive(Default)]
struct Indexes(HashMap<String, (String, serde_json::Map<String, Value>)>);

/// The index that an author's own slice points at, as (author, ticket, sha256), when it is not fetched yet.
fn pointers_to_fetch(document: &Value, indexes: &Indexes) -> Vec<(String, String, String)> {
    document
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(author, slice)| {
            let pointer = slice.get("index")?;
            let (ticket, sha) = (pointer.get("ticket")?.as_str()?, pointer.get("sha256")?.as_str()?);
            let fetched = indexes.0.get(author).is_some_and(|(fetched, _)| fetched == sha);
            (!fetched).then(|| (author.clone(), ticket.to_owned(), sha.to_owned()))
        })
        .collect()
}

/// The index fetches to start, and the authors whose running fetch is cancelled. A running fetch that is for the
/// author's current pointer stays. A fetch for an older pointer is cancelled, and the new one starts at once.
fn fetch_plan(
    document: &Value,
    indexes: &Indexes,
    running: &HashMap<String, String>,
) -> (Vec<(String, String, String)>, Vec<String>) {
    let mut starts = Vec::new();
    let mut cancels = Vec::new();
    for (author, ticket, sha) in pointers_to_fetch(document, indexes) {
        match running.get(&author) {
            Some(running_sha) if *running_sha == sha => {}
            Some(_) => {
                cancels.push(author.clone());
                starts.push((author, ticket, sha));
            }
            None => starts.push((author, ticket, sha)),
        }
    }
    (starts, cancels)
}

/// The entries of an index blob, checked against its sha256 and the cap.
fn check_index(bytes: &[u8], sha: &str) -> Result<serde_json::Map<String, Value>> {
    if bytes.len() > INDEX_MAX_BYTES {
        bail!("index refused: {} bytes is over the {INDEX_MAX_BYTES}-byte cap", bytes.len());
    }
    if sha256_bytes(bytes) != sha {
        bail!("index refused: its content does not match its sha256");
    }
    match serde_json::from_slice::<Value>(bytes)? {
        Value::Object(entries) => Ok(entries),
        _ => bail!("index refused: it is not an object"),
    }
}

/// The entries of each author slice whose index is fetched and matches the pointer. A slice with no fetched index
/// holds no data.
fn live_slices<'a>(
    document: &'a Value,
    indexes: &'a Indexes,
) -> impl Iterator<Item = (&'a String, &'a serde_json::Map<String, Value>)> + 'a {
    document.as_object().into_iter().flatten().filter_map(move |(author, slice)| {
        let pointer = slice.get("index")?.get("sha256")?.as_str()?;
        let (fetched, entries) = indexes.0.get(author)?;
        (fetched == pointer).then_some((author, entries))
    })
}

/// The winning entry of each path across the live slices. An entry is a file or a tombstone (`deleted: true`). The larger
/// `ts` wins, and a tie goes to the larger author key. An entry with no `ts` counts as 0.
fn winners_of(document: &Value, indexes: &Indexes) -> serde_json::Map<String, Value> {
    let mut best: HashMap<String, (u64, String, Value)> = HashMap::new();
    let reach = now_ms() + TS_REACH;
    for (author, entries) in live_slices(document, indexes) {
        for (path, entry) in entries {
            let ts = entry.get("ts").and_then(Value::as_u64).unwrap_or(0);
            if ts > reach {
                continue;
            }
            let beats = best
                .get(path)
                .is_none_or(|(old_ts, old_author, _)| (ts, author.as_str()) > (*old_ts, old_author.as_str()));
            if beats {
                best.insert(path.clone(), (ts, author.clone(), entry.clone()));
            }
        }
    }
    best.into_iter().map(|(path, (_, _, entry))| (path, entry)).collect()
}

/// The files that the document names: the winners that are not tombstones.
fn view_of(document: &Value, indexes: &Indexes) -> serde_json::Map<String, Value> {
    winners_of(document, indexes).into_iter().filter(|(_, entry)| !is_tombstone(entry)).collect()
}

fn is_tombstone(entry: &Value) -> bool {
    entry.get("deleted").and_then(Value::as_bool) == Some(true)
}

/// The tracked paths whose winning entry is a tombstone. A path that no live slice lists is kept.
fn removed_by_tombstone(document: &Value, indexes: &Indexes, tracked: &[String]) -> Vec<String> {
    let winners = winners_of(document, indexes);
    tracked.iter().filter(|path| winners.get(*path).is_some_and(is_tombstone)).cloned().collect()
}

/// One index fetch that ended: the author, the sha256 it was pointed at, and its entries or the reason it failed.
struct IndexDone {
    author: String,
    sha: String,
    result: Result<serde_json::Map<String, Value>, String>,
}

/// Fetch an index blob, and check it. A fetch that fails is retried with the same backoff as a file.
async fn fetch_index(
    consumer: Arc<Consumer<Endpoint>>,
    ticket: &str,
    sha: &str,
) -> Result<serde_json::Map<String, Value>> {
    let mut attempt = 0;
    loop {
        let fetched = tokio::time::timeout(
            ATTEMPT_DEADLINE,
            fetch_blob(consumer.clone(), ticket.to_owned(), Progress::default()),
        )
        .await;
        match fetched {
            Ok(Ok(bytes)) => return check_index(&bytes, sha),
            Ok(Err(error)) if error.is::<BadTicket>() => return Err(error),
            _ if attempt >= INDEX_ATTEMPTS => bail!("index fetch gave up after {attempt} retries"),
            _ => {}
        }
        tokio::time::sleep(backoff(attempt)).await;
        attempt += 1;
    }
}

/// The wall clock in milliseconds since the epoch: the `ts` of an entry.
fn now_ms() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()).unwrap_or(u64::MAX)
}

/// The text of a fetch error with its whole chain: each cause follows the one before it after ": ".
fn error_text(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

/// The first blob retry waits this long. Each retry doubles the wait, up to `BLOB_RETRY_CAP`.
const BLOB_RETRY_BASE: Duration = Duration::from_secs(1);
/// The longest wait between two blob retries, before jitter.
const BLOB_RETRY_CAP: Duration = Duration::from_secs(10);

/// The wait before retry `attempt`: `BLOB_RETRY_BASE` doubling up to `BLOB_RETRY_CAP`, plus up to half of that as jitter.
fn backoff(attempt: u32) -> Duration {
    let base = BLOB_RETRY_BASE.saturating_mul(2u32.saturating_pow(attempt)).min(BLOB_RETRY_CAP);
    let half_ms = u64::try_from((base / 2).as_millis()).unwrap_or(u64::MAX);
    let nanos = u64::from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos());
    base + Duration::from_millis(nanos % (half_ms + 1))
}

/// A ticket that does not decode. No retry can fix it.
#[derive(Debug)]
struct BadTicket(String);

impl std::fmt::Display for BadTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad ticket: {}", self.0)
    }
}

impl std::error::Error for BadTicket {}

/// The lookups of every blob endpoint. The server and the consumers must share them, so a ticket that one side
/// publishes is found by the other side.
fn blob_lookups() -> LookupOpts {
    LookupOpts::public_preset()
}

/// What the consumer needs from an endpoint: whether it has closed, and how to close it.
trait Closable {
    fn closed(&self) -> bool;
    fn close_now(&self) -> impl Future<Output = ()> + Send;
}

impl Closable for Endpoint {
    fn closed(&self) -> bool {
        self.is_closed()
    }

    async fn close_now(&self) {
        self.close().await;
    }
}

/// A build error that a hedged fetch can report. It carries only the message.
#[derive(Debug)]
struct BuildFailed(String);

impl std::fmt::Display for BuildFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BuildFailed {}

/// Holds a build's endpoint until the caller reads it. If the caller is gone first, the endpoint is closed here.
struct Undelivered<E: Closable + Send + 'static>(Option<tokio::sync::oneshot::Receiver<Result<E, BuildFailed>>>);

impl<E: Closable + Send + 'static> Undelivered<E> {
    async fn wait(&mut self) -> Result<E, BuildFailed> {
        let Some(receiver) = self.0.as_mut() else {
            return Err(BuildFailed("the build was already read".to_owned()));
        };
        let received = receiver.await.unwrap_or_else(|_| Err(BuildFailed("the build task ended".to_owned())));
        self.0 = None;
        received
    }
}

impl<E: Closable + Send + 'static> Drop for Undelivered<E> {
    fn drop(&mut self) {
        let Some(mut receiver) = self.0.take() else {
            return;
        };
        // Stops a late send from landing unread: the sender then closes the endpoint itself.
        receiver.close();
        if let Ok(Ok(endpoint)) = receiver.try_recv() {
            tokio::spawn(async move { endpoint.close_now().await });
        }
    }
}

/// Runs `build` in its own task. A caller that drops the returned future does not leave the built endpoint open:
/// the task closes it. iroh logs an error for an endpoint dropped without a close, and a hedged fetch drops its losing build.
async fn detach_build<E, Fut, Err>(build: Fut) -> Result<E, BuildFailed>
where
    E: Closable + Send + 'static,
    Fut: Future<Output = Result<E, Err>> + Send + 'static,
    Err: std::fmt::Display,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let built = build.await.map_err(|error| BuildFailed(error.to_string()));
        // The receiver is gone when the caller was cancelled, so nobody will use this endpoint.
        if let Err(Ok(endpoint)) = sender.send(built) {
            endpoint.close_now().await;
        }
    });
    Undelivered(Some(receiver)).wait().await
}

/// How long a failed endpoint build is remembered. The waiting downloads get the same error until then.
const BUILD_RETRY_WAIT: Duration = Duration::from_secs(5);

/// How often a retired endpoint checks whether its last lease has ended.
const RETIRE_POLL: Duration = Duration::from_secs(1);

/// The endpoint that the blob fetches of one folder use. It is built on first use, and again after it closes or stalls.
struct Consumer<E> {
    state: tokio::sync::Mutex<ConsumerState<E>>,
    /// Set on shutdown. A download that starts after it must not build an endpoint.
    closed: AtomicBool,
}

struct ConsumerState<E> {
    endpoint: Option<E>,
    /// Counts the builds. A stall reports the generation it used, so only the first stall of an endpoint replaces it.
    generation: u64,
    /// The leases that hold the endpoint of `generation`.
    leases: Arc<AtomicUsize>,
    last_failure: Option<(tokio::time::Instant, String)>,
    /// Endpoints that stalled and are waiting for their leases to end. Shutdown closes them too.
    retired: Vec<E>,
}

/// The endpoint a download uses. It counts as held until it is dropped.
struct Lease<E> {
    endpoint: E,
    generation: u64,
    leases: Arc<AtomicUsize>,
}

impl<E> Drop for Lease<E> {
    fn drop(&mut self) {
        self.leases.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<E: Clone + Closable + Send + 'static> Consumer<E> {
    fn new() -> Self {
        let state = ConsumerState {
            endpoint: None,
            generation: 0,
            leases: Arc::new(AtomicUsize::new(0)),
            retired: Vec::new(),
            last_failure: None,
        };
        Self { state: tokio::sync::Mutex::new(state), closed: AtomicBool::new(false) }
    }

    async fn get<Fut>(&self, build: impl FnOnce() -> Fut) -> Result<Lease<E>>
    where
        Fut: Future<Output = Result<E>>,
    {
        let mut state = self.state.lock().await;
        // Read under the lock: take() sets the flag before it takes the lock, so a build here is never left unclosed.
        if self.closed.load(Ordering::SeqCst) {
            bail!("the folder is shutting down");
        }
        if let Some(endpoint) = state.endpoint.as_ref().filter(|endpoint| !endpoint.closed()) {
            return Ok(lease_of(&state, endpoint.clone()));
        }
        if let Some((failed_at, message)) = &state.last_failure
            && failed_at.elapsed() < BUILD_RETRY_WAIT
        {
            bail!("{message}");
        }
        match build().await {
            Ok(endpoint) => {
                state.generation += 1;
                state.leases = Arc::new(AtomicUsize::new(0));
                state.endpoint = Some(endpoint.clone());
                state.last_failure = None;
                Ok(lease_of(&state, endpoint))
            }
            Err(error) => {
                state.last_failure = Some((tokio::time::Instant::now(), format!("{error:#}")));
                Err(error)
            }
        }
    }

    /// The endpoint of `generation` stalled. The next get() builds a new one. The old endpoint closes when its last
    /// lease ends, so a transfer in flight on it is not cut. A stall from an older generation does nothing: a newer
    /// endpoint is already in place.
    async fn stalled(&self, generation: u64) {
        let mut state = self.state.lock().await;
        if state.generation != generation {
            return;
        }
        if let Some(endpoint) = state.endpoint.take() {
            state.retired.retain(|retired| !retired.closed());
            state.retired.push(endpoint.clone());
            tokio::spawn(close_when_idle(endpoint, state.leases.clone()));
        }
    }

    /// Shutdown: no new endpoint is built, and every endpoint, current or retired, is closed before this returns.
    async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let mut state = self.state.lock().await;
        let mut endpoints: Vec<E> = state.retired.drain(..).collect();
        endpoints.extend(state.endpoint.take());
        drop(state);
        for endpoint in endpoints {
            endpoint.close_now().await;
        }
    }
}

fn lease_of<E: Clone>(state: &ConsumerState<E>, endpoint: E) -> Lease<E> {
    state.leases.fetch_add(1, Ordering::SeqCst);
    Lease { endpoint, generation: state.generation, leases: state.leases.clone() }
}

/// Close `endpoint` once no lease holds it. A lease always ends, because `guarded` cuts a transfer that goes quiet.
async fn close_when_idle<E: Closable>(endpoint: E, leases: Arc<AtomicUsize>) {
    while leases.load(Ordering::SeqCst) > 0 {
        tokio::time::sleep(RETIRE_POLL).await;
    }
    endpoint.close_now().await;
}

async fn fetch_blob(consumer: Arc<Consumer<Endpoint>>, ticket: String, progress: Progress) -> Result<Vec<u8>> {
    let ticket = BlobTicket::decode(&ticket).map_err(|error| anyhow::Error::new(BadTicket(error.to_string())))?;
    let lease =
        consumer.get(|| async { build_peer_endpoint(&blob_lookups()).await.map_err(anyhow::Error::new) }).await?;
    let mut bytes = Collected { bytes: Vec::new(), progress };
    let lookups = blob_lookups();
    let build = || {
        let lookups = lookups.clone();
        detach_build(async move { build_peer_endpoint(&lookups).await })
    };
    match fetch_hedged(&lease.endpoint, build, &ticket, &mut bytes, None).await {
        Ok(()) => Ok(bytes.bytes),
        // Checked before the error becomes a string, so the stall reaches the consumer, which retires its endpoint.
        Err(BlobError::HandshakeStalled) => {
            consumer.stalled(lease.generation).await;
            tracing::debug!(
                generation = lease.generation,
                "blob handshake stalled; the next attempt builds an endpoint"
            );
            Err(anyhow!("blob handshake stalled"))
        }
        Err(error) => Err(fetch_error(error)),
    }
}

/// The writes of one folder, and the sha that the document names for each path.
/// Writes run one at a time, and each re-checks its sha under the lock, so the newest entry writes last.
#[derive(Clone, Default)]
struct Gate {
    turn: Arc<Mutex<()>>,
    wanted: Arc<Mutex<HashMap<String, (String, String)>>>,
    /// The sha that the last download wrote to each path. It holds only while the disk still has that sha.
    landed: Arc<Mutex<HashMap<String, String>>>,
}

impl Gate {
    fn set_wanted(&self, path: &str, sha: &str, ticket: &str) {
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.to_owned(), (sha.to_owned(), ticket.to_owned()));
    }

    /// Whether the document still names this entry, and not another ticket for the same content.
    fn is_latest(&self, path: &str, sha: &str, ticket: &str) -> bool {
        self.wanted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .is_some_and(|(s, t)| s == sha && t == ticket)
    }

    fn unwant(&self, path: &str) {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).remove(path);
    }

    fn wanted_sha(&self, path: &str) -> Option<String> {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).get(path).map(|(sha, _)| sha.clone())
    }

    fn wanted_paths(&self) -> Vec<String> {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect()
    }

    fn record_landed(&self, path: &str, sha: &str) {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner).insert(path.to_owned(), sha.to_owned());
    }

    fn landed_sha(&self, path: &str) -> Option<String> {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner).get(path).cloned()
    }

    fn landed_paths(&self) -> Vec<String> {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect()
    }

    fn forget_landed(&self, path: &str) {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner).remove(path);
    }

    /// Keeps a record only while the disk still holds the sha that it records.
    fn keep_landed(&self, disk: &BTreeMap<String, String>) {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner).retain(|path, sha| disk.get(path) == Some(sha));
    }

    /// Write `bytes` to `local`, unless the document no longer names `sha` for `path`.
    /// The write runs on a blocking thread, so an aborted download cannot stop it halfway.
    async fn write(&self, local: &Path, path: &str, sha: &str, bytes: Vec<u8>) -> Result<bool> {
        let gate = self.clone();
        let (local, path, sha) = (local.to_path_buf(), path.to_owned(), sha.to_owned());
        tokio::task::spawn_blocking(move || {
            let _turn = gate.turn.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.wanted_sha(&path).as_deref() != Some(sha.as_str()) {
                return Ok(false);
            }
            write_file(&local, &bytes)?;
            gate.record_landed(&path, &sha);
            Ok(true)
        })
        .await
        .context("write task ended")?
    }
}

/// Write through a hidden sibling with its own name, then rename it, so an editor never sees half a file.
/// The scanner skips hidden names, so the temp file never gets published.
fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let name = path.file_name().context("no file name")?.to_string_lossy();
    let temp = path.with_file_name(format!(".{name}.{}.agent-knowledge", NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::write(&temp, bytes)?;
    if let Err(error) = std::fs::rename(&temp, path) {
        let _ = std::fs::remove_file(&temp);
        return Err(error.into());
    }
    Ok(())
}

fn file_sha(path: &Path) -> Option<String> {
    std::fs::read(path).ok().map(|bytes| sha256_bytes(&bytes))
}

/// Wait for the membership `ready` event and return the mesh id and this peer's nick.
/// Fails if the mesh stops first or the event lacks either field.
async fn wait_ready(events: &mut mpsc::UnboundedReceiver<String>) -> Result<Ready> {
    while let Some(event) = events.recv().await {
        let event: Value = serde_json::from_str(&event)?;
        if event["kind"] == "ready" {
            let field = |name: &str| {
                event[name].as_str().map(str::to_owned).with_context(|| format!("ready event without a {name}"))
            };
            return Ok(Ready { mesh: field("mesh")?, nick: field("nick")? });
        }
    }
    bail!("the mesh stopped before it was ready")
}

/// A `joined` or `left` event as a [`Presence`]. Every other kind, `quiet` and `returned`
/// included, is `None`.
fn presence(event: &Value) -> Option<Presence> {
    let presence = match event["kind"].as_str()? {
        "joined" => Presence::Joined,
        "left" => Presence::Left,
        _ => return None,
    };
    Some(presence(event["nick"].as_str()?.to_owned()))
}

/// Whether `event` is another peer's change to the shared `state` document: the cue to reconcile.
fn is_remote_state_change(event: &Value) -> bool {
    event["kind"] == "state_changed" && event["channel"] == "state" && event["is_self"] == false
}

fn spool_dir() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let instance = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("agent-knowledge-{}-{instance}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test(start_paused = true)]
    async fn a_fetch_slower_than_the_cap_still_completes() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);

        let result = download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_, _| async {
            tokio::time::sleep(Duration::from_secs(40)).await;
            Ok(bytes.to_vec())
        })
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(std::fs::read(&local).unwrap(), bytes);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stuck_download_does_not_hold_back_the_next_file() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![
            ("stuck.md".to_owned(), sha.clone(), "stuck".to_owned()),
            ("start.md".to_owned(), sha.clone(), "start".to_owned()),
        ];
        let fetch = move |ticket: String, _: Progress| async move {
            if ticket == "stuck" {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(std::fs::read(dir.path().join("start.md")).ok(), Some(bytes.to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_bad_blob_is_not_fetched_again_for_the_same_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<Vec<u8>, anyhow::Error>(b"tampered".to_vec())
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch.clone());
        let settled = finished.recv().await.unwrap();
        downloads.settle(&settled);
        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_path_in_flight_is_not_started_twice() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "hangs".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<Vec<u8>>>().await
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch.clone());
        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_changed_sha_stops_the_old_retry() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let new_bytes = b"from author";
        let new_sha = sha256_bytes(new_bytes);
        let old_calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let old_calls = old_calls.clone();
            move |ticket: String, _: Progress| {
                let old_calls = old_calls.clone();
                async move {
                    if ticket == "old" {
                        old_calls.fetch_add(1, Ordering::SeqCst);
                        return Err(anyhow!("dead source"));
                    }
                    Ok(new_bytes.to_vec())
                }
            }
        };
        let old: Vec<Entry> = vec![("start.md".to_owned(), "0".repeat(64), "old".to_owned())];
        let new: Vec<Entry> = vec![("start.md".to_owned(), new_sha, "new".to_owned())];

        downloads.reconcile(&old, &HashMap::new(), fetch.clone());
        tokio::time::sleep(Duration::from_secs(10)).await;
        downloads.reconcile(&new, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(10)).await;
        let after_change = old_calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(old_calls.load(Ordering::SeqCst), after_change);
        assert_eq!(std::fs::read(dir.path().join("start.md")).ok(), Some(new_bytes.to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_fetch_is_cut_off_and_tried_again() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "hangs".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<Vec<u8>>>().await
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(ATTEMPT_DEADLINE + Duration::from_secs(5)).await;

        assert!(calls.load(Ordering::SeqCst) >= 2, "calls {}", calls.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_publish_is_retried_by_the_next_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        std::fs::write(dir.path().join("start.md"), bytes).unwrap();
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let fetch = |_: String, _: Progress| async { Ok::<Vec<u8>, anyhow::Error>(Vec::new()) };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let first = finished.recv().await.unwrap();
        downloads.settle(&first);
        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let second = finished.recv().await.unwrap();

        assert!(first.result.is_ok());
        assert!(second.result.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a_finished_download_is_reported_while_another_is_stuck() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![
            ("stuck.md".to_owned(), sha.clone(), "stuck".to_owned()),
            ("start.md".to_owned(), sha.clone(), "start".to_owned()),
        ];
        let fetch = move |ticket: String, _: Progress| async move {
            if ticket == "stuck" {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let first =
            tokio::time::timeout(Duration::from_secs(1), finished.recv()).await.expect("a result").expect("open");

        assert_eq!(first.path, "start.md");
    }

    #[tokio::test(start_paused = true)]
    async fn abort_all_stops_a_retrying_download() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "dead".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err::<Vec<u8>, anyhow::Error>(anyhow!("dead source"))
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(10)).await;
        downloads.abort_all();
        let at_shutdown = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(calls.load(Ordering::SeqCst), at_shutdown);
    }

    #[test]
    fn forget_drops_what_the_document_named() {
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(PathBuf::from("."), done);
        downloads.gate.set_wanted("start.md", &"0".repeat(64), "ticket");

        downloads.forget("start.md");

        assert!(downloads.gate.wanted_paths().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_ticket_that_does_not_decode_is_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let sha = sha256_bytes(b"from author");

        let consumer = Arc::new(Consumer::<Endpoint>::new());
        let outcome = tokio::time::timeout(
            Duration::from_secs(3600),
            download_file(
                &local,
                "start.md",
                &sha,
                "not a ticket",
                &gate_wanting("start.md", &sha),
                move |ticket, progress| fetch_blob(consumer.clone(), ticket, progress),
            ),
        )
        .await;

        assert!(matches!(outcome, Ok(Err(Failure::Permanent(_)))), "{outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_ticket_for_the_same_sha_replaces_the_one_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let fetch = move |ticket: String, _: Progress| async move {
            if ticket == "dead" {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };
        let dead: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "dead".to_owned())];
        let live: Vec<Entry> = vec![("start.md".to_owned(), sha, "live".to_owned())];

        downloads.reconcile(&dead, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;
        downloads.reconcile(&live, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(std::fs::read(dir.path().join("start.md")).ok(), Some(bytes.to_vec()));
    }

    #[test]
    fn a_late_result_for_an_old_entry_does_not_clear_a_current_failure() {
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(PathBuf::from("."), done);
        downloads.failed.insert("start.md".to_owned(), ("same".to_owned(), "t2".to_owned()));

        downloads.settle(&Finished {
            path: "start.md".to_owned(),
            sha: "same".to_owned(),
            ticket: "t1".to_owned(),
            result: Ok(()),
        });

        assert!(downloads.failed.contains_key("start.md"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_local_write_error_is_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        // A folder where the file should go makes every write fail.
        std::fs::create_dir(&local).unwrap();
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);

        let outcome = tokio::time::timeout(
            Duration::from_secs(3600),
            download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_, _| async {
                Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
            }),
        )
        .await;

        assert!(matches!(outcome, Ok(Err(Failure::LocalWrite(_)))), "{outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_fetch_still_reports_back() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let fetch = |_: String, _: Progress| async {
            panic!("the fetch broke");
            #[allow(unreachable_code)]
            Ok::<Vec<u8>, anyhow::Error>(Vec::new())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let reported = tokio::time::timeout(Duration::from_secs(1), finished.recv()).await;

        assert!(matches!(reported, Ok(Some(_))), "no result within a second");
    }

    #[tokio::test(start_paused = true)]
    async fn a_forgotten_path_is_not_written_by_a_retry_that_later_works() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "flaky".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(anyhow!("dead source"));
                    }
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;
        downloads.forget("start.md");
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(!dir.path().join("start.md").exists());
    }

    /// What a `tracing` subscriber writes, so a test can read it back.
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;

        fn make_writer(&'a self) -> Captured {
            self.clone()
        }
    }

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    /// A gate in which the document names `sha` for `path`, as a download expects.
    fn gate_wanting(path: &str, sha: &str) -> Gate {
        let gate = Gate::default();
        gate.set_wanted(path, sha, "ticket");
        gate
    }

    #[tokio::test]
    async fn a_write_for_a_replaced_entry_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let gate = gate_wanting("start.md", &sha256_bytes(b"new bytes"));

        let wrote = gate.write(&local, "start.md", &sha256_bytes(b"old bytes"), b"old bytes".to_vec()).await;

        assert!(matches!(wrote, Ok(false)), "{wrote:?}");
        assert!(!local.exists());
    }

    /// Install a subscriber that keeps what `filter` lets through, for the rest of the test.
    fn capture(filter: &str) -> (Captured, tracing::subscriber::DefaultGuard) {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .finish();
        (captured, tracing::subscriber::set_default(subscriber))
    }

    /// A download whose first fetch fails, so the retry loop runs once.
    async fn download_after_one_failure(dir: &Path) {
        let local = dir.join("start.md");
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let mut calls = 0;
        let fetch = |_: String, _: Progress| {
            calls += 1;
            let attempt = calls;
            async move {
                if attempt < 2 {
                    return Err(anyhow!("dead source"));
                }
                Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
            }
        };
        download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), fetch).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_is_logged_at_debug() {
        let dir = tempfile::tempdir().unwrap();
        let (captured, _guard) = capture("agent_knowledge=debug");

        download_after_one_failure(dir.path()).await;

        let text = captured.text();
        assert!(text.contains("retrying a blob fetch"), "{text}");
        assert!(text.contains("attempt=1"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_is_silent_without_the_debug_filter() {
        let dir = tempfile::tempdir().unwrap();
        let (captured, _guard) = capture("error");

        download_after_one_failure(dir.path()).await;

        assert!(!captured.text().contains("retrying a blob fetch"), "{}", captured.text());
    }

    #[test]
    fn unnamed_keeps_only_the_paths_the_document_does_not_name() {
        let files = serde_json::json!({ "a.md": {} });
        let files = files.as_object().unwrap();

        let gone = unnamed(vec!["a.md".to_owned(), "b.md".to_owned(), "c.md".to_owned()], files);

        assert_eq!(gone, vec!["b.md".to_owned(), "c.md".to_owned()]);
    }

    #[test]
    fn a_landed_file_is_known_only_once_it_is_published() {
        let mut known = HashMap::new();

        mark_landed(&mut known, "start.md".to_owned(), "sha".to_owned(), Err(anyhow!("offline")));
        assert!(!known.contains_key("start.md"));

        mark_landed(&mut known, "start.md".to_owned(), "sha".to_owned(), Ok(()));
        assert_eq!(known.get("start.md").map(String::as_str), Some("sha"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_fetch_is_not_started_again() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("the fetch broke");
                    #[allow(unreachable_code)]
                    Ok::<Vec<u8>, anyhow::Error>(Vec::new())
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch.clone());
        let first = finished.recv().await.unwrap();
        downloads.settle(&first);
        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(10)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_result_for_an_older_ticket_is_not_current() {
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(PathBuf::from("."), done);
        downloads.running.insert(
            ("start.md".to_owned(), "t2".to_owned()),
            ("same".to_owned(), tokio::spawn(async {}).abort_handle()),
        );

        let current = downloads.settle(&Finished {
            path: "start.md".to_owned(),
            sha: "same".to_owned(),
            ticket: "t1".to_owned(),
            result: Ok(()),
        });

        assert!(!current);
    }

    #[tokio::test(start_paused = true)]
    async fn a_local_write_error_is_retried_after_a_minute() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        // A folder where the file goes makes the write fail, until the folder is removed.
        std::fs::create_dir(&local).unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let fetch = |_: String, _: Progress| async { Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec()) };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let first = finished.recv().await.unwrap();
        downloads.settle(&first);
        std::fs::remove_dir(&local).unwrap();
        tokio::time::sleep(Duration::from_secs(61)).await;
        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(std::fs::read(&local).ok(), Some(bytes.to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_ticket_change_does_not_restart_a_transfer_in_progress() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let fetch = move |ticket: String, _: Progress| async move {
            match ticket.as_str() {
                "slow" => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
                _ => std::future::pending::<Result<Vec<u8>>>().await,
            }
        };
        let slow: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "slow".to_owned())];
        let new: Vec<Entry> = vec![("start.md".to_owned(), sha, "new".to_owned())];

        downloads.reconcile(&slow, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;
        downloads.reconcile(&new, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(20)).await;

        assert_eq!(std::fs::read(dir.path().join("start.md")).ok(), Some(bytes.to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn an_older_ticket_stops_retrying_once_the_document_names_another() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let old_calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let old_calls = old_calls.clone();
            move |ticket: String, _: Progress| {
                let old_calls = old_calls.clone();
                async move {
                    if ticket == "old" {
                        old_calls.fetch_add(1, Ordering::SeqCst);
                        return Err(anyhow!("dead source"));
                    }
                    std::future::pending::<Result<Vec<u8>>>().await
                }
            }
        };
        let old: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "old".to_owned())];
        let new: Vec<Entry> = vec![("start.md".to_owned(), sha, "new".to_owned())];

        downloads.reconcile(&old, &HashMap::new(), fetch.clone());
        tokio::time::sleep(Duration::from_secs(10)).await;
        downloads.reconcile(&new, &HashMap::new(), fetch);
        // The old attempt may be in a backoff of up to a minute, so give it time to notice.
        tokio::time::sleep(Duration::from_secs(120)).await;
        let after_switch = old_calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(old_calls.load(Ordering::SeqCst), after_switch);
    }

    #[tokio::test(start_paused = true)]
    async fn a_path_the_document_drops_is_not_written_by_a_retry() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "flaky".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(anyhow!("dead source"));
                    }
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let document = serde_json::json!({ "files": {} });
        downloads.prune(document["files"].as_object().unwrap());
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(!dir.path().join("start.md").exists());
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_ticket_for_the_same_sha_does_not_skip_the_write_wait() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        // A folder where the file goes makes the write fail.
        std::fs::create_dir(&local).unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };
        let first: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "t1".to_owned())];
        let second: Vec<Entry> = vec![("start.md".to_owned(), sha, "t2".to_owned())];

        downloads.reconcile(&first, &HashMap::new(), fetch.clone());
        let failed = finished.recv().await.unwrap();
        downloads.settle(&failed);
        downloads.reconcile(&second, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_superseded_attempt_does_not_run_after_its_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let old_calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let old_calls = old_calls.clone();
            move |ticket: String, _: Progress| {
                let old_calls = old_calls.clone();
                async move {
                    if ticket == "old" {
                        old_calls.fetch_add(1, Ordering::SeqCst);
                        return Err(anyhow!("dead source"));
                    }
                    std::future::pending::<Result<Vec<u8>>>().await
                }
            }
        };
        let old: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "old".to_owned())];
        let new: Vec<Entry> = vec![("start.md".to_owned(), sha, "new".to_owned())];

        downloads.reconcile(&old, &HashMap::new(), fetch.clone());
        // The first attempt fails at once and waits about 2 s. The document names another ticket during that wait.
        tokio::time::sleep(Duration::from_secs(1)).await;
        downloads.reconcile(&new, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    }

    /// A stand-in endpoint: it closes when a test says so.
    #[derive(Clone, Default)]
    struct FakeEndpoint(Arc<std::sync::atomic::AtomicBool>);

    impl Closable for FakeEndpoint {
        async fn close_now(&self) {
            self.0.store(true, Ordering::SeqCst);
        }

        fn closed(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn many_downloads_build_the_endpoint_once() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };

        for _ in 0..3 {
            consumer.get(build).await.unwrap();
        }

        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_download_does_not_build_another_endpoint() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };

        consumer.get(build).await.unwrap();
        // A fetch that fails leaves the endpoint open, so the next download reuses it.
        consumer.get(build).await.unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_closed_endpoint_is_rebuilt_once() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };

        let first = consumer.get(build).await.unwrap();
        first.endpoint.0.store(true, Ordering::SeqCst);
        consumer.get(build).await.unwrap();
        consumer.get(build).await.unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_download_after_shutdown_does_not_build_an_endpoint() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };
        consumer.get(build).await.unwrap();

        consumer.shutdown().await;
        let late = consumer.get(build).await;

        assert!(late.is_err());
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_build_error_is_not_retried_at_once() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Err::<FakeEndpoint, anyhow::Error>(anyhow!("offline"))
        };

        let first = consumer.get(build).await;
        let second = consumer.get(build).await;

        assert!(first.is_err() && second.is_err());
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_build_is_tried_again_after_its_window() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Err::<FakeEndpoint, anyhow::Error>(anyhow!("offline"))
        };
        assert!(consumer.get(build).await.is_err());

        tokio::time::sleep(BUILD_RETRY_WAIT + Duration::from_secs(1)).await;
        assert!(consumer.get(build).await.is_err());

        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_stalled_endpoint_is_replaced_once_for_all_slots() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };
        let first = consumer.get(build).await.unwrap();

        consumer.stalled(first.generation).await;
        consumer.stalled(first.generation).await;
        consumer.get(build).await.unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_late_stall_does_not_replace_a_newer_endpoint() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let builds = AtomicUsize::new(0);
        let build = || async {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default())
        };
        let first = consumer.get(build).await.unwrap();
        consumer.stalled(first.generation).await;
        let second = consumer.get(build).await.unwrap();

        consumer.stalled(first.generation).await;
        consumer.get(build).await.unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_ne!(first.generation, second.generation);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_does_not_cut_a_transfer_in_flight() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let build = || async { Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default()) };
        let in_flight = consumer.get(build).await.unwrap();

        consumer.stalled(in_flight.generation).await;
        tokio::time::sleep(Duration::from_secs(30)).await;

        assert!(!in_flight.endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn the_old_endpoint_closes_after_its_last_lease() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let build = || async { Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default()) };
        let lease = consumer.get(build).await.unwrap();
        let endpoint = lease.endpoint.clone();
        consumer.stalled(lease.generation).await;

        drop(lease);
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_retired_endpoint_waits_for_its_own_leases_only() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let build = || async { Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default()) };
        let old = consumer.get(build).await.unwrap();
        let old_endpoint = old.endpoint.clone();
        consumer.stalled(old.generation).await;
        let new = consumer.get(build).await.unwrap();

        drop(old);
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(old_endpoint.closed());
        assert!(!new.endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn the_old_endpoint_stays_open_while_a_lease_moves() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let build = || async { Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default()) };
        let lease = consumer.get(build).await.unwrap();
        let endpoint = lease.endpoint.clone();
        consumer.stalled(lease.generation).await;

        tokio::time::sleep(ATTEMPT_DEADLINE + Duration::from_secs(5)).await;
        assert!(!endpoint.closed(), "closed under a live lease");

        drop(lease);
        tokio::time::sleep(RETIRE_POLL * 2).await;
        assert!(endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_closes_a_retired_endpoint_at_once() {
        let consumer = Consumer::<FakeEndpoint>::new();
        let build = || async { Ok::<FakeEndpoint, anyhow::Error>(FakeEndpoint::default()) };
        let lease = consumer.get(build).await.unwrap();
        let retired = lease.endpoint.clone();
        consumer.stalled(lease.generation).await;

        consumer.shutdown().await;

        assert!(retired.closed());
        drop(lease);
    }

    #[tokio::test(start_paused = true)]
    async fn a_build_dropped_while_running_closes_its_endpoint() {
        let endpoint = FakeEndpoint::default();
        let built = endpoint.clone();
        let build = async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<FakeEndpoint, anyhow::Error>(built)
        };

        let cancelled = tokio::time::timeout(Duration::from_millis(100), detach_build(build)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(cancelled.is_err());
        assert!(endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn an_endpoint_sent_but_never_read_is_closed() {
        let endpoint = FakeEndpoint::default();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let _ = sender.send(Ok::<FakeEndpoint, BuildFailed>(endpoint.clone()));

        drop(Undelivered(Some(receiver)));
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(endpoint.closed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_transfer_that_keeps_moving_is_not_cut_at_the_attempt_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "slow".to_owned())];
        let fetch = move |_: String, progress: Progress| async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_secs(30)).await;
                progress.record(1);
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let reported = tokio::time::timeout(Duration::from_secs(400), finished.recv()).await;

        assert!(matches!(reported, Ok(Some(ref done)) if done.result.is_ok()), "no successful result");
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_that_has_not_started_is_not_cut() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(Duration::from_secs(300)).await;
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };
        let gate = gate_wanting("start.md", &sha);

        let result = tokio::time::timeout(
            Duration::from_secs(400),
            download_file(&dir.path().join("start.md"), "start.md", &sha, "ticket", &gate, fetch),
        )
        .await;

        assert!(result.is_ok_and(|result| result.is_ok()), "the fetch was cut before it started");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_setup_with_no_byte_is_not_cut_before_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "slow".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(100)).await;
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let reported = tokio::time::timeout(Duration::from_secs(200), finished.recv()).await;

        assert!(matches!(reported, Ok(Some(ref done)) if done.result.is_ok()), "no successful result");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_download_is_not_retried_for_its_wait() {
        let dir = tempfile::tempdir().unwrap();
        let (captured, _guard) = capture("agent_knowledge=debug");
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let hog_sha = sha256_bytes(b"hog");
        let late_sha = sha256_bytes(b"late");
        let hogs: Vec<Entry> =
            (0..FETCH_SLOTS).map(|i| (format!("hog{i}.md"), hog_sha.clone(), format!("hog{i}"))).collect();
        let fetch = |ticket: String, progress: Progress| async move {
            if ticket.starts_with("hog") {
                for _ in 0..25 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    progress.record(1);
                }
                return Ok::<Vec<u8>, anyhow::Error>(b"hog".to_vec());
            }
            Ok(b"late".to_vec())
        };

        downloads.reconcile(&hogs, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut all = hogs.clone();
        all.push(("late.md".to_owned(), late_sha, "late".to_owned()));
        downloads.reconcile(&all, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(300)).await;

        assert_eq!(std::fs::read(dir.path().join("late.md")).unwrap(), b"late");
        assert!(!captured.text().contains("retrying a blob fetch"), "{}", captured.text());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_transfer_is_cut_after_its_idle_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "stuck".to_owned())];
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, progress: Progress| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    progress.record(1);
                    std::future::pending::<Result<Vec<u8>>>().await
                }
            }
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        tokio::time::sleep(Duration::from_secs(100)).await;

        assert!(calls.load(Ordering::SeqCst) >= 2, "attempts: {}", calls.load(Ordering::SeqCst));
    }

    /// Waits until `path` holds `bytes`. The write runs on a blocking thread, so the clock is polled, not waited on.
    async fn until_written(path: &Path, bytes: &[u8]) {
        let wait = async {
            while std::fs::read(path).ok().as_deref() != Some(bytes) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(60), wait).await.expect("the file never landed");
    }

    #[tokio::test(start_paused = true)]
    async fn a_restarted_peer_does_not_abort_the_download_it_needs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("start.md"), "old copy").unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"new copy");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "new".to_owned())];
        let fetch = |_: String, _: Progress| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<Vec<u8>, anyhow::Error>(b"new copy".to_vec())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        downloads.claim_edits(&HashMap::new()).unwrap();
        let reported = tokio::time::timeout(Duration::from_secs(60), finished.recv()).await;

        assert!(matches!(reported, Ok(Some(ref done)) if done.result.is_ok()), "the download was aborted");
        assert_eq!(std::fs::read(dir.path().join("start.md")).unwrap(), b"new copy");
    }

    #[tokio::test(start_paused = true)]
    async fn a_download_the_document_moved_past_is_not_aborted_by_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("start.md");
        std::fs::write(&path, "X").unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let known = HashMap::from([("start.md".to_owned(), sha256_bytes(b"X"))]);
        let entry = |sha: &str, ticket: &str| -> Vec<Entry> {
            vec![("start.md".to_owned(), sha.to_owned(), ticket.to_owned())]
        };
        let y_fetch = |_: String, _: Progress| async { Ok::<Vec<u8>, anyhow::Error>(b"Y".to_vec()) };
        let z_fetch = |_: String, _: Progress| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<Vec<u8>, anyhow::Error>(b"Z".to_vec())
        };

        downloads.reconcile(&entry(&sha256_bytes(b"Y"), "ty"), &known, y_fetch);
        until_written(&path, b"Y").await;
        downloads.reconcile(&entry(&sha256_bytes(b"Z"), "tz"), &known, z_fetch);
        downloads.claim_edits(&known).unwrap();

        let landed = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let done = finished.recv().await?;
                if done.sha == sha256_bytes(b"Z") && done.result.is_ok() {
                    return Some(());
                }
            }
        })
        .await;

        assert!(matches!(landed, Ok(Some(()))), "the scan aborted the download of Z");
        assert_eq!(std::fs::read(&path).unwrap(), b"Z");
    }

    #[tokio::test(start_paused = true)]
    async fn a_landed_copy_that_is_not_the_document_is_fetched_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("start.md");
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let x = sha256_bytes(b"X");
        let known = HashMap::from([("start.md".to_owned(), x.clone())]);
        let entry = |sha: &str, ticket: &str| -> Vec<Entry> {
            vec![("start.md".to_owned(), sha.to_owned(), ticket.to_owned())]
        };
        let y_fetch = |_: String, _: Progress| async { Ok::<Vec<u8>, anyhow::Error>(b"Y".to_vec()) };
        let x_fetch = |_: String, _: Progress| async { Ok::<Vec<u8>, anyhow::Error>(b"X".to_vec()) };

        downloads.reconcile(&entry(&sha256_bytes(b"Y"), "ty"), &known, y_fetch);
        until_written(&path, b"Y").await;
        downloads.reconcile(&entry(&x, "tx"), &known, x_fetch);

        let landed = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let done = finished.recv().await?;
                if done.sha == x && done.result.is_ok() {
                    return Some(());
                }
            }
        })
        .await;

        assert!(matches!(landed, Ok(Some(()))), "the document names X, but the disk copy was never fetched again");
        assert_eq!(std::fs::read(&path).unwrap(), b"X");
    }

    #[test]
    fn an_untracked_file_is_not_published_over_the_document() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let downloads = Downloads::new(dir.path().to_path_buf(), done);
        downloads.gate.set_wanted("start.md", &sha256_bytes(b"Y"), "ty");
        downloads.gate.set_wanted("same.md", &sha256_bytes(b"W"), "tw");
        let disk = BTreeMap::from([
            ("start.md".to_owned(), sha256_bytes(b"X")),
            ("same.md".to_owned(), sha256_bytes(b"W")),
            ("new.md".to_owned(), sha256_bytes(b"Z")),
        ]);

        let publish = downloads.publishable(&disk, &HashMap::new());

        assert_eq!(publish, vec![("new.md".to_owned(), sha256_bytes(b"Z"))]);
    }

    #[test]
    fn a_user_edit_back_to_a_landed_sha_is_published() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("start.md");
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let known = HashMap::from([("start.md".to_owned(), sha256_bytes(b"X"))]);
        std::fs::write(&path, "Y").unwrap();
        downloads.gate.record_landed("start.md", &sha256_bytes(b"Y"));
        downloads.claim_edits(&known).unwrap();
        std::fs::write(&path, "W").unwrap();
        downloads.claim_edits(&known).unwrap();
        std::fs::write(&path, "Y").unwrap();

        let disk = downloads.claim_edits(&known).unwrap();

        assert_eq!(downloads.publishable(&disk, &known), vec![("start.md".to_owned(), sha256_bytes(b"Y"))]);
    }

    #[test]
    fn a_dropped_path_removes_only_a_copy_from_the_mesh() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.md"), "Y").unwrap();
        std::fs::write(dir.path().join("edit.md"), "W").unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let downloads = Downloads::new(dir.path().to_path_buf(), done);
        downloads.gate.record_landed("gone.md", &sha256_bytes(b"Y"));
        let known = HashMap::from([("edit.md".to_owned(), sha256_bytes(b"X"))]);

        let copies = downloads.drop_unnamed(&serde_json::Map::new(), &known);

        let removed: Vec<&str> =
            copies.iter().filter(|(_, copy)| copy.is_some()).map(|(path, _)| path.as_str()).collect();
        assert_eq!(removed, vec!["gone.md"]);
    }

    #[test]
    fn a_dropped_path_keeps_its_record_until_its_file_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.md"), "Y").unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let downloads = Downloads::new(dir.path().to_path_buf(), done);
        downloads.gate.record_landed("gone.md", &sha256_bytes(b"Y"));
        let known = HashMap::from([("gone.md".to_owned(), sha256_bytes(b"X"))]);

        let first = downloads.drop_unnamed(&serde_json::Map::new(), &known);
        let again = downloads.drop_unnamed(&serde_json::Map::new(), &known);

        assert_eq!(first.len(), 1, "the first drop lists the file");
        assert_eq!(again.len(), 1, "the record was cleared before the file was removed");
    }

    #[test]
    fn a_removed_copy_drops_its_landed_record() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.md"), "Y").unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        downloads.gate.record_landed("gone.md", &sha256_bytes(b"Y"));
        let mut known = HashMap::from([("gone.md".to_owned(), sha256_bytes(b"X"))]);

        downloads.remove_dropped(&serde_json::Map::new(), &mut known, |local| std::fs::remove_file(local));

        assert!(downloads.gate.landed_sha("gone.md").is_none(), "the landed record survived the removal");
    }

    #[test]
    fn a_failed_removal_is_retried_and_not_published_back() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gone.md"), "X").unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let mut known = HashMap::from([("gone.md".to_owned(), sha256_bytes(b"X"))]);

        downloads.remove_dropped(&serde_json::Map::new(), &mut known, |_| Err(io::Error::other("read-only")));
        let disk = downloads.claim_edits(&known).unwrap();

        assert!(downloads.publishable(&disk, &known).is_empty(), "the file was published back to the document");
        assert!(dir.path().join("gone.md").exists());

        downloads.remove_dropped(&serde_json::Map::new(), &mut known, |local| std::fs::remove_file(local));
        assert!(!dir.path().join("gone.md").exists(), "the retry did not remove the file");
        assert!(known.is_empty());
    }

    #[derive(Debug)]
    struct ConnectionLost(io::Error);

    impl std::fmt::Display for ConnectionLost {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("connection lost")
        }
    }

    impl std::error::Error for ConnectionLost {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_blob_error_keeps_its_cause_when_it_is_a_fetch_error() {
        let cause = BlobError::Internal(Box::new(ConnectionLost(io::Error::other("reset by peer"))));

        assert_eq!(error_text(&fetch_error(cause)), "connection lost: reset by peer");
    }

    /// A document of pointers, and the indexes those pointers name, as the sync reads them.
    fn pointed(slices: &[(&str, serde_json::Value)]) -> (serde_json::Value, Indexes) {
        let mut document = serde_json::Map::new();
        let mut indexes = Indexes::default();
        for (author, entries) in slices {
            let sha = sha256_bytes(entries.to_string().as_bytes());
            let pointer = serde_json::json!({ "index": { "ticket": "t", "sha256": sha, "ts": 1 } });
            document.insert(author.to_string(), pointer);
            indexes.0.insert(author.to_string(), (sha, entries.as_object().unwrap().clone()));
        }
        (serde_json::Value::Object(document), indexes)
    }

    #[test]
    fn two_authors_with_different_ts_give_the_newer() {
        let (document, indexes) = pointed(&[
            ("a", serde_json::json!({ "p": { "sha256": "old", "ticket": "t1", "ts": 1 } })),
            ("b", serde_json::json!({ "p": { "sha256": "new", "ticket": "t2", "ts": 2 } })),
        ]);

        assert_eq!(view_of(&document, &indexes)["p"]["sha256"], "new");
    }

    #[test]
    fn a_winning_tombstone_in_another_index_drops_the_file() {
        let (document, indexes) = pointed(&[
            ("a", serde_json::json!({ "p": { "sha256": "x", "ticket": "t", "ts": 1 } })),
            ("b", serde_json::json!({ "p": { "deleted": true, "ts": 2 } })),
        ]);

        assert_eq!(removed_by_tombstone(&document, &indexes, &["p".to_owned()]), vec!["p".to_owned()]);
    }

    #[test]
    fn a_null_slice_keeps_its_file() {
        let (mut document, indexes) =
            pointed(&[("a", serde_json::json!({ "p": { "sha256": "x", "ticket": "t", "ts": 1 } }))]);
        document["b"] = serde_json::Value::Null;

        assert!(removed_by_tombstone(&document, &indexes, &["p".to_owned()]).is_empty());
        assert!(view_of(&document, &indexes).contains_key("p"));
    }

    #[test]
    fn a_failed_index_fetch_drops_nothing() {
        let (document, _) = pointed(&[("a", serde_json::json!({ "p": { "deleted": true, "ts": 2 } }))]);

        assert!(removed_by_tombstone(&document, &Indexes::default(), &["p".to_owned()]).is_empty());
    }

    #[test]
    fn the_pointer_change_makes_a_fetch() {
        let (document, indexes) = pointed(&[("a", serde_json::json!({}))]);

        assert_eq!(pointers_to_fetch(&document, &Indexes::default()).len(), 1);
        assert!(pointers_to_fetch(&document, &indexes).is_empty());
    }

    #[test]
    fn a_pointer_change_cancels_the_older_fetch() {
        let (document, _) = pointed(&[("a", serde_json::json!({}))]);
        let running = HashMap::from([("a".to_owned(), "older".to_owned())]);

        let (starts, cancels) = fetch_plan(&document, &Indexes::default(), &running);

        assert_eq!(cancels, vec!["a".to_owned()], "the older fetch was not cancelled");
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].2, sha256_bytes(b"{}"));
    }

    #[test]
    fn the_own_index_file_is_never_published_or_removed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(INDEX_FILE), "{}").unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);

        let disk = scan::scan(dir.path()).unwrap();
        assert!(!disk.contains_key(INDEX_FILE), "the index file was scanned");
        let publish = downloads.publishable(&disk, &HashMap::new());
        assert!(publish.iter().all(|(path, _)| path != INDEX_FILE), "the index file would be published");

        downloads.remove_dropped(&serde_json::Map::new(), &mut HashMap::new(), |local| std::fs::remove_file(local));
        assert!(dir.path().join(INDEX_FILE).exists(), "the index file was removed");
    }

    #[test]
    fn a_saved_index_of_another_mesh_is_ignored() {
        let bytes = br#"{"mesh":"other","own":{"a.md":{"deleted":true,"ts":5}}}"#;

        assert!(load_saved(bytes, "this").is_empty());
    }

    #[test]
    fn a_ts_far_ahead_does_not_beat_a_later_honest_edit() {
        let now = now_ms();
        let (document, indexes) = pointed(&[
            ("bad", serde_json::json!({ "p": { "sha256": "bad", "ticket": "t1", "ts": u64::MAX } })),
            ("good", serde_json::json!({ "p": { "sha256": "good", "ticket": "t2", "ts": now } })),
        ]);

        assert_eq!(view_of(&document, &indexes)["p"]["sha256"], "good");
    }

    #[test]
    fn the_order_of_ts_is_the_same_on_every_peer() {
        let now = now_ms();
        let minute = 60_000;
        let (document, indexes) = pointed(&[
            ("a", serde_json::json!({ "p": { "sha256": "early", "ticket": "t1", "ts": now + 8 * minute } })),
            ("b", serde_json::json!({ "p": { "sha256": "late", "ticket": "t2", "ts": now + 9 * minute } })),
        ]);
        assert_eq!(view_of(&document, &indexes)["p"]["sha256"], "late");

        let (document, indexes) = pointed(&[
            ("a", serde_json::json!({ "p": { "sha256": "a", "ticket": "t1", "ts": now + 30 * minute } })),
            ("b", serde_json::json!({ "p": { "sha256": "b", "ticket": "t2", "ts": now + 20 * minute } })),
        ]);
        assert!(view_of(&document, &indexes).get("p").is_none(), "an entry past the reach must not win");
    }

    #[test]
    fn an_index_that_is_no_longer_the_pointer_is_not_cached() {
        let pointers = HashMap::from([("a".to_owned(), "new".to_owned())]);

        assert!(!current_pointer(&pointers, "a", "old"));
    }

    #[test]
    fn a_restart_keeps_its_tombstones() {
        let saved = BTreeMap::from([("gone.md".to_owned(), serde_json::json!({ "deleted": true, "ts": 5 }))]);

        let own = own_after_restart(saved, &BTreeMap::new(), 9);

        assert_eq!(own["gone.md"]["deleted"], true);
        assert_eq!(own["gone.md"]["ts"], 5);
    }

    #[test]
    fn a_restart_turns_a_file_deleted_while_stopped_into_a_tombstone() {
        let saved = BTreeMap::from([("a.md".to_owned(), serde_json::json!({ "sha256": "x", "ticket": "t", "ts": 1 }))]);

        let own = own_after_restart(saved, &BTreeMap::new(), 9);

        assert_eq!(own["a.md"]["deleted"], true, "a file gone while stopped was not tombstoned: {own:?}");
        assert_eq!(own["a.md"]["ts"], 9);
    }

    #[test]
    fn a_restart_keeps_a_file_still_on_disk() {
        let saved = BTreeMap::from([("a.md".to_owned(), serde_json::json!({ "sha256": "x", "ticket": "t", "ts": 1 }))]);
        let disk = BTreeMap::from([("a.md".to_owned(), "x".to_owned())]);

        let own = own_after_restart(saved, &disk, 9);

        assert_eq!(own["a.md"]["sha256"], "x");
    }

    #[test]
    fn a_sha256_mismatch_of_the_index_is_refused() {
        let wrong = "0".repeat(64);

        assert!(check_index(b"{}", &wrong).is_err());
        assert!(check_index(b"{}", &sha256_bytes(b"{}")).is_ok());
    }

    #[test]
    fn an_index_over_the_cap_is_refused() {
        // A valid object, so only the cap can refuse it.
        let mut bytes = b"{".to_vec();
        bytes.resize(INDEX_MAX_BYTES, b' ');
        bytes.push(b'}');
        assert!(bytes.len() > INDEX_MAX_BYTES);

        let refused = check_index(&bytes, &sha256_bytes(&bytes));

        assert!(refused.is_err_and(|error| format!("{error}").contains("cap")));
    }

    #[test]
    fn a_retry_log_shows_the_whole_error_chain() {
        let error = anyhow::Error::from(io::Error::other("connection lost")).context("blob fetch");

        assert_eq!(error_text(&error), "blob fetch: connection lost");
    }

    #[tokio::test(start_paused = true)]
    async fn six_retries_finish_within_the_retry_schedule() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = {
            let calls = calls.clone();
            move |_: String, _: Progress| {
                let calls = calls.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) < 6 {
                        return Err(anyhow!("connection lost"));
                    }
                    Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
                }
            }
        };
        let started = tokio::time::Instant::now();

        download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), fetch).await.unwrap();

        // The waits are 1, 2, 4, 8, 10 and 10 s, and jitter adds up to half of each: 35 s to 52.5 s.
        let elapsed = started.elapsed();
        assert_eq!(calls.load(Ordering::SeqCst), 7);
        assert!(elapsed <= Duration::from_secs(53), "six retries took {elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracked_local_edit_is_not_overwritten_by_an_older_download() {
        // The baseline is known, so the edit is kept. A file with no baseline is replaced instead; see `claim_edits`.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("start.md"), "local edit").unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let known = HashMap::from([("start.md".to_owned(), sha256_bytes(b"baseline"))]);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "slow".to_owned())];
        let fetch = |_: String, _: Progress| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<Vec<u8>, anyhow::Error>(b"from author".to_vec())
        };

        downloads.reconcile(&entries, &known, fetch);
        downloads.claim_edits(&known).unwrap();
        tokio::time::sleep(Duration::from_secs(20)).await;

        assert_eq!(std::fs::read_to_string(dir.path().join("start.md")).unwrap(), "local edit");
        assert!(tokio::time::timeout(Duration::from_secs(1), finished.recv()).await.is_err());
    }

    #[tokio::test]
    async fn a_file_already_on_disk_is_still_published() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let bytes = b"from author";
        std::fs::write(&local, bytes).unwrap();
        let sha = sha256_bytes(bytes);

        let result = download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_, _| async {
            Ok(Vec::new())
        })
        .await;

        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_fetch_that_fails_twice_then_works_still_completes() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let mut calls = 0;

        let started = tokio::time::Instant::now();
        let result = download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_, _| {
            calls += 1;
            let attempt = calls;
            async move {
                if attempt < 3 {
                    bail!("dead source");
                }
                Ok(bytes.to_vec())
            }
        })
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls, 3);
        // Two waits, at least BLOB_RETRY_BASE and twice that, so a dropped sleep fails here.
        let minimum = BLOB_RETRY_BASE * 3;
        assert!(started.elapsed() >= minimum, "elapsed {:?}", started.elapsed());
        assert_eq!(std::fs::read(&local).unwrap(), bytes);
    }
}
