//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` document is the file list: `{"files": {"<rel/path>": {"sha256", "ticket"}}}`.
//! It is an automerge CRDT, so concurrent writes from both sides merge, and the last write to one
//! path wins. File bytes do not fit in a gossip frame, so each entry holds a blob ticket that a
//! peer redeems against the author's blob server.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{self, BlobServer, BlobTicket, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts, Transport};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::AbortHandle;

use crate::scan::{self, resolve, sha256_bytes};
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);

/// Sync `root` until `shutdown` resolves. With `mesh` `None`, create a new mesh; else join it.
/// `on_ready` gets the mesh id, which is the hash that `mount` takes.
pub async fn run(
    root: PathBuf,
    mesh: Option<String>,
    on_ready: impl FnOnce(&str),
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let root = std::fs::canonicalize(&root).with_context(|| format!("no folder at {}", root.display()))?;
    let (sink, mut events) = membership::json_sink();
    let options = JoinOptions::default()
        .mesh(mesh)
        .name(Some(MESH_NAME.to_owned()))
        .lookup([Lookup::Mdns, Lookup::Dht, Lookup::Relay])
        // The engine default plus `Relay`. Without it, 2 peers whose hole punch fails find each
        // other through the relay but never link, because the relay is kept for lookup alone.
        .transport([Transport::Udp, Transport::WebRtc, Transport::Multihop, Transport::Gossip, Transport::Relay]);
    let member = membership::join(&options, sink).await.map_err(|error| anyhow!("{error}"))?;
    on_ready(&wait_ready(&mut events).await?);

    let blobs = BlobServer::start(LookupOpts::public_preset(), spool_dir(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let (done, mut finished) = mpsc::unbounded_channel();
    let mut folder = Folder { root: root.clone(), member, blobs, known: HashMap::new(), downloads: Downloads::new(root, done) };

    folder.reconcile().await;
    folder.publish_local().await;

    let mut every = tokio::time::interval(RECONCILE_EVERY);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(()) = ticks.recv() => folder.publish_local().await,
            Some(event) = events.recv() => {
                // Not the event's own document: events queue up while a download runs, and an
                // older snapshot that lacks a path would delete a file that a newer one has.
                if is_remote_state_change(&event) {
                    folder.reconcile().await;
                }
            }
            Some(done) = finished.recv() => folder.finish(done).await,
            _ = every.tick() => folder.reconcile().await,
        }
    }
    folder.downloads.abort_all();
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
}

/// One file the document names: its path, its sha256, and its blob ticket.
type Entry = (String, String, String);

/// Why a download failed. A permanent failure is not retried for the same entry.
#[derive(Debug)]
enum Failure {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

/// At most this many blob fetches run at once.
const FETCH_SLOTS: usize = 4;
/// A fetch that runs this long is cut off, and the retry loop starts a new attempt. The dial is capped at 90 s.
const ATTEMPT_DEADLINE: Duration = Duration::from_secs(180);

/// Cancels the task it holds when dropped, so an aborted outer task takes its inner task with it.
struct CancelOnDrop(AbortHandle);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The downloads of one folder. The loop owns it. Each download runs in its own task.
struct Downloads {
    root: PathBuf,
    gate: Gate,
    /// The sha and ticket of each entry whose download failed for good.
    failed: HashMap<String, (String, String)>,
    /// The sha of each download in flight, and the handle that cancels it.
    running: HashMap<String, (String, String, AbortHandle)>,
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
            running: HashMap::new(),
            slots: Arc::new(Semaphore::new(FETCH_SLOTS)),
            done,
        }
    }

    /// Start a download for each entry that `known` does not hold and no download is already running for.
    /// Each one reports back on the `done` channel. A download for a sha that the document no longer names is cancelled.
    fn reconcile<F, Fut>(&mut self, entries: &[Entry], known: &HashMap<String, String>, fetch: F)
    where
        F: Fn(String) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send,
    {
        for (path, sha, ticket) in entries {
            self.gate.set_wanted(path, sha);
            if known.get(path) == Some(sha) || self.failed.get(path) == Some(&(sha.clone(), ticket.clone())) {
                continue;
            }
            match self.running.get(path) {
                Some((running_sha, running_ticket, _)) if running_sha == sha && running_ticket == ticket => continue,
                Some((_, _, handle)) => {
                    handle.abort();
                    self.running.remove(path);
                }
                None => {}
            }
            let Some(local) = resolve(&self.root, path) else {
                self.failed.insert(path.clone(), (sha.clone(), ticket.clone()));
                eprintln!("download {path} failed: unsafe path {path:?}");
                continue;
            };
            // A fetch takes a slot only while it runs, so a download that waits to retry holds none.
            let (slots, fetch_again) = (self.slots.clone(), fetch.clone());
            let attempt = move |ticket: String| {
                let (slots, fetch) = (slots.clone(), fetch_again.clone());
                async move {
                    let _slot = slots.acquire_owned().await;
                    tokio::time::timeout(ATTEMPT_DEADLINE, fetch(ticket))
                        .await
                        .unwrap_or_else(|_| Err(anyhow!("fetch timed out")))
                }
            };
            let done = self.done.clone();
            let (path_owned, sha_owned, ticket_owned) = (path.clone(), sha.clone(), ticket.clone());
            let (path_for_fetch, sha_for_fetch, ticket_for_fetch) = (path.clone(), sha_owned.clone(), ticket_owned.clone());
            let gate = self.gate.clone();
            let task = tokio::spawn(async move {
                // The download runs in an inner task, so a panic in it still reports back as a failure.
                let inner = tokio::spawn(async move {
                    download_file(&local, &path_for_fetch, &sha_for_fetch, &ticket_for_fetch, &gate, attempt).await
                });
                let _cancel = CancelOnDrop(inner.abort_handle());
                let result = match inner.await {
                    Ok(result) => result,
                    Err(error) => Err(Failure::Transient(anyhow!("download task ended: {error}"))),
                };
                let _ = done.send(Finished { path: path_owned, sha: sha_owned, ticket: ticket_owned, result });
            });
            self.running.insert(path.clone(), (sha.clone(), ticket.clone(), task.abort_handle()));
        }
    }

    /// Record how a download ended, so a permanent failure is not started again.
    fn settle(&mut self, done: &Finished) {
        // A result from an entry that was replaced or cancelled must not change the current entry's state.
        let current = self.running.get(&done.path).is_some_and(|(sha, ticket, _)| sha == &done.sha && ticket == &done.ticket);
        if !current {
            return;
        }
        self.running.remove(&done.path);
        match &done.result {
            Err(Failure::Permanent(_)) => {
                self.failed.insert(done.path.clone(), (done.sha.clone(), done.ticket.clone()));
            }
            _ => {
                self.failed.remove(&done.path);
            }
        }
    }

    /// Stop everything for a path that the document no longer names.
    fn forget(&mut self, path: &str) {
        self.gate.unwant(path);
        self.failed.remove(path);
        if let Some((_, _, handle)) = self.running.remove(path) {
            handle.abort();
        }
    }

    /// Stop every download in flight, on shutdown.
    fn abort_all(&mut self) {
        for (_, (_, _, handle)) in self.running.drain() {
            handle.abort();
        }
    }
}

impl Folder {
    async fn publish_local(&mut self) {
        let disk = match scan::scan(&self.root) {
            Ok(disk) => disk,
            Err(error) => return eprintln!("scan failed: {error}"),
        };
        for (path, sha) in &disk {
            if self.known.get(path) == Some(sha) {
                continue;
            }
            match self.publish(path, sha).await {
                Ok(()) => {
                    eprintln!("↑ {path}");
                    self.known.insert(path.clone(), sha.clone());
                }
                Err(error) => eprintln!("publish {path} failed: {error:#}"),
            }
        }
        let gone: Vec<String> = self.known.keys().filter(|path| !disk.contains_key(*path)).cloned().collect();
        for path in gone {
            match self.merge(json!({ "files": { &path: null } })).await {
                Ok(()) => {
                    eprintln!("↑ deleted {path}");
                    self.known.remove(&path);
                }
                Err(error) => eprintln!("delete {path} failed: {error:#}"),
            }
        }
    }

    async fn publish(&self, path: &str, sha: &str) -> Result<()> {
        let ticket = self
            .blobs
            .register(&self.root.join(path), ContentId::new(sha))
            .await
            .map_err(|error| anyhow!("{error}"))?;
        self.merge(json!({ "files": { path: { "sha256": sha, "ticket": ticket.encode() } } })).await
    }

    async fn merge(&self, merge: Value) -> Result<()> {
        self.member
            .request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply }))
            .await
            .and_then(|sent| sent)
            .map_err(|error| anyhow!("{error}"))
    }

    async fn reconcile(&mut self) {
        let Ok(text) = self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await else {
            return;
        };
        if let Ok(document) = serde_json::from_str::<Value>(&text) {
            self.apply(&document);
        }
    }

    /// Make the disk match the document, for every path the document or `known` names.
    fn apply(&mut self, document: &Value) {
        let empty = serde_json::Map::new();
        let files = document.get("files").and_then(Value::as_object).unwrap_or(&empty);
        let entries: Vec<Entry> = files
            .iter()
            .filter_map(|(path, entry)| {
                let (Some(sha), Some(ticket)) = (entry["sha256"].as_str(), entry["ticket"].as_str()) else {
                    return None;
                };
                Some((path.clone(), sha.to_owned(), ticket.to_owned()))
            })
            .collect();
        self.downloads.reconcile(&entries, &self.known, fetch_blob);
        // Every path the document named at some point, and no longer names, stops downloading.
        let removed = unnamed(self.downloads.gate.wanted_paths(), files);
        for path in removed {
            self.downloads.forget(&path);
        }
        let gone = unnamed(self.known.keys().cloned().collect(), files);
        for path in gone {
            let synced = self.known.remove(&path);
            let Some(local) = resolve(&self.root, &path) else { continue };
            // Keep a local edit that no peer has seen yet; the next tick publishes it again.
            if file_sha(&local).is_some() && file_sha(&local) == synced {
                let _ = std::fs::remove_file(&local);
                eprintln!("↓ deleted {path}");
            }
        }
    }

    /// Take the end of one download. A sha that the document no longer names is dropped.
    async fn finish(&mut self, done: Finished) {
        self.downloads.settle(&done);
        let Finished { path, sha, result, .. } = done;
        if self.downloads.gate.wanted_sha(&path).as_deref() != Some(sha.as_str()) {
            return;
        }
        let published = match result {
            // Serve the file too, so a peer can still get it after its author left.
            Ok(()) => self.publish(&path, &sha).await,
            Err(Failure::Transient(error) | Failure::Permanent(error)) => Err(error),
        };
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
    mut fetch: impl FnMut(String) -> Fut,
) -> Result<(), Failure>
where
    Fut: Future<Output = Result<Vec<u8>>>,
{
    if file_sha(local).as_deref() == Some(sha) {
        return Ok(());
    }
    let mut attempt = 0;
    let bytes = loop {
        match fetch(ticket.to_owned()).await {
            Ok(bytes) => break bytes,
            Err(error) if error.is::<BadTicket>() => return Err(Failure::Permanent(error)),
            Err(error) => {
                let wait = backoff(attempt);
                tracing::debug!(path = %local.display(), attempt = attempt.saturating_add(1), wait = ?wait, error = %error, "retrying a blob fetch");
                tokio::time::sleep(wait).await;
                attempt = attempt.saturating_add(1);
            }
        }
    };
    if sha256_bytes(&bytes) != sha {
        return Err(Failure::Permanent(anyhow!("content does not match its sha256")));
    }
    // A local write error, such as a full disk or a folder where the file goes, repeats until the entry changes.
    if !gate.write(local, path, sha, bytes).await.map_err(Failure::Permanent)? {
        return Err(Failure::Transient(anyhow!("{path} was replaced before its write")));
    }
    Ok(())
}

/// The wait before retry `attempt`: 2 s, doubling up to 60 s, plus up to half of that as jitter.
fn backoff(attempt: u32) -> Duration {
    let base = Duration::from_secs(2).saturating_mul(2u32.saturating_pow(attempt)).min(Duration::from_secs(60));
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

async fn fetch_blob(ticket: String) -> Result<Vec<u8>> {
    let ticket = BlobTicket::decode(&ticket).map_err(|error| anyhow::Error::new(BadTicket(error.to_string())))?;
    let mut bytes = Vec::new();
    blob::fetch(&ticket, &mut bytes, None).await.map_err(|error| anyhow!("{error}"))?;
    Ok(bytes)
}

/// The writes of one folder, and the sha that the document names for each path.
/// Writes run one at a time, and each re-checks its sha under the lock, so the newest entry writes last.
#[derive(Clone, Default)]
struct Gate {
    turn: Arc<Mutex<()>>,
    wanted: Arc<Mutex<HashMap<String, String>>>,
}

impl Gate {
    fn set_wanted(&self, path: &str, sha: &str) {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).insert(path.to_owned(), sha.to_owned());
    }

    fn unwant(&self, path: &str) {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).remove(path);
    }

    fn wanted_sha(&self, path: &str) -> Option<String> {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).get(path).cloned()
    }

    fn wanted_paths(&self) -> Vec<String> {
        self.wanted.lock().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect()
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

async fn wait_ready(events: &mut mpsc::UnboundedReceiver<String>) -> Result<String> {
    while let Some(event) = events.recv().await {
        let event: Value = serde_json::from_str(&event)?;
        if event["kind"] == "ready" {
            return event["mesh"].as_str().map(str::to_owned).context("ready event without a mesh id");
        }
    }
    bail!("the mesh stopped before it was ready")
}

fn is_remote_state_change(event: &str) -> bool {
    serde_json::from_str::<Value>(event).is_ok_and(|event| {
        event["kind"] == "state_changed" && event["channel"] == "state" && event["is_self"] == false
    })
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

        let result = download_file(
            &local,
            "start.md",
            &sha,
            "ticket",
            &gate_wanting("start.md", &sha),
            |_| async {
                tokio::time::sleep(Duration::from_secs(40)).await;
                Ok(bytes.to_vec())
            },
        )
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
        let fetch = move |ticket: String| async move {
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
            move |_: String| {
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
            move |_: String| {
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
            move |ticket: String| {
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
            move |_: String| {
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
        let fetch = |_: String| async { Ok::<Vec<u8>, anyhow::Error>(Vec::new()) };

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
        let fetch = move |ticket: String| async move {
            if ticket == "stuck" {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };

        downloads.reconcile(&entries, &HashMap::new(), fetch);
        let first = tokio::time::timeout(Duration::from_secs(1), finished.recv()).await.expect("a result").expect("open");

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
            move |_: String| {
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
        downloads.gate.set_wanted("start.md", &"0".repeat(64));

        downloads.forget("start.md");

        assert!(downloads.gate.wanted_paths().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_ticket_that_does_not_decode_is_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let sha = sha256_bytes(b"from author");

        let outcome =
            tokio::time::timeout(Duration::from_secs(3600), download_file(&local, "start.md", &sha, "not a ticket", &gate_wanting("start.md", &sha), fetch_blob)).await;

        assert!(matches!(outcome, Ok(Err(Failure::Permanent(_)))), "{outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_ticket_for_the_same_sha_replaces_the_one_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let (done, _finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let bytes = b"from author";
        let sha = sha256_bytes(bytes);
        let fetch = move |ticket: String| async move {
            if ticket == "dead" {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec())
        };
        let dead: Vec<Entry> = vec![("start.md".to_owned(), sha.clone(), "dead".to_owned())];
        let live: Vec<Entry> = vec![("start.md".to_owned(), sha, "live".to_owned())];

        downloads.reconcile(&dead, &HashMap::new(), fetch.clone());
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
            download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_| async { Ok::<Vec<u8>, anyhow::Error>(bytes.to_vec()) }),
        )
        .await;

        assert!(matches!(outcome, Ok(Err(Failure::Permanent(_)))), "{outcome:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_fetch_still_reports_back() {
        let dir = tempfile::tempdir().unwrap();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut downloads = Downloads::new(dir.path().to_path_buf(), done);
        let sha = sha256_bytes(b"from author");
        let entries: Vec<Entry> = vec![("start.md".to_owned(), sha, "ticket".to_owned())];
        let fetch = |_: String| async {
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
            move |_: String| {
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
        gate.set_wanted(path, sha);
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
        let fetch = |_: String| {
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

    #[tokio::test]
    async fn a_file_already_on_disk_is_still_published() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("start.md");
        let bytes = b"from author";
        std::fs::write(&local, bytes).unwrap();
        let sha = sha256_bytes(bytes);

        let result = download_file(&local, "start.md", &sha, "ticket", &gate_wanting("start.md", &sha), |_| async { Ok(Vec::new()) }).await;

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
        let result = download_file(
            &local,
            "start.md",
            &sha,
            "ticket",
            &gate_wanting("start.md", &sha),
            |_| {
                calls += 1;
                let attempt = calls;
                async move {
                    if attempt < 3 {
                        bail!("dead source");
                    }
                    Ok(bytes.to_vec())
                }
            },
        )
        .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls, 3);
        // Two waits, at least 2 s and 4 s, so a dropped sleep fails here.
        assert!(started.elapsed() >= Duration::from_secs(6), "elapsed {:?}", started.elapsed());
        assert_eq!(std::fs::read(&local).unwrap(), bytes);
    }
}
