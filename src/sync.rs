//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` is one slice for each peer, and a peer writes only its own. A slice holds a
//! pointer to the peer's manifest: a blob that lists every file with a version (`ts_ms`) and a
//! ticket, or a tombstone for a deleted file. Each cycle takes the union of every manifest, picks
//! the highest version of each path (see [`crate::manifest`]), and makes the disk match it.
//!
//! A peer that takes a version also writes it into its own manifest, with its own ticket. That
//! way a version survives the departure of the peer that wrote it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{self, BlobServer, BlobTicket, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts, Transport};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::manifest::{self, Entry, Manifest, Resolved, next_ts};
use crate::scan::{self, resolve, sha256_bytes};
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MANIFEST_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// A manifest of 1000 files is about 0.5 MB.
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PARALLEL_MANIFEST_FETCHES: usize = 8;
/// How long a ticket that failed is left alone: a crashed peer's ticket stays in the state for a while.
const FAILED_TICKET_PAUSE: Duration = Duration::from_secs(60);
/// A version that this peer took from another is written to its own manifest after this quiet
/// time, so a burst of changes costs one write, and every peer does not fetch every other's copy.
const ADOPT_DELAY: Duration = Duration::from_secs(3);
/// A joining peer waits this long for the first view of the mesh before it publishes anything.
const JOIN_GRACE: Duration = Duration::from_secs(5);
/// What this peer last agreed with the mesh, kept in the folder. The scanner skips hidden names.
const KNOWN_FILE: &str = ".agent-knowledge-known.json";

/// What to sync, and as whom.
pub struct Options {
    /// The folder to keep in sync.
    pub root: PathBuf,
    /// The mesh to join, or `None` to create a new one.
    pub mesh: Option<String>,
    /// This peer's nickname, or `None` for the mesh to mint a random one.
    pub nickname: Option<String>,
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
    let Options { root, mesh, nickname } = options;
    let root = std::fs::canonicalize(&root).with_context(|| format!("no folder at {}", root.display()))?;
    let joining = mesh.is_some();
    let (sink, mut events) = membership::json_sink();
    let options = JoinOptions::default()
        .mesh(mesh)
        .nick(nickname)
        .name(Some(MESH_NAME.to_owned()))
        .lookup([Lookup::Mdns, Lookup::Dht, Lookup::Relay])
        // The engine default. `Relay` is in the list because, without it, 2 peers whose hole punch
        // fails find each other through the relay but never link, because the relay is kept for
        // lookup alone.
        .transport([Transport::Udp, Transport::WebRtc, Transport::Multihop, Transport::Relay]);
    let member = membership::join(&options, sink).await.map_err(|error| anyhow!("{error}"))?;
    on_ready(&wait_ready(&mut events).await?);

    let blobs = BlobServer::start(LookupOpts::public_preset(), spool_dir(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let mut folder = Folder::new(root, member, blobs, joining);

    folder.cycle().await;

    let mut every = tokio::time::interval(RECONCILE_EVERY);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(()) = ticks.recv() => folder.cycle().await,
            Some(event) = events.recv() => {
                let Ok(event) = serde_json::from_str::<Value>(&event) else { continue };
                if let Some(presence) = presence(&event) {
                    on_presence(presence);
                }
                if is_remote_state_change(&event) {
                    folder.cycle().await;
                }
            }
            _ = every.tick() => folder.cycle().await,
        }
    }
    folder.blobs.shutdown().await;
    membership::depart(folder.member.node).await.map_err(|error| anyhow!("{error}"))
}

struct Folder {
    root: PathBuf,
    member: Membership,
    blobs: BlobServer,
    /// The sha256 of each path as both the disk and the mesh last agreed on it, kept in [`KNOWN_FILE`].
    /// A disk copy that equals it is not a local edit. Without it, a peer that restarts with a stale
    /// copy would publish the copy as a new edit.
    known: HashMap<String, String>,
    known_text: String,
    /// This peer's own manifest: the versions it wrote and the versions it took.
    mine: Manifest,
    /// The highest `ts_ms` seen for each path, so that a new local version is after every other.
    stamps: HashMap<String, u64>,
    /// The manifests fetched, by their sha256.
    manifests: HashMap<String, Manifest>,
    /// When each ticket last failed.
    failed: HashMap<String, Instant>,
    /// The sha256 of the manifest that the own slice points to.
    pointer: Option<String>,
    /// The manifest versions that the blob server still holds, oldest first.
    held_versions: Vec<String>,
    /// A local change is in `mine` and not in the slice yet.
    local_dirty: bool,
    /// A version taken from another peer is in `mine` and not in the slice yet, since this instant.
    adopted_since: Option<Instant>,
    /// A joining peer publishes nothing until this instant, or until it sees a manifest.
    hold_until: Option<Instant>,
}

impl Folder {
    fn new(root: PathBuf, member: Membership, blobs: BlobServer, joining: bool) -> Self {
        let known = load_known(&root);
        let known_text = known_text(&known);
        Self {
            root,
            member,
            blobs,
            known,
            known_text,
            mine: Manifest::new(),
            stamps: HashMap::new(),
            manifests: HashMap::new(),
            failed: HashMap::new(),
            pointer: None,
            held_versions: Vec::new(),
            local_dirty: false,
            adopted_since: None,
            hold_until: joining.then(|| Instant::now() + JOIN_GRACE),
        }
    }

    /// One pass: read the mesh, make the disk match the winning versions, publish the local changes.
    async fn cycle(&mut self) {
        self.failed.retain(|_, at| at.elapsed() < FAILED_TICKET_PAUSE);
        let Some(authors) = self.read_view().await else { return };
        let disk = match scan::scan(&self.root) {
            Ok(disk) => disk,
            Err(error) => return eprintln!("scan failed: {error}"),
        };
        let now = now_ms();
        let union = self.resolve_union(&authors, now);
        if !union.is_empty() {
            self.hold_until = None;
        }
        let may_publish = self.hold_until.is_none_or(|until| Instant::now() >= until);

        let paths: BTreeSet<&String> = union.keys().chain(disk.keys()).chain(self.known.keys()).collect();
        let paths: Vec<String> = paths.into_iter().cloned().collect();
        for path in paths {
            self.settle(&path, disk.get(&path), union.get(&path), may_publish, now).await;
        }
        self.flush_pointer().await;
        self.save_known();
    }

    /// The pointer of each author's manifest, with the manifest fetched, as `(author, sha256)`.
    /// `None` if the mesh did not answer.
    async fn read_view(&mut self) -> Option<Vec<(String, String)>> {
        let text = self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await.ok()?;
        let document: Value = serde_json::from_str(&text).ok()?;
        let pointers = pointers(&document);

        let permits = Arc::new(Semaphore::new(MAX_PARALLEL_MANIFEST_FETCHES));
        let mut fetches = JoinSet::new();
        for (_, sha, ticket) in &pointers {
            let paused = self.failed.get(ticket).is_some_and(|at| at.elapsed() < FAILED_TICKET_PAUSE);
            if self.manifests.contains_key(sha) || paused {
                continue;
            }
            let (sha, ticket, permits) = (sha.clone(), ticket.clone(), Arc::clone(&permits));
            fetches.spawn(async move {
                let manifest = fetch_manifest(&ticket, &sha, &permits).await;
                (sha, ticket, manifest)
            });
        }
        while let Some(Ok((sha, ticket, fetched))) = fetches.join_next().await {
            match fetched {
                Ok(manifest) => {
                    self.manifests.insert(sha, manifest);
                }
                Err(error) => {
                    eprintln!("manifest fetch failed: {error:#}");
                    self.failed.insert(ticket, Instant::now());
                }
            }
        }
        let current: BTreeSet<&String> = pointers.iter().map(|(_, sha, _)| sha).collect();
        self.manifests.retain(|sha, _| current.contains(sha));
        Some(pointers.into_iter().filter(|(_, sha, _)| self.manifests.contains_key(sha)).map(|(author, sha, _)| (author, sha)).collect())
    }

    /// The winning version of each path over every fetched manifest and the own one, which is
    /// always in the union from memory. Also records the highest `ts_ms` seen for each path.
    fn resolve_union(&mut self, authors: &[(String, String)], now: u64) -> BTreeMap<String, Resolved> {
        let mut lists: Vec<(&str, &Manifest)> =
            authors.iter().filter_map(|(author, sha)| Some((author.as_str(), self.manifests.get(sha)?))).collect();
        lists.push(("", &self.mine));
        for (_, list) in &lists {
            for (path, entry) in list.iter().filter(|(_, entry)| manifest::within_bound(entry, now)) {
                let seen = self.stamps.entry(path.clone()).or_default();
                *seen = (*seen).max(entry.ts_ms());
            }
        }
        manifest::resolve(&lists, now)
    }

    /// Decide one path. A local change is published and not overwritten. Otherwise the winning
    /// version of the mesh is taken. A path that no manifest names is published.
    async fn settle(&mut self, path: &str, disk: Option<&String>, resolved: Option<&Resolved>, may_publish: bool, now: u64) {
        if resolve(&self.root, path).is_none() {
            return;
        }
        let known = self.known.get(path).cloned();
        if known.as_ref().is_some_and(|known| disk != Some(known)) {
            if may_publish {
                self.stage_local(path, disk, now).await;
            }
            return;
        }
        match (resolved, disk) {
            (None, Some(_)) if may_publish => self.stage_local(path, disk, now).await,
            (None, _) => {}
            (Some(resolved), _) => match &resolved.winner {
                Entry::Live { sha256, .. } => self.take_live(path, disk, sha256, resolved).await,
                Entry::Gone { sha256, .. } => self.take_gone(path, disk, sha256, &resolved.winner, may_publish, now).await,
            },
        }
    }

    async fn take_live(&mut self, path: &str, disk: Option<&String>, sha: &str, resolved: &Resolved) {
        if disk.map(String::as_str) != Some(sha) {
            if let Err(error) = self.download(path, sha, &resolved.tickets).await {
                return eprintln!("download {path} failed: {error:#}");
            }
            eprintln!("↓ {path}");
        }
        self.known.insert(path.to_owned(), sha.to_owned());
        self.adopt(path, &resolved.winner).await;
    }

    /// `gone_sha` is the content that the winning delete removed. The disk copy goes if it is the
    /// copy that this peer synced, or if it has that content. A different file at the path is new.
    async fn take_gone(&mut self, path: &str, disk: Option<&String>, gone_sha: &str, winner: &Entry, may_publish: bool, now: u64) {
        if let Some(sha) = disk {
            if self.known.contains_key(path) || sha == gone_sha {
                if let Some(local) = resolve(&self.root, path) {
                    let _ = std::fs::remove_file(local);
                    eprintln!("↓ deleted {path}");
                }
            } else {
                if may_publish {
                    self.stage_local(path, disk, now).await;
                }
                return;
            }
        }
        self.known.remove(path);
        self.adopt(path, winner).await;
    }

    /// Put the disk state of `path` into the own manifest as a new version: the file, or a tombstone.
    async fn stage_local(&mut self, path: &str, disk: Option<&String>, now: u64) {
        let ts_ms = next_ts(now, self.stamps.get(path).copied());
        let entry = match disk {
            Some(sha) => {
                let Some(ticket) = self.register(path, sha).await else { return };
                Entry::Live { sha256: sha.clone(), ticket, ts_ms }
            }
            None => {
                let Some(sha256) = self.known.get(path).cloned() else { return };
                Entry::Gone { sha256, ts_ms }
            }
        };
        match &entry {
            Entry::Live { sha256, .. } => {
                eprintln!("↑ {path}");
                self.known.insert(path.to_owned(), sha256.clone());
            }
            Entry::Gone { .. } => {
                eprintln!("↑ deleted {path}");
                self.known.remove(path);
            }
        }
        self.stamps.insert(path.to_owned(), ts_ms);
        self.mine.insert(path.to_owned(), entry);
        self.local_dirty = true;
    }

    /// Copy a version that this peer took into the own manifest, with the own ticket and the
    /// same `ts_ms`. It changes no winner, and it keeps the version when its author leaves.
    async fn adopt(&mut self, path: &str, winner: &Entry) {
        if self.mine.get(path).is_some_and(|held| held.same_version(winner)) {
            return;
        }
        let entry = match winner {
            Entry::Live { sha256, ts_ms, .. } => {
                let Some(ticket) = self.register(path, sha256).await else { return };
                Entry::Live { sha256: sha256.clone(), ticket, ts_ms: *ts_ms }
            }
            Entry::Gone { .. } => winner.clone(),
        };
        self.mine.insert(path.to_owned(), entry);
        self.adopted_since.get_or_insert_with(Instant::now);
    }

    /// The ticket of the file at `path`, which must still have content `sha` on the disk.
    async fn register(&self, path: &str, sha: &str) -> Option<String> {
        match self.blobs.register(&self.root.join(path), ContentId::new(sha)).await {
            Ok(ticket) if ticket.sha256_hex() == sha => Some(ticket.encode()),
            Ok(_) => None,
            Err(error) => {
                eprintln!("publish {path} failed: {error}");
                None
            }
        }
    }

    /// Write the own manifest and the pointer to it, if the manifest changed and a local change
    /// is waiting, or an adoption has waited [`ADOPT_DELAY`]. A failure leaves both flags, so the next cycle retries.
    async fn flush_pointer(&mut self) {
        let due = self.local_dirty || self.adopted_since.is_some_and(|since| since.elapsed() >= ADOPT_DELAY);
        if !due {
            return;
        }
        let bytes = manifest::encode(&self.mine);
        let sha = sha256_bytes(&bytes);
        if self.pointer.as_deref() != Some(sha.as_str())
            && let Err(error) = self.write_pointer(&bytes, &sha).await
        {
            return eprintln!("publish manifest failed: {error:#}");
        }
        self.local_dirty = false;
        self.adopted_since = None;
    }

    async fn write_pointer(&mut self, bytes: &[u8], sha: &str) -> Result<()> {
        let staged = std::env::temp_dir().join(format!("agent-knowledge-manifest-{}-{sha}.json", std::process::id()));
        tokio::fs::write(&staged, bytes).await?;
        let content = ContentId::new(&format!("manifest-{sha}"));
        let registered = self.blobs.register(&staged, content.clone()).await;
        let _ = tokio::fs::remove_file(&staged).await;
        let ticket = registered.map_err(|error| anyhow!("{error}"))?;
        self.merge(json!({ "manifest": { "sha256": sha, "ticket": ticket.encode() } })).await?;
        self.pointer = Some(sha.to_owned());
        // A peer can be fetching the version before this one, so only older versions go.
        self.held_versions.push(sha.to_owned());
        while self.held_versions.len() > 2 {
            let old = self.held_versions.remove(0);
            self.blobs.evict_content(&ContentId::new(&format!("manifest-{old}"))).await;
        }
        Ok(())
    }

    async fn merge(&self, merge: Value) -> Result<()> {
        self.member
            .request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply }))
            .await
            .map_err(|error| anyhow!("{error}"))?
            .map_err(|error| anyhow!("{error}"))
    }

    /// Fetch the file with the first ticket that serves content `sha`. A ticket that fails is left alone for a while.
    async fn download(&mut self, path: &str, sha: &str, tickets: &[String]) -> Result<()> {
        let local = resolve(&self.root, path).with_context(|| format!("unsafe path {path:?}"))?;
        let mut last = anyhow!("no ticket that is not paused");
        for ticket in tickets {
            if self.failed.get(ticket).is_some_and(|at| at.elapsed() < FAILED_TICKET_PAUSE) {
                continue;
            }
            match fetch_file(ticket, sha).await {
                Ok(bytes) => {
                    if let Some(parent) = local.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    return write_atomic(&local, &bytes).await;
                }
                Err(error) => {
                    self.failed.insert(ticket.clone(), Instant::now());
                    last = error;
                }
            }
        }
        Err(last)
    }

    fn save_known(&mut self) {
        let text = known_text(&self.known);
        if text == self.known_text {
            return;
        }
        let (file, temp) = (self.root.join(KNOWN_FILE), self.root.join(format!("{KNOWN_FILE}.tmp")));
        match std::fs::write(&temp, &text).and_then(|()| std::fs::rename(&temp, &file)) {
            Ok(()) => self.known_text = text,
            Err(error) => eprintln!("saving {KNOWN_FILE} failed: {error}"),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

/// `(author, manifest sha256, manifest ticket)` for each slice that points to a manifest.
fn pointers(document: &Value) -> Vec<(String, String, String)> {
    let Some(slices) = document.as_object() else { return Vec::new() };
    slices
        .iter()
        .filter_map(|(author, slice)| {
            let pointer = slice.get("manifest")?;
            Some((author.clone(), pointer.get("sha256")?.as_str()?.to_owned(), pointer.get("ticket")?.as_str()?.to_owned()))
        })
        .collect()
}

async fn fetch_manifest(ticket: &str, sha: &str, permits: &Semaphore) -> Result<Manifest> {
    let _permit = permits.acquire().await?;
    let ticket = BlobTicket::decode(ticket).map_err(|error| anyhow!("{error}"))?;
    if ticket.size > MAX_MANIFEST_BYTES {
        bail!("manifest of {} bytes is over the {MAX_MANIFEST_BYTES} byte limit", ticket.size);
    }
    let mut bytes = Vec::new();
    tokio::time::timeout(MANIFEST_FETCH_TIMEOUT, blob::fetch(&ticket, &mut bytes, None))
        .await
        .context("fetch timed out")?
        .map_err(|error| anyhow!("{error}"))?;
    if sha256_bytes(&bytes) != sha {
        bail!("manifest does not match its sha256");
    }
    manifest::decode(&bytes).context("not a manifest")
}

async fn fetch_file(ticket: &str, sha: &str) -> Result<Vec<u8>> {
    let ticket = BlobTicket::decode(ticket).map_err(|error| anyhow!("{error}"))?;
    let mut bytes = Vec::new();
    tokio::time::timeout(FETCH_TIMEOUT, blob::fetch(&ticket, &mut bytes, None))
        .await
        .context("fetch timed out")?
        .map_err(|error| anyhow!("{error}"))?;
    if sha256_bytes(&bytes) != sha {
        bail!("content does not match its sha256");
    }
    Ok(bytes)
}

fn load_known(root: &Path) -> HashMap<String, String> {
    let Ok(bytes) = std::fs::read(root.join(KNOWN_FILE)) else { return HashMap::new() };
    let Ok(Value::Object(entries)) = serde_json::from_slice(&bytes) else { return HashMap::new() };
    entries.into_iter().filter_map(|(path, sha)| Some((path, sha.as_str()?.to_owned()))).collect()
}

/// The text of [`KNOWN_FILE`]: sorted, so equal maps give equal text.
fn known_text(known: &HashMap<String, String>) -> String {
    let sorted: BTreeMap<&String, &String> = known.iter().collect();
    serde_json::to_string(&sorted).expect("a map of strings always serializes")
}

/// Write through a hidden sibling and rename it, so an editor never sees half a file.
/// The scanner skips hidden names, so the temp file never gets published.
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path.file_name().context("no file name")?.to_string_lossy();
    let temp = path.with_file_name(format!(".{name}.agent-knowledge"));
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
    Ok(())
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
