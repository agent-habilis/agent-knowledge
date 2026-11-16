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
/// A manifest that cannot be fetched for this long no longer holds back the first publish of a file:
/// the peer that wrote it crashed, and its slice stays in the state until it lapses.
const UNREADABLE_AFTER: Duration = Duration::from_secs(180);
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

/// What a cycle knows about the mesh as a whole.
#[derive(Clone, Copy)]
struct View {
    may_publish: bool,
    /// See [`Facts::view_complete`].
    complete: bool,
}

enum Downloaded {
    Written,
    /// The disk copy changed after the scan. It is the sha256 now, or `None` if the file is gone.
    Edited(Option<String>),
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
    /// When each manifest ticket first failed, while the mesh still points to it.
    unreadable: HashMap<String, Instant>,
    /// The sha256 of the manifest that the own slice points to.
    pointer: Option<String>,
    /// The sha256 of the last manifest that was over [`MAX_MANIFEST_BYTES`], so the refusal is reported once.
    refused: Option<String>,
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
            unreadable: HashMap::new(),
            pointer: None,
            refused: None,
            held_versions: Vec::new(),
            local_dirty: false,
            adopted_since: None,
            hold_until: joining.then(|| Instant::now() + JOIN_GRACE),
        }
    }

    /// One pass: read the mesh, make the disk match the winning versions, publish the local changes.
    async fn cycle(&mut self) {
        self.failed.retain(|_, at| at.elapsed() < FAILED_TICKET_PAUSE);
        let Some((authors, complete)) = self.read_view().await else { return };
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
        let view = View { may_publish, complete };

        let paths: BTreeSet<&String> = union.keys().chain(disk.keys()).chain(self.known.keys()).collect();
        let paths: Vec<String> = paths.into_iter().cloned().collect();
        for path in paths {
            self.settle(&path, disk.get(&path), union.get(&path), view, now).await;
        }
        self.flush_pointer().await;
        self.save_known();
    }

    /// The pointer of each author's manifest, with the manifest fetched, as `(author, sha256)`.
    /// The flag says whether every manifest is there, except those that stayed unreadable for [`UNREADABLE_AFTER`].
    /// `None` if the mesh did not answer.
    async fn read_view(&mut self) -> Option<(Vec<(String, String)>, bool)> {
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
                    self.unreadable.entry(ticket.clone()).or_insert_with(Instant::now);
                    self.failed.insert(ticket, Instant::now());
                }
            }
        }
        let current: BTreeSet<&String> = pointers.iter().map(|(_, sha, _)| sha).collect();
        self.manifests.retain(|sha, _| current.contains(sha));
        let tickets: BTreeSet<&String> = pointers.iter().map(|(_, _, ticket)| ticket).collect();
        self.unreadable.retain(|ticket, _| tickets.contains(ticket));
        let complete = pointers.iter().all(|(_, sha, ticket)| {
            self.manifests.contains_key(sha) || self.unreadable.get(ticket).is_some_and(|since| since.elapsed() >= UNREADABLE_AFTER)
        });
        let fetched = pointers.into_iter().filter(|(_, sha, _)| self.manifests.contains_key(sha)).map(|(author, sha, _)| (author, sha)).collect();
        Some((fetched, complete))
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

    /// Decide one path with [`decide`], then act. A local change is published and not overwritten.
    /// Otherwise the winning version of the mesh is taken.
    async fn settle(&mut self, path: &str, disk: Option<&String>, resolved: Option<&Resolved>, view: View, now: u64) {
        if resolve(&self.root, path).is_none() {
            return;
        }
        let facts = Facts {
            known: self.known.get(path).map(String::as_str),
            disk: disk.map(String::as_str),
            winner: resolved.map(|resolved| &resolved.winner),
            may_publish: view.may_publish,
            view_complete: view.complete,
            mine_holds_winner: resolved.is_some_and(|resolved| self.mine.get(path).is_some_and(|held| held.same_version(&resolved.winner))),
        };
        match (decide(&facts), resolved) {
            (Action::Publish, _) => self.stage_local(path, disk, now).await,
            (Action::TakeLive, Some(resolved)) => self.take_live(path, disk, resolved, now).await,
            (Action::TakeGone, Some(resolved)) => self.take_gone(path, &resolved.winner).await,
            _ => {}
        }
    }

    async fn take_live(&mut self, path: &str, disk: Option<&String>, resolved: &Resolved, now: u64) {
        let Entry::Live { sha256: sha, .. } = &resolved.winner else { return };
        if disk.map(String::as_str) != Some(sha.as_str()) {
            match self.download(path, sha, disk, &resolved.tickets).await {
                Ok(Downloaded::Written) => eprintln!("↓ {path}"),
                Ok(Downloaded::Edited(edited)) => return self.stage_local(path, edited.as_ref(), now).await,
                Err(error) => return eprintln!("download {path} failed: {error:#}"),
            }
        }
        self.known.insert(path.to_owned(), sha.clone());
        self.adopt(path, &resolved.winner).await;
    }

    /// Remove the disk copy and take the tombstone. If the copy cannot be removed, this peer keeps
    /// its `known` entry and does not take the tombstone, so the next cycle tries again.
    async fn take_gone(&mut self, path: &str, winner: &Entry) {
        if let Some(local) = resolve(&self.root, path) {
            match std::fs::remove_file(&local) {
                Ok(()) => eprintln!("↓ deleted {path}"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return eprintln!("delete {path} failed: {error}"),
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
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            if self.refused.as_deref() != Some(sha.as_str()) {
                eprintln!("folder too large to sync: its manifest is {} bytes, over the {MAX_MANIFEST_BYTES} byte limit; nothing more is published", bytes.len());
                self.refused = Some(sha);
            }
            return;
        }
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
    /// `scanned` is the sha256 of the disk copy that the pass decided on: if the copy is another by
    /// the time the bytes arrive, the file is left alone, and the caller treats it as a local edit.
    async fn download(&mut self, path: &str, sha: &str, scanned: Option<&String>, tickets: &[String]) -> Result<Downloaded> {
        let local = resolve(&self.root, path).with_context(|| format!("unsafe path {path:?}"))?;
        let mut last = anyhow!("no ticket that is not paused");
        for ticket in tickets {
            if self.failed.get(ticket).is_some_and(|at| at.elapsed() < FAILED_TICKET_PAUSE) {
                continue;
            }
            match fetch_file(ticket, sha).await {
                Ok(bytes) => {
                    let current = std::fs::read(&local).ok().map(|bytes| sha256_bytes(&bytes));
                    if current.as_ref() != scanned {
                        return Ok(Downloaded::Edited(current));
                    }
                    if let Some(parent) = local.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    write_atomic(&local, &bytes).await?;
                    return Ok(Downloaded::Written);
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

/// What is known about one path when it is settled.
struct Facts<'a> {
    /// The sha256 that the disk and the mesh last agreed on.
    known: Option<&'a str>,
    /// The sha256 of the file on the disk now.
    disk: Option<&'a str>,
    /// The winning version in the mesh.
    winner: Option<&'a Entry>,
    may_publish: bool,
    /// Every manifest that the mesh points to is fetched. Without it, a path that no fetched
    /// manifest names can still be named by one that is missing.
    view_complete: bool,
    /// The own manifest already holds the winning version, so this peer wrote or took it.
    mine_holds_winner: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Skip,
    /// Put the disk state of the path into the own manifest as a new version.
    Publish,
    TakeLive,
    TakeGone,
}

fn decide(facts: &Facts) -> Action {
    let publish = if facts.may_publish { Action::Publish } else { Action::Skip };
    if facts.known.is_some_and(|known| facts.disk != Some(known)) {
        return publish;
    }
    match facts.winner {
        None if facts.disk.is_some() && facts.may_publish && facts.view_complete => Action::Publish,
        None => Action::Skip,
        Some(Entry::Live { .. }) => Action::TakeLive,
        Some(Entry::Gone { sha256, .. }) => match facts.disk {
            None => Action::TakeGone,
            Some(_) if facts.known.is_some() => Action::TakeGone,
            Some(_) if facts.mine_holds_winner => publish,
            Some(disk) if disk == sha256 => Action::TakeGone,
            Some(_) => publish,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gone(sha: &str) -> Entry {
        Entry::Gone { sha256: sha.to_owned(), ts_ms: 5 }
    }

    fn facts<'a>() -> Facts<'a> {
        Facts { known: None, disk: None, winner: None, may_publish: true, view_complete: true, mine_holds_winner: false }
    }

    #[test]
    fn a_new_file_is_published_only_when_the_view_is_complete() {
        let new_file = Facts { disk: Some("aa"), ..facts() };
        assert_eq!(decide(&new_file), Action::Publish);
        assert_eq!(decide(&Facts { view_complete: false, ..new_file }), Action::Skip);
    }

    #[test]
    fn a_file_recreated_after_this_peer_took_the_delete_is_published() {
        let tombstone = gone("aa");
        let stale = Facts { disk: Some("aa"), winner: Some(&tombstone), ..facts() };
        assert_eq!(decide(&stale), Action::TakeGone);
        assert_eq!(decide(&Facts { mine_holds_winner: true, ..stale }), Action::Publish);
    }

    #[test]
    fn a_synced_copy_goes_with_the_delete_and_a_local_edit_does_not() {
        let tombstone = gone("aa");
        let synced = Facts { known: Some("aa"), disk: Some("aa"), winner: Some(&tombstone), ..facts() };
        assert_eq!(decide(&synced), Action::TakeGone);
        assert_eq!(decide(&Facts { disk: Some("bb"), ..synced }), Action::Publish);
    }

    #[test]
    fn a_different_file_at_a_deleted_path_is_new() {
        let tombstone = gone("aa");
        let other = Facts { disk: Some("bb"), winner: Some(&tombstone), ..facts() };
        assert_eq!(decide(&other), Action::Publish);
    }
}
