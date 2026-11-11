//! Two-way live sync of one folder with every other member of a mesh.
//!
//! The mesh `state` document is the file list: `{"files": {"<rel/path>": {"sha256", "ticket"}}}`.
//! It is an automerge CRDT, so concurrent writes from both sides merge, and the last write to one
//! path wins. File bytes do not fit in a gossip frame, so each entry holds a blob ticket that a
//! peer redeems against the author's blob server. Every peer that holds a copy offers its own
//! ticket too (see [`crate::providers`]), so a file outlives its author.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{self, BlobServer, BlobTicket, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts, Transport};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::providers;
use crate::scan::{self, resolve, sha256_bytes};
use crate::watch::watch;

const MESH_NAME: &str = "agent-knowledge";
/// A safety net for a `state_changed` event that never fires, for example after a backfill.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);
/// Per ticket. The tickets of one file are tried at once, so a dead one costs no extra wait.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

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
    on_ready(&wait_ready(&mut events).await?);

    let blobs = BlobServer::start(LookupOpts::public_preset(), spool_dir(), None, PayloadPath::RelayAllowed)
        .await
        .map_err(|error| anyhow!("{error}"))?;
    let (_watcher, mut ticks) = watch(&root)?;
    let mut folder = Folder { root, member, blobs, known: HashMap::new(), offered: HashMap::new() };

    folder.reconcile().await;
    folder.publish_local().await;
    folder.offer_held().await;

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
            _ = every.tick() => {
                folder.reconcile().await;
                folder.offer_held().await;
            }
        }
    }
    folder.withdraw_all().await;
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
    /// The content this peer offers in the `providers` map, by sha256.
    offered: HashMap<String, Offer>,
}

/// This peer's offer of one content.
struct Offer {
    /// The key this peer offers under: its blob server's endpoint id.
    peer: String,
    /// Whether the ticket names a relay. Without one, a peer behind another NAT can never reach
    /// it, so the offer is minted again until the blob server has a relay.
    relayed: bool,
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
        let text = match self.member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await {
            Ok(text) => text,
            Err(error) => return eprintln!("reading the shared file list failed: {error}"),
        };
        match serde_json::from_str::<Value>(&text) {
            Ok(document) => self.apply(&document).await,
            Err(error) => eprintln!("the shared file list is not JSON: {error}"),
        }
    }

    /// Offer this peer's ticket for each content it holds in sync, and withdraw the offers for
    /// content it no longer holds. An offer whose ticket names no relay is minted again on every
    /// call, until the blob server has a relay.
    async fn offer_held(&mut self) {
        let mut held: HashMap<&str, &str> = HashMap::new();
        for (path, sha) in &self.known {
            held.entry(sha.as_str()).or_insert(path.as_str());
        }
        let stale: Vec<String> = self.offered.keys().filter(|sha| !held.contains_key(sha.as_str())).cloned().collect();
        let due: Vec<(String, String)> = held
            .iter()
            .filter(|(sha, _)| self.offered.get(**sha).is_none_or(|offer| !offer.relayed))
            .map(|(sha, path)| ((*sha).to_owned(), (*path).to_owned()))
            .collect();
        for sha in stale {
            self.withdraw(&sha).await;
        }
        for (sha, path) in due {
            if let Err(error) = self.offer(&path, &sha).await {
                eprintln!("offering {path} failed: {error:#}");
            }
        }
    }

    /// Mint a ticket for the content `sha`, read from `path`, and offer it if it is new or now
    /// names a relay.
    async fn offer(&mut self, path: &str, sha: &str) -> Result<()> {
        let ticket = self
            .blobs
            .register(&self.root.join(path), ContentId::new(sha))
            .await
            .map_err(|error| anyhow!("{error}"))?;
        let relayed = ticket.address.relay_urls().next().is_some();
        if self.offered.get(sha).is_some_and(|offer| offer.relayed == relayed) {
            return Ok(());
        }
        let peer = ticket.address.id.to_string();
        self.merge(providers::offer(sha, &peer, &ticket.encode())).await?;
        self.offered.insert(sha.to_owned(), Offer { peer, relayed });
        Ok(())
    }

    async fn withdraw(&mut self, sha: &str) {
        let Some(offer) = self.offered.remove(sha) else { return };
        if let Err(error) = self.merge(providers::withdraw(sha, &offer.peer)).await {
            eprintln!("withdrawing an offer failed: {error:#}");
        }
    }

    /// Take back every offer, so peers stop trying a blob server that is about to stop.
    async fn withdraw_all(&mut self) {
        let shas: Vec<String> = self.offered.keys().cloned().collect();
        for sha in shas {
            self.withdraw(&sha).await;
        }
    }

    /// Make the disk match the document, for every path the document or `known` names.
    async fn apply(&mut self, document: &Value) {
        let empty = serde_json::Map::new();
        let files = document.get("files").and_then(Value::as_object).unwrap_or(&empty);
        for (path, entry) in files {
            let Some(sha) = entry["sha256"].as_str() else { continue };
            if self.known.get(path).map(String::as_str) == Some(sha) {
                continue;
            }
            match self.download(path, sha, providers::tickets(document, entry, sha)).await {
                Ok(()) => {
                    eprintln!("↓ {path}");
                    self.known.insert(path.clone(), sha.to_owned());
                }
                Err(error) => eprintln!("download {path} failed: {error:#}"),
            }
        }
        let gone: Vec<String> = self.known.keys().filter(|path| !files.contains_key(*path)).cloned().collect();
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

    async fn download(&self, path: &str, sha: &str, tickets: Vec<String>) -> Result<()> {
        let local = resolve(&self.root, path).with_context(|| format!("unsafe path {path:?}"))?;
        if file_sha(&local).as_deref() == Some(sha) {
            return Ok(());
        }
        let bytes = fetch_any(tickets, sha).await?;
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        write_atomic(&local, &bytes).await
    }
}

/// Fetch the content `sha` through every ticket at once and keep the first copy that matches.
/// The others are cancelled. Fails, naming each ticket's error, only if every ticket fails.
async fn fetch_any(tickets: Vec<String>, sha: &str) -> Result<Vec<u8>> {
    if tickets.is_empty() {
        bail!("no peer offers this content");
    }
    let mut fetches = tokio::task::JoinSet::new();
    for ticket in tickets {
        let sha = sha.to_owned();
        fetches.spawn(async move { fetch_one(&ticket, &sha).await });
    }
    let mut errors = Vec::new();
    while let Some(joined) = fetches.join_next().await {
        match joined.map_err(anyhow::Error::from).and_then(|fetched| fetched) {
            Ok(bytes) => return Ok(bytes),
            Err(error) => errors.push(format!("{error:#}")),
        }
    }
    bail!("every peer failed: {}", errors.join("; "))
}

/// Fetch the content `sha` through one ticket, within [`FETCH_TIMEOUT`].
async fn fetch_one(ticket: &str, sha: &str) -> Result<Vec<u8>> {
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

/// Write through a hidden sibling and rename it, so an editor never sees half a file.
/// The scanner skips hidden names, so the temp file never gets published.
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path.file_name().context("no file name")?.to_string_lossy();
    let temp = path.with_file_name(format!(".{name}.agent-knowledge"));
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
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
