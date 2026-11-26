mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agent_knowledge::scan::sha256_bytes;
use agent_knowledge::status::Status;
use agent_knowledge::sync::{Lookups, Options};
use agent_knowledge::writers::{self, CreatorKey, Writers, sign_entry};
use common::{Peer, WAIT, wait_for};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{BlobServer, ContentId};
use serde_json::{Value, json};
use tempfile::TempDir;

/// Long enough for an entry to cross the mesh several times, so a missing file is not a slow file.
const SETTLE: Duration = Duration::from_secs(6);

const CREATOR_V1: &str = "creator v1";

const KEY_ERROR: &str = "the creator key in the hash is not 64 lowercase hex digits of an Ed25519 key";

fn dir() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn write(dir: &TempDir, name: &str, text: &str) {
    std::fs::write(dir.path().join(name), text).unwrap();
}

fn read(dir: &TempDir, name: &str) -> Option<String> {
    std::fs::read_to_string(dir.path().join(name)).ok()
}

fn mesh_of(hash: &str) -> &str {
    hash.split_once('.').map_or(hash, |(mesh, _)| mesh)
}

/// A loopback mount of `hash` that writes its status to `status`.
async fn mount_with_status(dir: &TempDir, hash: &str, status: &Path) -> Peer {
    let options = Options {
        lookups: Lookups::Loopback,
        status_file: Some(status.to_path_buf()),
        ..common::options(dir.path(), Some(hash.to_owned()), None)
    };
    Peer::start_with(options, |_| {}).await
}

/// How many transfers the mount that writes `status` counts as failed. The mount writes it after each step.
fn failed_transfers(status: &Path) -> u64 {
    Status::read(status).unwrap().expect("the mount did not write its status").failed
}

async fn mount(dir: &TempDir, hash: &str) -> Peer {
    Peer::mount_loopback(dir.path(), hash).await
}

/// A creator share on `creator_dir` with one file, and one mounter for each of `mounter_dirs`.
async fn creator_mesh(creator_dir: &TempDir, mounter_dirs: &[&TempDir]) -> (String, Peer, Vec<Peer>) {
    write(creator_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_loopback(creator_dir.path(), Writers::Creator).await;
    let hash = creator.ready.mesh.clone();
    let mut mounters = Vec::new();
    for dir in mounter_dirs {
        mounters.push(mount(dir, &hash).await);
    }
    for dir in mounter_dirs {
        wait_for(&dir.path().join("base.md"), Some(CREATOR_V1)).await;
    }
    (hash, creator, mounters)
}

async fn stop_all(peers: Vec<Peer>) {
    for peer in peers {
        peer.stop().await;
    }
}

/// A file that a hostile peer offers. `entry` holds every field of its index entry except the ticket. The ticket serves
/// `bytes`, and `entry` can name other bytes.
struct Offer<'a> {
    path: &'a str,
    bytes: &'a [u8],
    entry: Value,
}

/// A live peer of the mesh that publishes an index of the files that it offers.
struct Hostile {
    member: Membership,
    blobs: BlobServer,
    author: String,
    published: usize,
    _dir: TempDir,
}

impl Hostile {
    /// Join `mesh` and publish an index of `offers`.
    async fn join(mesh: &str, offers: &[Offer<'_>]) -> Self {
        let mut hostile = Self::enter(mesh).await;
        let ts = offers[0].entry["ts"].clone();
        hostile.publish(offers, &ts).await;
        hostile
    }

    /// Join `mesh` and publish nothing yet, so the author key of the peer is not known. The join and the blob server use
    /// the loopback lookups of [`Lookups::Loopback`], as every other peer of the mesh does.
    async fn enter(mesh: &str) -> Self {
        let (sink, mut events) = membership::json_sink();
        let options = JoinOptions::default()
            .mesh(Some(mesh.to_owned()))
            .nick(None)
            .name(Some("agent-knowledge".to_owned()))
            .lookup(Lookups::Loopback.join())
            .transport(Lookups::Loopback.transports());
        let member = membership::join(&options, sink).await.unwrap();
        tokio::time::timeout(WAIT, async {
            while let Some(event) = events.recv().await {
                if serde_json::from_str::<Value>(&event).unwrap()["kind"] == "ready" {
                    return;
                }
            }
        })
        .await
        .expect("the hostile peer was not ready");

        let dir = dir();
        let blobs =
            BlobServer::start(Lookups::Loopback.blob(), dir.path().join("spool"), None, PayloadPath::RelayAllowed)
                .await
                .unwrap();
        Self { member, blobs, author: String::new(), published: 0, _dir: dir }
    }

    /// Publish an index of `offers` at `ts`, in place of the last index of this peer. Each publish makes an index that
    /// no other publish made, so the slice of the peer is known by the sha256 of its index.
    async fn publish(&mut self, offers: &[Offer<'_>], ts: &Value) {
        self.published += 1;
        let round = self.published;
        let mut files = serde_json::Map::new();
        for (number, offer) in offers.iter().enumerate() {
            let file = self._dir.path().join(format!("offer-{round}-{number}"));
            std::fs::write(&file, offer.bytes).unwrap();
            let sha = ContentId::new(offer.entry["sha256"].as_str().unwrap());
            let ticket = self.blobs.register(&file, sha).await.unwrap();
            let mut entry = offer.entry.clone();
            entry["ticket"] = json!(ticket.encode());
            files.insert(offer.path.to_owned(), entry);
        }

        let mut index = serde_json::to_vec(&files).unwrap();
        index.extend(std::iter::repeat_n(b' ', round));
        let index_sha = sha256_bytes(&index);
        let file = self._dir.path().join(format!("index-{round}.json"));
        std::fs::write(&file, &index).unwrap();
        let index_ticket = self.blobs.register(&file, ContentId::new(&index_sha)).await.unwrap();
        let merge = json!({ "index": { "ticket": index_ticket.encode(), "sha256": index_sha, "ts": ts } });
        self.member.request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply })).await.unwrap().unwrap();

        self.author = authors_of(&self.member)
            .await
            .into_iter()
            .find_map(|(author, sha)| (sha == index_sha).then_some(author))
            .expect("the hostile slice is not in the document");
    }

    async fn leave(self) {
        self.blobs.shutdown().await;
        let _ = membership::depart(self.member.node).await;
    }
}

/// The author and the index pointer of each slice in the document that `member` holds.
async fn authors_of(member: &Membership) -> Vec<(String, String)> {
    let text = member.request(|reply| Request::Inspect(Inspect::StateJson { reply })).await.unwrap();
    let document: Value = serde_json::from_str(&text).unwrap();
    document
        .as_object()
        .unwrap()
        .iter()
        .filter_map(|(author, slice)| Some((author.clone(), slice["index"]["sha256"].as_str()?.to_owned())))
        .collect()
}

/// Two live peers that will hold the same entry of `base.md`. They join and publish an empty index first, so their author
/// keys are known. The peer with the larger key is `bad`: its entry wins the tie between equal entries, so its ticket
/// comes first. The roles do not depend on luck.
async fn two_holders_ordered_by_key(mesh: &str, ts: &Value) -> (Hostile, Hostile) {
    let mut peers = [Hostile::enter(mesh).await, Hostile::enter(mesh).await];
    for peer in &mut peers {
        peer.publish(&[], ts).await;
    }
    let [first, second] = peers;
    if first.author > second.author { (first, second) } else { (second, first) }
}

/// The entry of `base.md` that both holders offer. In creator mode `key` signs it.
fn base_entry(mesh: &str, key: Option<&CreatorKey>) -> Value {
    let entry = json!({ "sha256": sha256_bytes(CREATOR_V1.as_bytes()), "ts": now_ms(), "size": CREATOR_V1.len() });
    key.map_or(entry.clone(), |key| sign_entry(key, mesh, "base.md", &entry).unwrap())
}

/// When the other holder appears.
#[derive(Clone, Copy, PartialEq)]
enum GoodHolder {
    /// It publishes after the new mounter has tried the bad ticket, so the mounter has no other ticket yet.
    AfterTheBadTicketFailed,
    /// It publishes with the bad holder, before the mounter starts.
    WithTheBadHolder,
}

/// The holder with the larger key serves other bytes, and its entry wins the tie. The other holder serves the file. A new
/// mounter must still get the file. No real peer holds the file, so the keys of the two holders alone decide the tie.
/// The anchor only makes the mesh. It has a creator key of its own, so it ignores both entries.
///
/// With `AfterTheBadTicketFailed` the mounter sees only the bad ticket at first, so the file can come only from a
/// ticket that is added to the winner later. With `WithTheBadHolder` the mounter must not count a failed download.
async fn a_holder_that_wins_the_tie_with_bad_bytes_does_not_block_a_mounter(writers: Writers, good: GoodHolder) {
    let (anchor_dir, b_dir, status_dir) = (dir(), dir(), dir());
    let status = status_dir.path().join("status");
    let anchor = Peer::share_loopback(anchor_dir.path(), Writers::Creator).await;
    let mesh = mesh_of(&anchor.ready.mesh).to_owned();
    let key = (writers == Writers::Creator).then(|| CreatorKey::generate().unwrap());
    let hash = writers::encode_hash(&mesh, key.as_ref());
    let entry = base_entry(&mesh, key.as_ref());
    let ts = entry["ts"].clone();
    let (mut bad_holder, mut good_holder) = two_holders_ordered_by_key(&mesh, &ts).await;
    let bad_offer = Offer { path: "base.md", bytes: b"creator v2", entry: entry.clone() };
    let good_offer = Offer { path: "base.md", bytes: CREATOR_V1.as_bytes(), entry };

    bad_holder.publish(&[bad_offer], &ts).await;
    let late = if good == GoodHolder::AfterTheBadTicketFailed {
        let late = mount_with_status(&b_dir, &hash, &status).await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(read(&b_dir, "base.md"), None, "the bad bytes were applied");
        good_holder.publish(&[good_offer], &ts).await;
        late
    } else {
        good_holder.publish(&[good_offer], &ts).await;
        mount_with_status(&b_dir, &hash, &status).await
    };
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    if good == GoodHolder::WithTheBadHolder {
        tokio::time::sleep(SETTLE).await;
        assert_eq!(failed_transfers(&status), 0, "the mounter counted a failed download");
    }
    late.stop().await;
    anchor.stop().await;
    bad_holder.leave().await;
    good_holder.leave().await;
}

/// The creator leaves, and a new mounter must still get the file from the rehoster.
async fn a_new_mounter_gets_the_file_after_the_creator_left() {
    let (c_dir, r_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_loopback(c_dir.path(), Writers::Creator).await;
    let hash = creator.ready.mesh.clone();
    let rehoster = mount(&r_dir, &hash).await;
    wait_for(&r_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    // The rehoster publishes its copy a moment after it writes the file.
    tokio::time::sleep(SETTLE).await;

    creator.stop().await;
    let late = mount(&b_dir, &hash).await;
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    late.stop().await;
    rehoster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_write_is_not_applied_on_other_peers() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;

    write(&a_dir, "new.md", "from a");
    write(&a_dir, "base.md", "edited by a");
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&a_dir, "new.md").as_deref(), Some("from a"), "A lost its own new file");
    assert_eq!(read(&b_dir, "new.md"), None);
    assert_eq!(read(&b_dir, "base.md").as_deref(), Some(CREATOR_V1));
    assert_eq!(read(&c_dir, "new.md"), None);
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));

    creator.stop().await;
    stop_all(mounters).await;
}

/// The entry of `base.md` in the index that a peer keeps in its folder.
fn saved_entry(dir: &TempDir) -> Value {
    let saved = std::fs::read(dir.path().join(".agent-knowledge-index.json")).unwrap();
    serde_json::from_slice::<Value>(&saved).unwrap()["own"]["base.md"].clone()
}

fn now_ms() -> u64 {
    let since_epoch = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    u64::try_from(since_epoch.as_millis()).unwrap()
}

/// An entry of a file with `bytes`, 1 minute newer than any entry of the creator now.
fn newer_entry(bytes: &[u8]) -> Value {
    json!({ "sha256": sha256_bytes(bytes), "ts": now_ms() + 60_000, "size": bytes.len() })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_entry_from_a_member_is_not_applied() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "second.md", "second v1");
    let (hash, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;
    wait_for(&a_dir.path().join("second.md"), Some("second v1")).await;
    wait_for(&b_dir.path().join("second.md"), Some("second v1")).await;
    let mesh = mesh_of(&hash);
    let other_key = CreatorKey::generate().unwrap();
    // An entry that holds each signed field, signed by a key that is not the creator key.
    let forged = |path: &str, bytes: &[u8]| sign_entry(&other_key, mesh, path, &newer_entry(bytes)).unwrap();
    let offers = [
        Offer { path: "unsigned.md", bytes: b"unsigned", entry: newer_entry(b"unsigned") },
        Offer { path: "forged.md", bytes: b"forged", entry: forged("forged.md", b"forged") },
        Offer { path: "base.md", bytes: b"forged base", entry: forged("base.md", b"forged base") },
        Offer { path: "second.md", bytes: b"unsigned second", entry: newer_entry(b"unsigned second") },
    ];

    let hostile = Hostile::join(mesh, &offers).await;
    tokio::time::sleep(SETTLE).await;
    // The marker shows that the mounters follow the creator after the hostile peer joined.
    write(&c_dir, "marker.md", "marker");
    wait_for(&a_dir.path().join("marker.md"), Some("marker")).await;
    wait_for(&b_dir.path().join("marker.md"), Some("marker")).await;
    tokio::time::sleep(SETTLE).await;

    for mounter in [&a_dir, &b_dir] {
        assert_eq!(read(mounter, "unsigned.md"), None, "an unsigned entry was applied");
        assert_eq!(read(mounter, "forged.md"), None, "an entry with another key was applied");
        assert_eq!(read(mounter, "base.md").as_deref(), Some(CREATOR_V1), "a forged entry replaced base.md");
        assert_eq!(read(mounter, "second.md").as_deref(), Some("second v1"), "an unsigned entry replaced second.md");
    }
    creator.stop().await;
    stop_all(mounters).await;
    hostile.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_folder_syncs_to_a_new_mounter_after_the_creator_leaves() {
    a_new_mounter_gets_the_file_after_the_creator_left().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_larger_key_and_a_bad_ticket_does_not_block_a_mounter() {
    a_holder_that_wins_the_tie_with_bad_bytes_does_not_block_a_mounter(
        Writers::Creator,
        GoodHolder::AfterTheBadTicketFailed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn everybody_mode_falls_back_to_another_ticket() {
    a_holder_that_wins_the_tie_with_bad_bytes_does_not_block_a_mounter(
        Writers::Everybody,
        GoodHolder::AfterTheBadTicketFailed,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mounter_reports_no_failed_download_when_a_holder_serves_bad_bytes() {
    a_holder_that_wins_the_tie_with_bad_bytes_does_not_block_a_mounter(Writers::Creator, GoodHolder::WithTheBadHolder)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rehoster_that_restarts_after_the_creator_left_still_serves() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;

    creator.stop().await;
    mounters.remove(0).stop().await;
    let a = mount(&a_dir, &hash).await;
    let b = mount(&b_dir, &hash).await;
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rehoster_restart_keeps_the_creator_signatures() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    tokio::time::sleep(SETTLE).await;

    mounters.remove(0).stop().await;
    let a = mount(&a_dir, &hash).await;
    tokio::time::sleep(SETTLE).await;
    // The rehoster copies the entry of the creator with its ts, signature and size, and a ticket of its own.
    let creator_entry = saved_entry(&c_dir);
    let rehoster_entry = saved_entry(&a_dir);
    for field in ["sha256", "ts", "sig", "size"] {
        assert!(creator_entry[field].is_string() || creator_entry[field].is_u64(), "the creator has no {field}");
        assert_eq!(rehoster_entry[field], creator_entry[field], "the rehoster changed {field} after a restart");
    }
    creator.stop().await;
    let b = mount(&b_dir, &hash).await;
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_edit_is_reverted_and_the_next_creator_change_applies() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;

    write(&a_dir, "base.md", "edited by a");
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    write(&c_dir, "base.md", "creator v2");
    wait_for(&a_dir.path().join("base.md"), Some("creator v2")).await;
    wait_for(&b_dir.path().join("base.md"), Some("creator v2")).await;

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_creator_delete_is_reverted() {
    let (c_dir, a_dir) = (dir(), dir());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir]).await;

    std::fs::remove_file(a_dir.path().join("base.md")).unwrap();
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_peer_reverts_an_edit_made_while_stopped() {
    let (c_dir, a_dir) = (dir(), dir());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    mounters.remove(0).stop().await;

    write(&a_dir, "base.md", "offline edit on a");
    let a = mount(&a_dir, &hash).await;

    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));
    creator.stop().await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_given_edited_file_is_removed_when_the_creator_deletes_it() {
    let (c_dir, a_dir) = (dir(), dir());
    let (hash, creator, mut mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    tokio::time::sleep(SETTLE).await;
    mounters.remove(0).stop().await;

    write(&a_dir, "base.md", "offline edit on a");
    std::fs::remove_file(c_dir.path().join("base.md")).unwrap();
    let a = mount(&a_dir, &hash).await;

    wait_for(&a_dir.path().join("base.md"), None).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(read(&a_dir, "base.md"), None, "the file that the mesh gave came back");
    creator.stop().await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mounter_keeps_its_own_file_at_a_tombstoned_path() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (hash, creator, mounters) = creator_mesh(&c_dir, &[&a_dir]).await;
    std::fs::remove_file(c_dir.path().join("base.md")).unwrap();
    wait_for(&a_dir.path().join("base.md"), None).await;
    write(&c_dir, "marker.md", "marker");

    write(&b_dir, "base.md", "mine");
    let b = mount(&b_dir, &hash).await;
    // The marker shows that B has the creator state, so a kept file is a decision and not a slow join.
    wait_for(&b_dir.path().join("marker.md"), Some("marker")).await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "base.md").as_deref(), Some("mine"));
    assert_eq!(read(&c_dir, "base.md"), None, "B's file reached the creator");
    assert_eq!(read(&a_dir, "base.md"), None, "B's file reached the other mounter");
    creator.stop().await;
    b.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mounter_keeps_a_file_made_after_join_at_a_tombstoned_path() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    let (_, creator, mounters) = creator_mesh(&c_dir, &[&a_dir, &b_dir]).await;
    std::fs::remove_file(c_dir.path().join("base.md")).unwrap();
    wait_for(&a_dir.path().join("base.md"), None).await;
    wait_for(&b_dir.path().join("base.md"), None).await;

    write(&b_dir, "base.md", "mine");
    write(&c_dir, "marker.md", "marker");
    wait_for(&b_dir.path().join("marker.md"), Some("marker")).await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "base.md").as_deref(), Some("mine"));
    assert_eq!(read(&c_dir, "base.md"), None, "B's file reached the creator");
    assert_eq!(read(&a_dir, "base.md"), None, "B's file reached the other mounter");
    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_with_a_malformed_creator_suffix_fails() {
    let (c_dir, target) = (dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_loopback(c_dir.path(), Writers::Creator).await;

    let mut mount = MountProcess::start(&format!("{}.zz", mesh_of(&creator.ready.mesh)), target.path());

    let code = mount.exit_code().await;

    let stderr = std::fs::read_to_string(&mount.stderr).unwrap_or_default();
    assert_eq!(code, Some(1), "mount did not fail with a malformed suffix: {stderr:?}");
    assert!(stderr.contains(KEY_ERROR), "unexpected error: {stderr}");
    creator.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_with_another_creator_key_applies_nothing() {
    let (c_dir, a_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_loopback(c_dir.path(), Writers::Creator).await;
    let hash = creator.ready.mesh.clone();
    let other_key = CreatorKey::generate().unwrap().public_hex();

    // A mounts the true hash at the same time as B. When A has the file, the mesh has delivered it.
    let a = mount(&a_dir, &hash).await;
    let b = mount(&b_dir, &format!("{}.{other_key}", mesh_of(&hash))).await;
    wait_for(&a_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    tokio::time::sleep(SETTLE).await;

    assert_eq!(read(&b_dir, "base.md"), None);
    creator.stop().await;
    a.stop().await;
    b.stop().await;
}

/// `agent-knowledge mount` as a child process, with its stderr in a file.
struct MountProcess {
    child: Child,
    stderr: std::path::PathBuf,
    _home: TempDir,
    _log: TempDir,
}

impl MountProcess {
    fn start(hash: &str, target: &Path) -> Self {
        let (home, log) = (dir(), dir());
        let stderr = log.path().join("stderr");
        let child = Command::new(env!("CARGO_BIN_EXE_agent-knowledge"))
            .args(["mount", hash])
            .arg(target)
            .env("AGENT_KNOWLEDGE_HOME", home.path())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        Self { child, stderr, _home: home, _log: log }
    }

    /// The exit code of the process. A process that runs on after [`WAIT`] is a failure, and not a hang.
    async fn exit_code(&mut self) -> Option<i32> {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code();
            }
            let stderr = std::fs::read_to_string(&self.stderr).unwrap_or_default();
            assert!(tokio::time::Instant::now() < deadline, "the process did not exit: {stderr:?}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn stderr_text(&self) -> String {
        std::fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    /// Wait until stderr holds `line`; fail after [`WAIT`].
    async fn wait_for_line(&self, line: &str) {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let text = std::fs::read_to_string(&self.stderr).unwrap_or_default();
            if text.lines().any(|got| got.contains(line)) {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "stderr has no {line:?}: {text:?}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Drop for MountProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_two_read_only_lines_are_printed() {
    let (c_dir, target) = (dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(c_dir.path(), Writers::Creator).await;
    let mounter = MountProcess::start(&creator.ready.mesh, target.path());
    wait_for(&target.path().join("base.md"), Some(CREATOR_V1)).await;

    write(&target, "base.md", "edited by the mounter");
    mounter.wait_for_line("read-only share: reverted base.md").await;
    write(&target, "mine.md", "only on the mounter");
    mounter.wait_for_line("read-only share: not published mine.md").await;

    assert_eq!(read(&c_dir, "mine.md"), None, "the file of the mounter reached the creator");
    creator.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_creator_update_prints_the_download_and_no_reverted_line() {
    let (c_dir, target) = (dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(c_dir.path(), Writers::Creator).await;
    let mounter = MountProcess::start(&creator.ready.mesh, target.path());
    wait_for(&target.path().join("base.md"), Some(CREATOR_V1)).await;

    write(&c_dir, "base.md", "creator v2");
    wait_for(&target.path().join("base.md"), Some("creator v2")).await;
    tokio::time::sleep(SETTLE).await;

    let stderr = mounter.stderr_text();
    assert!(
        !stderr.contains("read-only share: reverted"),
        "an update of the creator was reported as a revert: {stderr}"
    );
    assert!(stderr.contains("↓ base.md"), "the update was not reported: {stderr}");
    creator.stop().await;
}
