mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agent_knowledge::scan::sha256_bytes;
use agent_knowledge::writers::{CreatorKey, Writers};
use common::{Peer, WAIT, wait_for};
use habilis_network::membership::{self, Inspect, JoinOptions, Membership, Messaging, Request};
use habilis_network::net::direct::PayloadPath;
use habilis_network::ops::blob::{BlobServer, ContentId};
use habilis_network::protocol::{Lookup, LookupOpts, Transport};
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

async fn mount(dir: &TempDir, hash: &str) -> Peer {
    Peer::start(dir.path(), Some(hash.to_owned()), None, |_| {}).await
}

/// A creator share on `creator_dir` with one file, and one mounter for each of `mounter_dirs`.
async fn creator_mesh(creator_dir: &TempDir, mounter_dirs: &[&TempDir]) -> (String, Peer, Vec<Peer>) {
    write(creator_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(creator_dir.path(), Writers::Creator).await;
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

/// Which holder of the file leaves before the new mounter joins.
/// A live peer of the mesh that offers `base.md` with the signed entry of the creator and a ticket for other bytes.
struct Hostile {
    member: Membership,
    blobs: BlobServer,
    author: String,
    _dir: TempDir,
}

impl Hostile {
    /// Join `mesh` and publish an index with a copy of `entry`, whose ticket serves other bytes than the entry names.
    async fn join(mesh: &str, entry: &Value) -> Self {
        let (sink, mut events) = membership::json_sink();
        let options = JoinOptions::default()
            .mesh(Some(mesh.to_owned()))
            .nick(None)
            .name(Some("agent-knowledge".to_owned()))
            .lookup([Lookup::Mdns, Lookup::Dht, Lookup::Relay])
            .transport([Transport::Udp, Transport::WebRtc, Transport::Multihop, Transport::Gossip, Transport::Relay]);
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
            BlobServer::start(LookupOpts::public_preset(), dir.path().join("spool"), None, PayloadPath::RelayAllowed)
                .await
                .unwrap();
        let wrong = dir.path().join("wrong");
        std::fs::write(&wrong, "other bytes").unwrap();
        let ticket = blobs.register(&wrong, ContentId::new(entry["sha256"].as_str().unwrap())).await.unwrap();
        let mut offered = entry.clone();
        offered["ticket"] = json!(ticket.encode());

        let index = serde_json::to_vec(&json!({ "base.md": offered })).unwrap();
        let index_sha = sha256_bytes(&index);
        let file = dir.path().join("index.json");
        std::fs::write(&file, &index).unwrap();
        let index_ticket = blobs.register(&file, ContentId::new(&index_sha)).await.unwrap();
        let merge = json!({ "index": { "ticket": index_ticket.encode(), "sha256": index_sha, "ts": entry["ts"] } });
        member.request(|reply| Request::Messaging(Messaging::StateMerge { merge, reply })).await.unwrap().unwrap();

        let author = authors_of(&member)
            .await
            .into_iter()
            .find_map(|(author, sha)| (sha == index_sha).then_some(author))
            .expect("the hostile slice is not in the document");
        Self { member, blobs, author, _dir: dir }
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

/// A hostile peer whose author key is larger than the key of each peer that holds the file, so its entry wins the
/// tie between equal entries and its bad ticket comes first. Each try joins with a new key.
async fn a_hostile_peer_that_wins_the_tie(mesh: &str, entry: &Value, holders: usize) -> Hostile {
    let mut earlier: Vec<String> = Vec::new();
    for _ in 0..20 {
        let hostile = Hostile::join(mesh, entry).await;
        let deadline = tokio::time::Instant::now() + WAIT;
        let others = loop {
            let others: Vec<String> = authors_of(&hostile.member)
                .await
                .into_iter()
                .map(|(author, _)| author)
                .filter(|author| *author != hostile.author && !earlier.contains(author))
                .collect();
            if others.len() >= holders {
                break others;
            }
            assert!(tokio::time::Instant::now() < deadline, "the hostile peer sees only {others:?}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        if others.iter().all(|author| *author < hostile.author) {
            return hostile;
        }
        earlier.push(hostile.author.clone());
        hostile.leave().await;
    }
    panic!("no hostile peer with the largest key in 20 tries");
}

/// The creator and a rehoster hold the file, and a live peer wins the tie with a ticket that gives other bytes. A new
/// mounter must still get the file from a good holder.
async fn a_hostile_holder_that_wins_the_tie_does_not_block_a_mounter(writers: Writers) {
    let (c_dir, r_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(c_dir.path(), writers).await;
    let hash = creator.ready.mesh.clone();
    let rehoster = mount(&r_dir, &hash).await;
    wait_for(&r_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    // The rehoster publishes its copy a moment after it writes the file.
    tokio::time::sleep(SETTLE).await;
    let saved: Value =
        serde_json::from_slice(&std::fs::read(c_dir.path().join(".agent-knowledge-index.json")).unwrap()).unwrap();
    let entry = saved["own"]["base.md"].clone();

    let hostile = a_hostile_peer_that_wins_the_tie(mesh_of(&hash), &entry, 2).await;
    let late = mount(&b_dir, &hash).await;
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    late.stop().await;
    rehoster.stop().await;
    creator.stop().await;
    hostile.leave().await;
}

/// The creator leaves, and a new mounter must still get the file from the rehoster.
async fn a_new_mounter_gets_the_file_after_the_creator_left() {
    let (c_dir, r_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(c_dir.path(), Writers::Creator).await;
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

    assert_eq!(read(&b_dir, "new.md"), None);
    assert_eq!(read(&b_dir, "base.md").as_deref(), Some(CREATOR_V1));
    assert_eq!(read(&c_dir, "new.md"), None);
    assert_eq!(read(&c_dir, "base.md").as_deref(), Some(CREATOR_V1));

    creator.stop().await;
    stop_all(mounters).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_folder_syncs_to_a_new_mounter_after_the_creator_leaves() {
    a_new_mounter_gets_the_file_after_the_creator_left().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_larger_key_and_a_bad_ticket_does_not_block_a_mounter() {
    a_hostile_holder_that_wins_the_tie_does_not_block_a_mounter(Writers::Creator).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn everybody_mode_falls_back_to_another_ticket() {
    a_hostile_holder_that_wins_the_tie_does_not_block_a_mounter(Writers::Everybody).await;
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
    let creator = Peer::share_as(c_dir.path(), Writers::Creator).await;

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
    let creator = Peer::share_as(c_dir.path(), Writers::Creator).await;
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
