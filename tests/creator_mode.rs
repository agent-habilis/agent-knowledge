mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use agent_knowledge::writers::{CreatorKey, Writers};
use common::{Peer, WAIT, wait_for};
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
    hash.split_once('.').expect("a creator hash has a suffix").0
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
#[derive(Clone, Copy)]
enum Leaver {
    Creator,
    Rehoster,
}

/// The creator and a rehoster hold the file. One of them leaves, and a new mounter must still get the file.
/// The entry of the leaver stays in the mesh with a ticket that no longer works.
async fn a_new_mounter_gets_the_file_after_a_holder_left(writers: Writers, leaver: Leaver) {
    let (c_dir, r_dir, b_dir) = (dir(), dir(), dir());
    write(&c_dir, "base.md", CREATOR_V1);
    let creator = Peer::share_as(c_dir.path(), writers).await;
    let hash = creator.ready.mesh.clone();
    let rehoster = mount(&r_dir, &hash).await;
    wait_for(&r_dir.path().join("base.md"), Some(CREATOR_V1)).await;
    // The rehoster publishes its copy a moment after it writes the file.
    tokio::time::sleep(SETTLE).await;

    let (stays, gone) = match leaver {
        Leaver::Creator => (rehoster, creator),
        Leaver::Rehoster => (creator, rehoster),
    };
    gone.stop().await;
    let late = mount(&b_dir, &hash).await;
    wait_for(&b_dir.path().join("base.md"), Some(CREATOR_V1)).await;

    late.stop().await;
    stays.stop().await;
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
    a_new_mounter_gets_the_file_after_a_holder_left(Writers::Creator, Leaver::Creator).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_larger_key_and_a_bad_ticket_does_not_block_a_mounter() {
    // The mesh breaks a tie by author key, and the keys are random. The leaver with the larger key
    // has its dead ticket first, so one of the 2 runs meets a bad ticket first.
    a_new_mounter_gets_the_file_after_a_holder_left(Writers::Creator, Leaver::Rehoster).await;
    a_new_mounter_gets_the_file_after_a_holder_left(Writers::Creator, Leaver::Creator).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn everybody_mode_falls_back_to_another_ticket() {
    a_new_mounter_gets_the_file_after_a_holder_left(Writers::Everybody, Leaver::Rehoster).await;
    a_new_mounter_gets_the_file_after_a_holder_left(Writers::Everybody, Leaver::Creator).await;
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
