mod common;

use common::{Peer, REJOIN_WAIT, wait_for, wait_for_within};

/// A file outlives its author: once a peer has downloaded it, a peer that joins after the author
/// left still gets it, from the peer that has it.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_peer_gets_a_file_after_its_author_left() {
    let author_dir = tempfile::tempdir().unwrap();
    let keeper_dir = tempfile::tempdir().unwrap();
    let late_dir = tempfile::tempdir().unwrap();
    std::fs::write(author_dir.path().join("note.md"), "outlives its author").unwrap();

    let author = Peer::start(author_dir.path(), None, None, |_| {}).await;
    let mesh = author.ready.mesh.clone();
    let keeper = Peer::start(keeper_dir.path(), Some(mesh.clone()), None, |_| {}).await;
    wait_for(&keeper_dir.path().join("note.md"), Some("outlives its author")).await;
    author.stop().await;

    // Without the keeper's offer this never arrives: the only ticket is the author's.
    let late = Peer::start(late_dir.path(), Some(mesh), None, |_| {}).await;
    wait_for_within(&late_dir.path().join("note.md"), Some("outlives its author"), REJOIN_WAIT).await;

    late.stop().await;
    keeper.stop().await;
}
