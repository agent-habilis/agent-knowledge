mod common;

use agent_knowledge::sync;
use common::wait_for;
use tokio::sync::{oneshot, watch};

fn stopped(mut stop: watch::Receiver<()>) -> impl Future<Output = ()> {
    async move {
        let _ = stop.changed().await;
    }
}

/// A file must reach a new mount after its author left, because the peers that hold it serve it.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_mount_gets_a_file_after_its_author_left() {
    let author_dir = tempfile::tempdir().unwrap();
    let first_dir = tempfile::tempdir().unwrap();
    let late_dir = tempfile::tempdir().unwrap();
    std::fs::write(author_dir.path().join("start.md"), "from author").unwrap();

    let (author_stop, author_stop_rx) = watch::channel(());
    let (hash_tx, hash_rx) = oneshot::channel();
    let author = tokio::spawn(sync::run(
        author_dir.path().to_path_buf(),
        None,
        move |hash: &str| hash_tx.send(hash.to_owned()).unwrap(),
        stopped(author_stop_rx),
    ));
    let hash = hash_rx.await.unwrap();

    let (rest_stop, rest_stop_rx) = watch::channel(());
    let first = tokio::spawn(sync::run(
        first_dir.path().to_path_buf(),
        Some(hash.clone()),
        |_: &str| {},
        stopped(rest_stop_rx.clone()),
    ));
    wait_for(&first_dir.path().join("start.md"), Some("from author")).await;

    author_stop.send(()).unwrap();
    author.await.unwrap().unwrap();

    let late = tokio::spawn(sync::run(late_dir.path().to_path_buf(), Some(hash), |_: &str| {}, stopped(rest_stop_rx)));
    wait_for(&late_dir.path().join("start.md"), Some("from author")).await;

    rest_stop.send(()).unwrap();
    first.await.unwrap().unwrap();
    late.await.unwrap().unwrap();
}
