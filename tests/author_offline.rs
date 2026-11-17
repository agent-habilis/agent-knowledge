mod common;

use agent_knowledge::sync;
use common::wait_for;
use tokio::sync::{oneshot, watch};

async fn stopped(mut stop: watch::Receiver<()>) {
    let _ = stop.changed().await;
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
        common::options(author_dir.path(), None, None),
        move |ready: &sync::Ready| hash_tx.send(ready.mesh.clone()).unwrap(),
        |_| {},
        stopped(author_stop_rx),
    ));
    let hash = hash_rx.await.unwrap();

    let (rest_stop, rest_stop_rx) = watch::channel(());
    let first = tokio::spawn(sync::run(
        common::options(first_dir.path(), Some(hash.clone()), None),
        |_: &sync::Ready| {},
        |_| {},
        stopped(rest_stop_rx.clone()),
    ));
    wait_for(&first_dir.path().join("start.md"), Some("from author")).await;

    author_stop.send(()).unwrap();
    author.await.unwrap().unwrap();

    let late = tokio::spawn(sync::run(
        common::options(late_dir.path(), Some(hash), None),
        |_: &sync::Ready| {},
        |_| {},
        stopped(rest_stop_rx),
    ));
    wait_for(&late_dir.path().join("start.md"), Some("from author")).await;

    rest_stop.send(()).unwrap();
    first.await.unwrap().unwrap();
    late.await.unwrap().unwrap();
}
