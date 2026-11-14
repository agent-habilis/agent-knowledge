mod common;

use agent_knowledge::sync::Presence;
use common::{Peer, WAIT};
use tokio::sync::mpsc;

/// Wait up to [`WAIT`] for the next research notice; panic on none.
async fn next_research(events: &mut mpsc::UnboundedReceiver<Presence>) -> String {
    let found = tokio::time::timeout(WAIT, async {
        while let Some(event) = events.recv().await {
            if let Presence::Research(topic) = event {
                return topic;
            }
        }
        panic!("channel closed before a research notice");
    });
    found.await.unwrap_or_else(|_| panic!("no research notice within {WAIT:?}"))
}

/// A new research folder on one peer gives a notice on both peers, once; a folder that was
/// there at start gives none.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_research_folder_is_announced_on_every_peer() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(share_dir.path().join("research/old-topic")).unwrap();
    std::fs::write(share_dir.path().join("research/old-topic/old-topic.md"), "old").unwrap();
    let (share_tx, mut share_events) = mpsc::unbounded_channel();
    let (mount_tx, mut mount_events) = mpsc::unbounded_channel();
    let share = Peer::start(share_dir.path(), None, None, move |e| share_tx.send(e).unwrap()).await;
    let mount =
        Peer::start(mount_dir.path(), Some(share.ready.mesh.clone()), None, move |e| mount_tx.send(e).unwrap()).await;
    common::wait_for(&mount_dir.path().join("research/old-topic/old-topic.md"), Some("old")).await;

    std::fs::create_dir_all(share_dir.path().join("research/foo-bar")).unwrap();
    std::fs::write(share_dir.path().join("research/foo-bar/foo-bar.md"), "new").unwrap();

    assert_eq!(next_research(&mut share_events).await, "foo bar");
    assert_eq!(next_research(&mut mount_events).await, "foo bar");
    mount.stop().await;
    share.stop().await;
    let rest: Vec<_> = std::iter::from_fn(|| share_events.try_recv().ok())
        .chain(std::iter::from_fn(|| mount_events.try_recv().ok()))
        .filter(|e| matches!(e, Presence::Research(_)))
        .collect();
    assert!(rest.is_empty(), "extra notices: {rest:?}");
}
