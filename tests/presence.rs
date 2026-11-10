mod common;

use agent_knowledge::sync::{self, Presence, Ready};
use common::{Peer, WAIT, options};
use tokio::sync::mpsc;

async fn next(events: &mut mpsc::UnboundedReceiver<Presence>, want: &Presence) {
    let found = tokio::time::timeout(WAIT, async {
        while let Some(event) = events.recv().await {
            if &event == want {
                return;
            }
        }
        panic!("presence channel closed before {want:?}");
    });
    found.await.unwrap_or_else(|_| panic!("no {want:?} within {WAIT:?}"));
}

/// A peer without a nickname gets a random one; the sharing peer hears the mounting peer join,
/// then leave, by the nickname it chose.
#[tokio::test(flavor = "multi_thread")]
async fn peers_get_nicknames_and_hear_join_and_leave() {
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    let (presence_tx, mut presence) = mpsc::unbounded_channel();
    let share = Peer::start(share_dir.path(), None, None, move |event| presence_tx.send(event).unwrap()).await;
    assert!(!share.ready.nick.is_empty(), "the mesh mints a nickname");

    let mount = Peer::start(mount_dir.path(), Some(share.ready.mesh.clone()), Some("b"), |_| {}).await;
    assert_eq!(mount.ready.nick, "b");

    next(&mut presence, &Presence::Joined("b".to_owned())).await;
    mount.stop().await;
    next(&mut presence, &Presence::Left("b".to_owned())).await;
    share.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_nickname_fails_before_joining() {
    let dir = tempfile::tempdir().unwrap();
    let result = sync::run(
        options(dir.path(), None, Some("has space")),
        |_: &Ready| panic!("an invalid nickname never gets ready"),
        |_: Presence| {},
        std::future::pending(),
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("nickname"));
}
