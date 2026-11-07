use std::path::Path;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

const QUIET: Duration = Duration::from_millis(300);

/// Sends one tick after each burst of changes under `root`, once the folder has been quiet for 300ms.
/// The tick carries no paths: the receiver rescans, which also catches events the OS coalesced or dropped.
/// The returned watcher must stay alive for the ticks to keep coming.
pub fn watch(root: &Path) -> notify::Result<(notify::RecommendedWatcher, mpsc::Receiver<()>)> {
    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.is_ok() {
            let _ = raw_tx.send(());
        }
    })?;
    watcher.watch(root, RecursiveMode::Recursive)?;

    let (tick_tx, tick_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        while raw_rx.recv().await.is_some() {
            while let Ok(Some(())) = tokio::time::timeout(QUIET, raw_rx.recv()).await {}
            // A full channel already holds a pending tick, which covers this burst too.
            let _ = tick_tx.try_send(());
        }
    });
    Ok((watcher, tick_rx))
}
