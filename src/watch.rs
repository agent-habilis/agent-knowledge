use std::path::Path;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::sync::INDEX_FILE;

const QUIET: Duration = Duration::from_millis(300);

/// Sends one tick after each burst of changes under `root`, once the folder has been quiet for 300ms.
/// A change to the own index file, or to its temporary copy, is no tick: the loop writes it itself, and a scan after
/// each of its own writes would be work with no new content. Neither is a report that `root` itself was created or had
/// its metadata changed: some systems send one beside each file that is created in it, and one late for a folder that
/// is new.
/// The tick carries no paths: the receiver rescans, which also catches events the OS coalesced or dropped.
/// The returned watcher must stay alive for the ticks to keep coming.
pub fn watch(root: &Path) -> notify::Result<(notify::RecommendedWatcher, mpsc::Receiver<()>)> {
    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel();
    let folder = root.to_path_buf();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.is_ok_and(|event| !is_own_index(&event) && !is_folder_metadata(&event, &folder)) {
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

/// Whether `event` touches only the own index file or its temporary copy.
fn is_own_index(event: &notify::Event) -> bool {
    !event.paths.is_empty()
        && event.paths.iter().all(|path| {
            path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.starts_with(INDEX_FILE))
        })
}

/// Whether `event` only reports that the watched folder itself was created, or that its metadata changed.
fn is_folder_metadata(event: &notify::Event, folder: &Path) -> bool {
    use notify::EventKind::{Create, Modify};
    use notify::event::ModifyKind::Metadata;
    matches!(event.kind, Create(_) | Modify(Metadata(_)))
        && !event.paths.is_empty()
        && event.paths.iter().all(|path| path == folder)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn ticked(ticks: &mut mpsc::Receiver<()>) -> bool {
        tokio::time::timeout(Duration::from_secs(2), ticks.recv()).await.is_ok()
    }

    #[tokio::test]
    async fn a_write_of_the_own_index_file_is_no_tick() {
        let dir = tempfile::tempdir().unwrap();
        let (_watcher, mut ticks) = watch(dir.path()).unwrap();
        let temp = dir.path().join(format!("{INDEX_FILE}.tmp"));
        std::fs::write(&temp, "{}").unwrap();
        std::fs::rename(&temp, dir.path().join(INDEX_FILE)).unwrap();
        assert!(!ticked(&mut ticks).await, "the own index file made a tick");
    }

    #[test]
    fn a_report_about_the_folder_itself_is_no_change() {
        let folder = Path::new("/watched");
        let metadata = notify::EventKind::Modify(notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Any));
        let on = |kind, path: &str| notify::Event::new(kind).add_path(path.into());
        assert!(is_folder_metadata(&on(metadata, "/watched"), folder));
        assert!(!is_folder_metadata(&on(metadata, "/watched/a.md"), folder));
        let created = notify::EventKind::Create(notify::event::CreateKind::Folder);
        assert!(is_folder_metadata(&on(created, "/watched"), folder));
        assert!(!is_folder_metadata(&on(created, "/watched/sub"), folder));
        let removed = notify::EventKind::Remove(notify::event::RemoveKind::Folder);
        assert!(!is_folder_metadata(&on(removed, "/watched"), folder), "a removed folder is a change");
    }

    #[tokio::test]
    async fn a_write_of_another_file_is_a_tick() {
        let dir = tempfile::tempdir().unwrap();
        let (_watcher, mut ticks) = watch(dir.path()).unwrap();
        std::fs::write(dir.path().join("note.md"), "x").unwrap();
        assert!(ticked(&mut ticks).await, "a note made no tick");
    }
}
