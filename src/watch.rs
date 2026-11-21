use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::sync::INDEX_FILE;

const QUIET: Duration = Duration::from_millis(300);

/// How long the timer of the loop waits for a folder that keeps changing: one period of that timer. A folder that never
/// is quiet, for example one with a log file that is written every 100 ms, would otherwise get no tick and no publish.
const TIMER_HOLD: Duration = Duration::from_secs(2);

/// When the folder changed, as far as a tick is concerned. The timer of the loop reads it, so it does not publish a file
/// that is still being written, which the tick waits for. It waits for one period of the timer at most.
#[derive(Clone, Default)]
pub struct Activity(Arc<Mutex<Changes>>);

#[derive(Default)]
struct Changes {
    /// The last change, and the first change of the run of changes that it ends, none of them `QUIET` apart.
    last: Option<Instant>,
    since: Option<Instant>,
}

impl Activity {
    fn touch(&self) {
        self.touch_at(Instant::now());
    }

    fn touch_at(&self, now: Instant) {
        let mut changes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if changes.last.is_none_or(|last| now.duration_since(last) >= QUIET) {
            changes.since = Some(now);
        }
        changes.last = Some(now);
    }

    /// Whether the timer should leave the publish to the tick: the folder changed less than the quiet time ago, so a
    /// tick is still to come, and the run of changes is younger than [`TIMER_HOLD`].
    pub fn holds_timer(&self) -> bool {
        self.holds_timer_at(Instant::now())
    }

    fn holds_timer_at(&self, now: Instant) -> bool {
        let changes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let busy = changes.last.is_some_and(|last| now.duration_since(last) < QUIET);
        busy && changes.since.is_some_and(|since| now.duration_since(since) < TIMER_HOLD)
    }
}

/// Sends one tick after each burst of changes under `root`, once the folder has been quiet for 300ms.
/// A change to the own index file, or to its temporary copy, is no tick: the loop writes it itself, and a scan after
/// each of its own writes would be work with no new content. Neither is a report that `root` itself was created or had
/// its metadata changed: some systems send one beside each file that is created in it, and one late for a folder that
/// is new.
/// The tick carries no paths: the receiver rescans, which also catches events the OS coalesced or dropped.
/// The returned watcher must stay alive for the ticks to keep coming.
pub fn watch(root: &Path) -> notify::Result<(notify::RecommendedWatcher, mpsc::Receiver<()>, Activity)> {
    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel();
    let folder = root.to_path_buf();
    let activity = Activity::default();
    let touched = activity.clone();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.is_ok_and(|event| is_change(&event, &folder)) {
            touched.touch();
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
    Ok((watcher, tick_rx, activity))
}

/// Whether `event` is a change to the folder that a scan could find. Opening or closing a file for a read is not one:
/// a system that reports each open, such as inotify, would make a tick for every read of the folder, and so for every scan.
fn is_change(event: &notify::Event, folder: &Path) -> bool {
    !matches!(event.kind, notify::EventKind::Access(_)) && !is_own_index(event) && !is_folder_metadata(event, folder)
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
        let (_watcher, mut ticks, _activity) = watch(dir.path()).unwrap();
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

    #[test]
    fn the_timer_waits_for_a_folder_that_changed_a_moment_ago() {
        let (activity, start) = (Activity::default(), Instant::now());
        assert!(!activity.holds_timer_at(start), "a folder with no change held the timer");
        activity.touch_at(start);
        assert!(activity.holds_timer_at(start + Duration::from_millis(100)));
        assert!(!activity.holds_timer_at(start + QUIET + Duration::from_millis(1)), "a quiet folder held the timer");
    }

    #[test]
    fn the_timer_does_not_wait_for_a_folder_that_never_is_quiet_longer_than_one_period() {
        let (activity, start) = (Activity::default(), Instant::now());
        for step in 0..40 {
            activity.touch_at(start + Duration::from_millis(100 * step));
        }
        assert!(
            activity.holds_timer_at(start + Duration::from_millis(3900) - TIMER_HOLD),
            "held for less than a period"
        );
        assert!(!activity.holds_timer_at(start + Duration::from_millis(3950)), "a busy folder held the timer for good");
    }

    #[test]
    fn a_new_run_of_changes_holds_the_timer_again() {
        let (activity, start) = (Activity::default(), Instant::now());
        for step in 0..40 {
            activity.touch_at(start + Duration::from_millis(100 * step));
        }
        let later = start + Duration::from_secs(10);
        activity.touch_at(later);
        assert!(activity.holds_timer_at(later + Duration::from_millis(100)));
    }

    #[test]
    fn opening_a_file_is_no_change() {
        let folder = Path::new("/watched");
        let on = |kind, path: &str| notify::Event::new(kind).add_path(path.into());
        let open = notify::EventKind::Access(notify::event::AccessKind::Open(notify::event::AccessMode::Any));
        let close = notify::EventKind::Access(notify::event::AccessKind::Close(notify::event::AccessMode::Read));
        let write = notify::EventKind::Modify(notify::event::ModifyKind::Data(notify::event::DataChange::Any));
        assert!(!is_change(&on(open, "/watched/a.md"), folder), "an open is no change");
        assert!(!is_change(&on(close, "/watched/a.md"), folder), "a close is no change");
        assert!(is_change(&on(write, "/watched/a.md"), folder), "a write is a change");
        let folder_created = notify::EventKind::Create(notify::event::CreateKind::Folder);
        assert!(!is_change(&on(folder_created, "/watched"), folder), "a report about the folder itself is no change");
        let own_index = format!("/watched/{INDEX_FILE}.tmp");
        assert!(!is_change(&on(write, &own_index), folder), "a write of the own index file is no change");
    }

    #[tokio::test]
    async fn a_write_of_another_file_is_a_tick() {
        let dir = tempfile::tempdir().unwrap();
        let (_watcher, mut ticks, _activity) = watch(dir.path()).unwrap();
        std::fs::write(dir.path().join("note.md"), "x").unwrap();
        assert!(ticked(&mut ticks).await, "a note made no tick");
    }
}
