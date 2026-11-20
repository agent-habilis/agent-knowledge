use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

/// A file hashed before: its size, modification time and identity then, and when it was hashed.
struct Seen {
    len: u64,
    modified: SystemTime,
    /// The inode and the change time, where the system has them. A file that replaces another by a rename can keep its
    /// size and its modification time, but not these. `git` keys its index on them for the same reason.
    identity: Option<(u64, i64, i64)>,
    sha: String,
    hashed_at: SystemTime,
}

/// A file modified this soon before it was hashed can change again within the clock tick of its modification time,
/// with the same size, and look unchanged. It is hashed again, as `git` does for a racy file.
const RACY: Duration = Duration::from_secs(2);

/// Scans a folder again and again, and hashes only the files that changed since the last scan.
#[derive(Default)]
pub struct Scanner {
    /// The folder that `seen` belongs to: its paths are relative to it, so another folder cannot reuse them.
    root: Option<PathBuf>,
    seen: HashMap<String, Seen>,
    hashed: usize,
}

impl Scanner {
    /// How many files the last [`Self::scan`] hashed.
    pub fn hashed(&self) -> usize {
        self.hashed
    }

    /// Every synced file under `root`, as relative path → sha256 hex.
    /// Hidden entries (a name that starts with `.`) are skipped, so `.git` and editor swap files stay local.
    /// A scan of another folder than the last one starts from nothing.
    /// A file whose size and modification time are those of the last scan keeps its hash, unless it was modified
    /// within 2 s of that scan, which a coarse clock could not tell from the edit that follows.
    pub fn scan(&mut self, root: &Path) -> io::Result<BTreeMap<String, String>> {
        self.scan_with(root, &mut SystemTime::now, &mut sha256_file)
    }

    /// [`Self::scan`] with the clock and the hash of a file given, so a test can see in what order they run.
    fn scan_with(
        &mut self,
        root: &Path,
        now: &mut dyn FnMut() -> SystemTime,
        hash: &mut dyn FnMut(&Path) -> io::Result<String>,
    ) -> io::Result<BTreeMap<String, String>> {
        if self.root.as_deref() != Some(root) {
            self.seen.clear();
            self.root = Some(root.to_path_buf());
        }
        let mut files = BTreeMap::new();
        let mut seen = HashMap::new();
        self.hashed = 0;
        let walked = self.walk(root, now, hash, &mut files, &mut seen);
        if let Err(error) = walked {
            // The entries moved to `seen` go back, so the files that did not change are not hashed again.
            self.seen.extend(seen);
            return Err(error);
        }
        self.seen = seen;
        Ok(files)
    }

    /// Visit every file under `root`: each one is added to `files`, and its cache entry moves from `self.seen` to `seen`.
    fn walk(
        &mut self,
        root: &Path,
        now: &mut dyn FnMut() -> SystemTime,
        hash: &mut dyn FnMut(&Path) -> io::Result<String>,
        files: &mut BTreeMap<String, String>,
        seen: &mut HashMap<String, Seen>,
    ) -> io::Result<()> {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let file_type = entry.file_type()?;
                let path = entry.path();
                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file() {
                    let Some(rel) = relative(root, &path) else { continue };
                    // A file can vanish between the listing and the read.
                    let Ok(meta) = entry.metadata() else { continue };
                    let Ok(modified) = meta.modified() else { continue };
                    let identity = identity_of(&meta);
                    let known = self.seen.remove(&rel).filter(|known| {
                        known.len == meta.len()
                            && known.modified == modified
                            && known.identity == identity
                            && known.modified.checked_add(RACY).is_some_and(|racy_end| known.hashed_at > racy_end)
                    });
                    let entry = match known {
                        Some(known) => known,
                        None => {
                            // Read the clock first: an edit during the read is then not older than `hashed_at`, so the
                            // file is hashed again at the next scan.
                            let hashed_at = now();
                            let Ok(sha) = hash(&path) else { continue };
                            self.hashed += 1;
                            Seen { len: meta.len(), modified, identity, sha, hashed_at }
                        }
                    };
                    files.insert(rel.clone(), entry.sha.clone());
                    seen.insert(rel, entry);
                }
            }
        }
        Ok(())
    }
}

/// Every synced file under `root`, as relative path → sha256 hex, all hashed. See [`Scanner`] to hash less.
pub fn scan(root: &Path) -> io::Result<BTreeMap<String, String>> {
    Scanner::default().scan(root)
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(unix)]
fn identity_of(meta: &std::fs::Metadata) -> Option<(u64, i64, i64)> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.ino(), meta.ctime(), meta.ctime_nsec()))
}

#[cfg(not(unix))]
fn identity_of(_: &std::fs::Metadata) -> Option<(u64, i64, i64)> {
    None
}

fn sha256_file(path: &Path) -> io::Result<String> {
    Ok(sha256_bytes(&std::fs::read(path)?))
}

fn relative(root: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(root).ok()?;
    let parts: Vec<_> = rel.components().map(|c| c.as_os_str().to_str()).collect::<Option<_>>()?;
    Some(parts.join("/"))
}

/// The local path for a relative path that came from a peer, or `None` if it could escape `root`.
pub fn resolve(root: &Path, rel: &str) -> Option<PathBuf> {
    let rel_path = Path::new(rel);
    let safe = !rel.is_empty()
        && rel_path
            .components()
            .all(|c| matches!(c, Component::Normal(name) if !name.to_string_lossy().starts_with('.')));
    safe.then(|| root.join(rel_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_rejects_escapes_and_hidden() {
        let root = Path::new("/r");
        assert_eq!(resolve(root, "a/b.md"), Some(PathBuf::from("/r/a/b.md")));
        assert_eq!(resolve(root, "../x"), None);
        assert_eq!(resolve(root, "/etc/passwd"), None);
        assert_eq!(resolve(root, ".git/config"), None);
        assert_eq!(resolve(root, ""), None);
    }

    /// Backdate a file, so it is not racy: set its modification time `age` ago.
    fn age(path: &Path, age: Duration) {
        let file = std::fs::File::options().write(true).open(path).unwrap();
        file.set_modified(SystemTime::now() - age).unwrap();
    }

    #[test]
    fn a_second_scan_hashes_no_file_that_did_not_change() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.md", "b.md"] {
            std::fs::write(dir.path().join(name), name).unwrap();
            age(&dir.path().join(name), Duration::from_secs(60));
        }
        let mut scanner = Scanner::default();
        let first = scanner.scan(dir.path()).unwrap();
        assert_eq!(scanner.hashed(), 2);
        let second = scanner.scan(dir.path()).unwrap();
        assert_eq!(scanner.hashed(), 0);
        assert_eq!(first, second);
    }

    #[test]
    fn a_second_scan_hashes_only_the_file_that_changed() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.md", "b.md"] {
            std::fs::write(dir.path().join(name), name).unwrap();
            age(&dir.path().join(name), Duration::from_secs(60));
        }
        let mut scanner = Scanner::default();
        scanner.scan(dir.path()).unwrap();
        std::fs::write(dir.path().join("a.md"), "edited, and longer").unwrap();
        let files = scanner.scan(dir.path()).unwrap();
        assert_eq!(scanner.hashed(), 1);
        assert_eq!(files["a.md"], sha256_bytes(b"edited, and longer"));
        assert_eq!(files["b.md"], sha256_bytes(b"b.md"));
    }

    #[test]
    fn an_edit_of_the_same_size_and_the_same_clock_tick_after_a_scan_is_not_missed() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("v.md");
        let tick = SystemTime::now();
        // Two writes in one tick of a coarse clock: same size, same modification time.
        let write = |content: &str| {
            std::fs::write(&file, content).unwrap();
            std::fs::File::options().write(true).open(&file).unwrap().set_modified(tick).unwrap();
        };
        write("v1");
        let mut scanner = Scanner::default();
        scanner.scan(dir.path()).unwrap();
        write("v2");
        let files = scanner.scan(dir.path()).unwrap();
        assert_eq!(files["v.md"], sha256_bytes(b"v2"));
    }

    #[cfg(unix)]
    #[test]
    fn a_replacement_with_the_same_size_and_the_same_mtime_is_not_missed() {
        let dir = tempfile::tempdir().unwrap();
        let (file, other) = (dir.path().join("a.md"), dir.path().join("b.md"));
        let tick = SystemTime::now() - Duration::from_secs(60);
        for (path, content) in [(&file, "AAAA"), (&other, "BBBB")] {
            std::fs::write(path, content).unwrap();
            std::fs::File::options().write(true).open(path).unwrap().set_modified(tick).unwrap();
        }
        let mut scanner = Scanner::default();
        scanner.scan(dir.path()).unwrap();
        // `mv`, `cp -p` and `rsync -t` give the file that replaces it the old size and the old mtime.
        std::fs::rename(&other, &file).unwrap();
        let files = scanner.scan(dir.path()).unwrap();
        assert_eq!(files["a.md"], sha256_bytes(b"BBBB"));
    }

    #[cfg(unix)]
    #[test]
    fn a_scan_that_fails_keeps_the_cache() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("locked")).unwrap();
        for name in ["a.md", "b.md", "c.md"] {
            std::fs::write(dir.path().join(name), name).unwrap();
            age(&dir.path().join(name), Duration::from_secs(60));
        }
        let mut scanner = Scanner::default();
        scanner.scan(dir.path()).unwrap();
        let locked = dir.path().join("locked");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let failed = scanner.scan(dir.path());
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        if failed.is_ok() {
            return; // a user that can read any directory, such as root, cannot make the scan fail
        }
        scanner.scan(dir.path()).unwrap();
        assert_eq!(scanner.hashed(), 0, "the failed scan dropped the hashes of the files that did not change");
    }

    #[test]
    fn the_cache_is_not_reused_for_another_root() {
        let (one, two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let tick = SystemTime::now() - Duration::from_secs(60);
        for (dir, content) in [(&one, "AAAA"), (&two, "BBBB")] {
            std::fs::write(dir.path().join("same.md"), content).unwrap();
            std::fs::File::options().write(true).open(dir.path().join("same.md")).unwrap().set_modified(tick).unwrap();
        }
        let mut scanner = Scanner::default();
        scanner.scan(one.path()).unwrap();
        let files = scanner.scan(two.path()).unwrap();
        assert_eq!(files["same.md"], sha256_bytes(b"BBBB"));
        assert_eq!(scanner.hashed(), 1);
    }

    #[test]
    fn the_clock_is_read_before_the_file_is_hashed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.md"), "a").unwrap();
        let log = std::cell::RefCell::new(Vec::new());
        let mut scanner = Scanner::default();
        scanner
            .scan_with(
                dir.path(),
                &mut || {
                    log.borrow_mut().push("clock");
                    SystemTime::now()
                },
                &mut |path| {
                    log.borrow_mut().push("read");
                    sha256_file(path)
                },
            )
            .unwrap();
        assert_eq!(*log.borrow(), ["clock", "read"], "an edit during the read must count as after the clock");
    }

    #[test]
    fn a_file_that_is_gone_or_new_is_followed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("old.md"), "old").unwrap();
        age(&dir.path().join("old.md"), Duration::from_secs(60));
        let mut scanner = Scanner::default();
        scanner.scan(dir.path()).unwrap();
        std::fs::remove_file(dir.path().join("old.md")).unwrap();
        std::fs::write(dir.path().join("new.md"), "new").unwrap();
        let files = scanner.scan(dir.path()).unwrap();
        assert_eq!(files.keys().collect::<Vec<_>>(), ["new.md"]);
        assert_eq!(scanner.hashed(), 1);
    }

    #[test]
    fn scan_skips_hidden_and_nests() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::create_dir_all(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join("sub/a.md"), "a").unwrap();
        std::fs::write(dir.path().join(".git/x"), "x").unwrap();
        std::fs::write(dir.path().join(".swp"), "x").unwrap();
        let files = scan(dir.path()).unwrap();
        assert_eq!(files.keys().collect::<Vec<_>>(), ["sub/a.md"]);
        assert_eq!(files["sub/a.md"], sha256_bytes(b"a"));
    }
}
