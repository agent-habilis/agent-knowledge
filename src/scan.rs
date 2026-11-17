use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

/// A file hashed before: its size and modification time then, and when it was hashed.
struct Seen {
    len: u64,
    modified: SystemTime,
    sha: String,
    hashed_at: SystemTime,
}

/// A file modified this soon before it was hashed can change again within the clock tick of its modification time,
/// with the same size, and look unchanged. It is hashed again, as `git` does for a racy file.
const RACY: Duration = Duration::from_secs(2);

/// Scans a folder again and again, and hashes only the files that changed since the last scan.
#[derive(Default)]
pub struct Scanner {
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
    /// A file whose size and modification time are those of the last scan keeps its hash, unless it was modified
    /// within 2 s of that scan, which a coarse clock could not tell from the edit that follows.
    pub fn scan(&mut self, root: &Path) -> io::Result<BTreeMap<String, String>> {
        let mut files = BTreeMap::new();
        let mut seen = HashMap::new();
        self.hashed = 0;
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
                    let known = self.seen.remove(&rel).filter(|known| {
                        known.len == meta.len() && known.modified == modified && known.hashed_at > known.modified + RACY
                    });
                    let entry = match known {
                        Some(known) => known,
                        None => {
                            let Ok(sha) = sha256_file(&path) else { continue };
                            self.hashed += 1;
                            Seen { len: meta.len(), modified, sha, hashed_at: SystemTime::now() }
                        }
                    };
                    files.insert(rel.clone(), entry.sha.clone());
                    seen.insert(rel, entry);
                }
            }
        }
        self.seen = seen;
        Ok(files)
    }
}

/// Every synced file under `root`, as relative path → sha256 hex, all hashed. See [`Scanner`] to hash less.
pub fn scan(root: &Path) -> io::Result<BTreeMap<String, String>> {
    Scanner::default().scan(root)
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
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
