use std::collections::BTreeMap;
use std::io;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

/// Every synced file under `root`, as relative path → sha256 hex.
/// Hidden entries (a name that starts with `.`) are skipped, so `.git` and editor swap files stay local.
pub fn scan(root: &Path) -> io::Result<BTreeMap<String, String>> {
    let mut files = BTreeMap::new();
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
                if let Ok(sha) = sha256_file(&path) {
                    files.insert(rel, sha);
                }
            }
        }
    }
    Ok(files)
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
