//! The file list of one peer, and the rule that picks one version of a path out of every peer's list.
//!
//! A peer publishes its list as a blob (the manifest) and puts one pointer to it in its own slice
//! of the shared state, because one slice holds about 5 file entries and a manifest holds a folder.

use std::collections::BTreeMap;

use serde_json::{Value, json};

/// An entry more than this far ahead of the local clock is ignored, and it can never overflow the
/// next timestamp.
pub const MAX_AHEAD_MS: u64 = 24 * 60 * 60 * 1000;

/// One version of one path. `ts_ms` orders versions of the same path across peers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// The file has this content, and `ticket` redeems it.
    Live { sha256: String, ticket: String, ts_ms: u64 },
    /// The file was deleted. `sha256` is the content that was deleted, so a peer that restarts with
    /// that content on disk can tell the delete is about its copy.
    Gone { sha256: String, ts_ms: u64 },
}

/// Relative path → entry. Sorted, so equal lists encode to equal bytes.
pub type Manifest = BTreeMap<String, Entry>;

/// The version of a path that wins, and every ticket that redeems it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub winner: Entry,
    /// Each live entry with the winner's `ts_ms` and `sha256`: any of them serves the same bytes.
    pub tickets: Vec<String>,
}

impl Entry {
    pub fn ts_ms(&self) -> u64 {
        match self {
            Self::Live { ts_ms, .. } | Self::Gone { ts_ms, .. } => *ts_ms,
        }
    }

    pub fn sha256(&self) -> &str {
        match self {
            Self::Live { sha256, .. } | Self::Gone { sha256, .. } => sha256,
        }
    }

    pub fn is_gone(&self) -> bool {
        matches!(self, Self::Gone { .. })
    }

    /// Whether `other` is the same version: same kind, content and timestamp. The ticket can differ.
    pub fn same_version(&self, other: &Self) -> bool {
        self.is_gone() == other.is_gone() && self.sha256() == other.sha256() && self.ts_ms() == other.ts_ms()
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Live { sha256, ticket, ts_ms } => json!({ "sha256": sha256, "ticket": ticket, "ts_ms": ts_ms }),
            Self::Gone { sha256, ts_ms } => json!({ "deleted": true, "sha256": sha256, "ts_ms": ts_ms }),
        }
    }

    fn from_json(value: &Value) -> Option<Self> {
        let sha256 = value.get("sha256")?.as_str()?.to_owned();
        let ts_ms = value.get("ts_ms")?.as_u64()?;
        if value.get("deleted") == Some(&Value::Bool(true)) {
            return Some(Self::Gone { sha256, ts_ms });
        }
        let ticket = value.get("ticket")?.as_str()?.to_owned();
        Some(Self::Live { sha256, ticket, ts_ms })
    }
}

pub fn encode(manifest: &Manifest) -> Vec<u8> {
    let files: serde_json::Map<String, Value> = manifest.iter().map(|(path, entry)| (path.clone(), entry.to_json())).collect();
    serde_json::to_vec(&json!({ "files": files })).expect("a JSON value always serializes")
}

/// The manifest in `bytes`, without the entries that lack a `sha256`, a `ts_ms`, or both a ticket and a delete mark.
/// `None` if `bytes` is not a manifest at all.
pub fn decode(bytes: &[u8]) -> Option<Manifest> {
    let document: Value = serde_json::from_slice(bytes).ok()?;
    let files = document.get("files")?.as_object()?;
    Some(files.iter().filter_map(|(path, value)| Some((path.clone(), Entry::from_json(value)?))).collect())
}

/// The timestamp for a new local entry of a path whose highest seen timestamp is `seen`.
/// It is after every version seen, even when the local clock is behind.
pub fn next_ts(now_ms: u64, seen: Option<u64>) -> u64 {
    now_ms.max(seen.map_or(0, |seen| seen.saturating_add(1)))
}

/// Whether `entry` is close enough to the local clock to count.
pub fn within_bound(entry: &Entry, now_ms: u64) -> bool {
    entry.ts_ms() <= now_ms.saturating_add(MAX_AHEAD_MS)
}

/// The winner of each path over the manifests of every author, by the highest `ts_ms`.
/// A tie goes to the lower `sha256`, then to a live entry, then to the lower author key.
pub fn resolve(manifests: &[(&str, &Manifest)], now_ms: u64) -> BTreeMap<String, Resolved> {
    let mut best: BTreeMap<&str, (&str, &Entry)> = BTreeMap::new();
    for (author, manifest) in manifests {
        for (path, entry) in manifest.iter().filter(|(_, entry)| within_bound(entry, now_ms)) {
            let beats = |(held_author, held): &(&str, &Entry)| {
                let key = |author: &str, entry: &Entry| (std::cmp::Reverse(entry.ts_ms()), entry.sha256().to_owned(), entry.is_gone(), author.to_owned());
                key(author, entry) < key(held_author, held)
            };
            if best.get(path.as_str()).is_none_or(beats) {
                best.insert(path, (author, entry));
            }
        }
    }
    best.into_iter()
        .map(|(path, (_, winner))| {
            let mut tickets: Vec<String> = Vec::new();
            for (_, manifest) in manifests {
                if let Some(Entry::Live { ticket, .. }) = manifest.get(path).filter(|entry| entry.same_version(winner)) {
                    tickets.push(ticket.clone());
                }
            }
            (path.to_owned(), Resolved { winner: winner.clone(), tickets })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(sha: &str, ts_ms: u64) -> Entry {
        Entry::Live { sha256: sha.to_owned(), ticket: format!("ticket-{sha}-{ts_ms}"), ts_ms }
    }

    fn gone(sha: &str, ts_ms: u64) -> Entry {
        Entry::Gone { sha256: sha.to_owned(), ts_ms }
    }

    fn manifest(entries: &[(&str, Entry)]) -> Manifest {
        entries.iter().map(|(path, entry)| ((*path).to_owned(), entry.clone())).collect()
    }

    const NOW: u64 = 1_000_000;

    #[test]
    fn encode_then_decode_returns_the_manifest() {
        let original = manifest(&[("a.md", live("aa", 5)), ("b/c.md", gone("bb", 7))]);
        assert_eq!(decode(&encode(&original)), Some(original));
    }

    #[test]
    fn equal_manifests_encode_to_equal_bytes() {
        let one = manifest(&[("b.md", live("bb", 2)), ("a.md", live("aa", 1))]);
        let two = manifest(&[("a.md", live("aa", 1)), ("b.md", live("bb", 2))]);
        assert_eq!(encode(&one), encode(&two));
    }

    #[test]
    fn decode_drops_an_entry_without_a_timestamp_or_a_ticket() {
        let bytes = br#"{"files":{
            "ok.md":{"sha256":"aa","ticket":"t","ts_ms":1},
            "no-ts.md":{"sha256":"aa","ticket":"t"},
            "no-ticket.md":{"sha256":"aa","ts_ms":1},
            "no-sha.md":{"deleted":true,"ts_ms":1},
            "old.md":{"sha256":"aa","ticket":"t"}}}"#;
        let decoded = decode(bytes).unwrap();
        assert_eq!(decoded.keys().collect::<Vec<_>>(), ["ok.md"]);
    }

    #[test]
    fn decode_refuses_what_is_not_a_manifest() {
        assert_eq!(decode(b"not json"), None);
        assert_eq!(decode(br#"{"files":[]}"#), None);
        assert_eq!(decode(br#"{"other":{}}"#), None);
    }

    #[test]
    fn the_highest_timestamp_wins() {
        let (one, two) = (manifest(&[("p", live("aa", 100))]), manifest(&[("p", live("bb", 200))]));
        let resolved = resolve(&[("k1", &one), ("k2", &two)], NOW);
        assert_eq!(resolved["p"].winner, live("bb", 200));
    }

    #[test]
    fn a_tie_goes_to_the_lower_sha_whoever_holds_it() {
        let (one, two) = (manifest(&[("p", live("bb", 100))]), manifest(&[("p", live("aa", 100))]));
        assert_eq!(resolve(&[("k1", &one), ("k2", &two)], NOW)["p"].winner, live("aa", 100));
        assert_eq!(resolve(&[("k2", &two), ("k1", &one)], NOW)["p"].winner, live("aa", 100));
    }

    #[test]
    fn a_tombstone_newer_than_a_live_entry_wins_and_an_older_one_loses() {
        let (live_new, live_old) = (manifest(&[("p", live("aa", 200))]), manifest(&[("p", live("aa", 100))]));
        let tombstone = manifest(&[("p", gone("aa", 150))]);
        assert_eq!(resolve(&[("k1", &live_new), ("k2", &tombstone)], NOW)["p"].winner, live("aa", 200));
        assert_eq!(resolve(&[("k1", &live_old), ("k2", &tombstone)], NOW)["p"].winner, gone("aa", 150));
    }

    #[test]
    fn every_ticket_of_the_winning_version_is_offered_and_other_versions_are_not() {
        let one = manifest(&[("p", Entry::Live { sha256: "aa".into(), ticket: "mine".into(), ts_ms: 9 })]);
        let two = manifest(&[("p", Entry::Live { sha256: "aa".into(), ticket: "theirs".into(), ts_ms: 9 })]);
        let stale = manifest(&[("p", live("aa", 3))]);
        let resolved = resolve(&[("k1", &one), ("k2", &two), ("k3", &stale)], NOW);
        assert_eq!(resolved["p"].tickets, ["mine", "theirs"]);
    }

    #[test]
    fn an_entry_far_ahead_of_the_clock_is_ignored() {
        let ahead = manifest(&[("p", live("bb", NOW + MAX_AHEAD_MS + 1)), ("q", live("cc", u64::MAX))]);
        let honest = manifest(&[("p", live("aa", 10))]);
        let resolved = resolve(&[("k1", &ahead), ("k2", &honest)], NOW);
        assert_eq!(resolved["p"].winner, live("aa", 10));
        assert!(!resolved.contains_key("q"));
    }

    #[test]
    fn the_next_timestamp_is_after_everything_seen_and_never_overflows() {
        assert_eq!(next_ts(1000, None), 1000);
        assert_eq!(next_ts(1000, Some(500)), 1000);
        assert_eq!(next_ts(1000, Some(5000)), 5001);
        assert_eq!(next_ts(1000, Some(u64::MAX)), u64::MAX);
    }
}
