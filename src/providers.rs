//! Who can serve a file's bytes.
//!
//! A file entry holds its author's blob ticket, and that ticket dies with the author's process.
//! So every peer that holds a copy also offers its own ticket for that content, in the state
//! document's `providers` map: `{"providers": {"<sha256>": {"<peer>": "<ticket>"}}}`. It is keyed
//! by content, not path, so one copy serves every path with the same bytes, and by peer, so
//! concurrent offers merge instead of overwriting each other. A peer that downloads tries every
//! ticket for the content at once.

use serde_json::{Value, json};

const KEY: &str = "providers";

/// The merge that lists `ticket` as `peer`'s copy of the content `sha`.
pub fn offer(sha: &str, peer: &str, ticket: &str) -> Value {
    json!({ KEY: { sha: { peer: ticket } } })
}

/// The merge that takes `peer` off the list for the content `sha`.
pub fn withdraw(sha: &str, peer: &str) -> Value {
    json!({ KEY: { sha: { peer: null } } })
}

/// Every ticket that can serve `sha`: the file entry's own ticket first, if it has one, then each
/// peer's offer. Duplicates are dropped.
pub fn tickets(document: &Value, entry: &Value, sha: &str) -> Vec<String> {
    let offers = document[KEY][sha].as_object().into_iter().flat_map(|peers| peers.values());
    let mut tickets: Vec<String> = Vec::new();
    for ticket in std::iter::once(&entry["ticket"]).chain(offers).filter_map(Value::as_str) {
        if !tickets.iter().any(|seen| seen == ticket) {
            tickets.push(ticket.to_owned());
        }
    }
    tickets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offer_and_withdraw_touch_only_one_peer_of_one_content() {
        assert_eq!(offer("s", "p", "t"), json!({ "providers": { "s": { "p": "t" } } }));
        assert_eq!(withdraw("s", "p"), json!({ "providers": { "s": { "p": null } } }));
    }

    #[test]
    fn tickets_list_the_author_first_then_every_offer_once() {
        let document = json!({ "providers": { "s": { "a": "author", "b": "keeper" }, "other": { "c": "x" } } });
        let entry = json!({ "sha256": "s", "ticket": "author" });

        assert_eq!(tickets(&document, &entry, "s"), ["author", "keeper"]);
    }

    #[test]
    fn tickets_work_without_an_entry_ticket_or_any_offer() {
        let entry = json!({ "sha256": "s" });

        assert!(tickets(&json!({}), &entry, "s").is_empty());
        assert_eq!(tickets(&json!({ "providers": { "s": { "b": "keeper" } } }), &entry, "s"), ["keeper"]);
    }
}
