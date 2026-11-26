//! Who may write to a share. It has no I/O.
//!
//! In `creator` mode the hash is `<mesh id>.<creator key>` and the creator signs each entry of its
//! index. A peer applies an entry only if the signature verifies against the key in the hash.

use anyhow::{Result, anyhow, bail};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde_json::{Value, json};
use zeroize::Zeroizing;

/// The `--writers` flag of `share`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Writers {
    #[default]
    Everybody,
    Creator,
}

/// What a peer needs to check an entry. `Creator` carries the bare mesh id and the creator key.
// One `Trust` exists for each share and callers pass it by reference, so the size gap costs nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trust {
    Everybody,
    Creator { mesh: String, key: VerifyingKey },
}

/// The fields of an entry that the signature covers. The ticket is not one of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Signed<'a> {
    File { path: &'a str, ts: u64, sha256: &'a str, size: u64 },
    Tombstone { path: &'a str, ts: u64 },
}

/// The signing key of the creator of a share. It lives in memory for 1 `share` run.
pub struct CreatorKey(SigningKey);

const ENTRY_DOMAIN: &[u8] = b"agent-knowledge/entry/v2\0";
const KIND_TOMBSTONE: u8 = 0;
const KIND_FILE: u8 = 1;
const KEY_HEX_LEN: usize = 64;
const SIGNATURE_HEX_LEN: usize = 128;

impl CreatorKey {
    pub fn generate() -> Result<Self> {
        let mut secret = Zeroizing::new([0u8; 32]);
        getrandom::fill(secret.as_mut()).map_err(|error| anyhow!("no random bytes for the creator key: {error}"))?;
        Ok(Self(SigningKey::from_bytes(&secret)))
    }

    pub fn public_hex(&self) -> String {
        hex::encode(self.0.verifying_key().to_bytes())
    }
}

/// Sign an index entry of the mesh `mesh`. The entry is `{sha256, ticket, ts, size}` for a file or
/// `{deleted: true, ts}` for a tombstone. The result is a copy of `entry` with `sig` added. An entry
/// without the signed fields, a file without `size` for example, is an error.
pub fn sign_entry(key: &CreatorKey, mesh: &str, path: &str, entry: &Value) -> Result<Value> {
    let signed =
        signed_of(path, entry).ok_or_else(|| anyhow!("the entry of {path} lacks a field that the signature covers"))?;
    let sig = hex::encode(key.0.sign(&message(mesh, &signed)).to_bytes());
    let mut signed_entry = entry.clone();
    signed_entry["sig"] = json!(sig);
    Ok(signed_entry)
}

/// `true` if `entry` at `path` carries a valid signature under `trust`. Everybody mode accepts all.
/// In creator mode a missing or mistyped `ts`, `sig`, or for a file `sha256` or `size`, is invalid.
pub fn verify_entry(trust: &Trust, path: &str, entry: &Value) -> bool {
    let Trust::Creator { mesh, key } = trust else {
        return true;
    };
    let (Some(signed), Some(signature)) =
        (signed_of(path, entry), entry.get("sig").and_then(Value::as_str).and_then(parse_signature))
    else {
        return false;
    };
    key.verify_strict(&message(mesh, &signed), &signature).is_ok()
}

/// The hash of a share: the bare mesh id, or `<mesh id>.<creator key>` for a creator share. The
/// mesh id is Base58Check, so `.` is outside its alphabet and an old build rejects the suffix.
pub fn encode_hash(mesh: &str, creator: Option<&CreatorKey>) -> String {
    match creator {
        Some(key) => format!("{mesh}.{}", key.public_hex()),
        None => mesh.to_owned(),
    }
}

/// Split a hash into the bare mesh id and the trust rule. A malformed suffix is an error.
pub fn parse_hash(hash: &str) -> Result<(String, Trust)> {
    let (mesh, suffix) = match hash.split_once('.') {
        Some((mesh, suffix)) => (mesh, Some(suffix)),
        None => (hash, None),
    };
    if mesh.is_empty() {
        bail!("the hash has no mesh id");
    }
    let Some(suffix) = suffix else {
        return Ok((mesh.to_owned(), Trust::Everybody));
    };
    let key = parse_key(suffix)
        .ok_or_else(|| anyhow!("the creator key in the hash is not 64 lowercase hex digits of an Ed25519 key"))?;
    Ok((mesh.to_owned(), Trust::Creator { mesh: mesh.to_owned(), key }))
}

/// The signed fields of an entry, or `None` if one is missing or has the wrong type.
fn signed_of<'a>(path: &'a str, entry: &'a Value) -> Option<Signed<'a>> {
    let ts = entry.get("ts")?.as_u64()?;
    if entry.get("deleted").and_then(Value::as_bool) == Some(true) {
        return Some(Signed::Tombstone { path, ts });
    }
    let sha256 = entry.get("sha256")?.as_str()?;
    let size = entry.get("size")?.as_u64()?;
    Some(Signed::File { path, ts, sha256, size })
}

fn parse_key(hex_text: &str) -> Option<VerifyingKey> {
    let bytes: [u8; 32] = decode_lower_hex(hex_text, KEY_HEX_LEN)?.try_into().ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

fn parse_signature(hex_text: &str) -> Option<Signature> {
    let bytes: [u8; 64] = decode_lower_hex(hex_text, SIGNATURE_HEX_LEN)?.try_into().ok()?;
    Some(Signature::from_bytes(&bytes))
}

fn decode_lower_hex(text: &str, len: usize) -> Option<Vec<u8>> {
    let is_lower_hex = text.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    if text.len() != len || !is_lower_hex {
        return None;
    }
    hex::decode(text).ok()
}

fn message(mesh: &str, signed: &Signed) -> Vec<u8> {
    let mut bytes = ENTRY_DOMAIN.to_vec();
    push_text(&mut bytes, mesh);
    match signed {
        Signed::File { path, ts, sha256, size } => {
            push_text(&mut bytes, path);
            bytes.extend(ts.to_be_bytes());
            bytes.push(KIND_FILE);
            push_text(&mut bytes, sha256);
            bytes.extend(size.to_be_bytes());
        }
        Signed::Tombstone { path, ts } => {
            push_text(&mut bytes, path);
            bytes.extend(ts.to_be_bytes());
            bytes.push(KIND_TOMBSTONE);
        }
    }
    bytes
}

/// `u32be(len) || text`. A field longer than `u32::MAX` bytes cannot occur in a bounded document.
fn push_text(bytes: &mut Vec<u8>, text: &str) {
    bytes.extend(u32::try_from(text.len()).unwrap_or(u32::MAX).to_be_bytes());
    bytes.extend(text.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESH: &str = "mesh-1";

    fn creator() -> (CreatorKey, Trust) {
        let key = CreatorKey::generate().unwrap();
        let (_, trust) = parse_hash(&encode_hash(MESH, Some(&key))).unwrap();
        (key, trust)
    }

    fn file(ts: u64, sha256: &str, size: u64, ticket: &str) -> Value {
        json!({ "sha256": sha256, "ticket": ticket, "ts": ts, "size": size })
    }

    fn tombstone(ts: u64) -> Value {
        json!({ "deleted": true, "ts": ts })
    }

    fn signed(key: &CreatorKey, path: &str, entry: &Value) -> Value {
        sign_entry(key, MESH, path, entry).unwrap()
    }

    #[test]
    fn the_signature_of_a_file_verifies() {
        let (key, trust) = creator();
        assert!(verify_entry(&trust, "a.md", &signed(&key, "a.md", &file(3, "ab12", 10, "t"))));
    }

    #[test]
    fn the_signature_of_a_tombstone_verifies() {
        let (key, trust) = creator();
        assert!(verify_entry(&trust, "a.md", &signed(&key, "a.md", &tombstone(4))));
    }

    #[test]
    fn signing_adds_the_signature_and_changes_nothing_else() {
        let (key, _) = creator();
        let entry = file(3, "ab12", 10, "t");
        let result = signed(&key, "a.md", &entry);
        let mut without_sig = result.clone();
        let sig = without_sig.as_object_mut().unwrap().remove("sig").unwrap();
        assert_eq!(without_sig, entry);
        assert_eq!(sig.as_str().map(str::len), Some(SIGNATURE_HEX_LEN));
        assert!(entry.get("sig").is_none(), "the input is not changed");
    }

    #[test]
    fn a_changed_field_does_not_verify() {
        let (key, trust) = creator();
        let good = signed(&key, "a.md", &file(3, "ab12", 10, "t"));
        for (field, value) in [("ts", json!(4)), ("sha256", json!("cd34")), ("size", json!(11))] {
            let mut changed = good.clone();
            changed[field] = value;
            assert!(!verify_entry(&trust, "a.md", &changed), "{field}");
        }
    }

    #[test]
    fn a_copied_entry_with_another_ticket_still_verifies() {
        let (key, trust) = creator();
        let mut copy = signed(&key, "a.md", &file(3, "ab12", 10, "creator-ticket"));
        copy["ticket"] = json!("mounter-ticket");
        assert!(verify_entry(&trust, "a.md", &copy));
    }

    #[test]
    fn an_entry_replayed_under_another_path_or_mesh_does_not_verify() {
        let (key, trust) = creator();
        let entry = file(3, "ab12", 10, "t");
        assert!(!verify_entry(&trust, "b.md", &signed(&key, "a.md", &entry)));
        assert!(!verify_entry(&trust, "a.md", &sign_entry(&key, "mesh-2", "a.md", &entry).unwrap()));
    }

    #[test]
    fn a_file_signature_is_not_a_tombstone_signature() {
        let (key, trust) = creator();
        let sig = signed(&key, "a.md", &file(3, "ab12", 10, "t"))["sig"].clone();
        let mut forged = tombstone(3);
        forged["sig"] = sig;
        assert!(!verify_entry(&trust, "a.md", &forged));
    }

    #[test]
    fn a_tombstone_forged_by_another_key_does_not_verify() {
        let (_, trust) = creator();
        let forger = CreatorKey::generate().unwrap();
        assert!(!verify_entry(&trust, "a.md", &signed(&forger, "a.md", &tombstone(9))));
    }

    #[test]
    fn an_entry_that_lacks_a_signed_field_does_not_verify() {
        let (key, trust) = creator();
        let good = signed(&key, "a.md", &file(3, "ab12", 10, "t"));
        for field in ["ts", "sig", "size", "sha256"] {
            let mut missing = good.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(!verify_entry(&trust, "a.md", &missing), "{field}");
        }
        let mut mistyped = good.clone();
        mistyped["ts"] = json!("3");
        assert!(!verify_entry(&trust, "a.md", &mistyped));
        assert!(!verify_entry(&trust, "a.md", &json!(null)));
    }

    #[test]
    fn a_malformed_signature_does_not_verify() {
        let (key, trust) = creator();
        let good = signed(&key, "a.md", &file(3, "ab12", 10, "t"));
        let sig = good["sig"].as_str().unwrap().to_owned();
        for bad in
            [String::new(), "zz".into(), sig[..126].into(), sig[2..].into(), format!("{sig}00"), sig.to_uppercase()]
        {
            let mut entry = good.clone();
            entry["sig"] = json!(bad);
            assert!(!verify_entry(&trust, "a.md", &entry), "{bad:?}");
        }
        let mut not_text = good;
        not_text["sig"] = json!(5);
        assert!(!verify_entry(&trust, "a.md", &not_text));
    }

    #[test]
    fn everybody_mode_accepts_any_entry() {
        assert!(verify_entry(&Trust::Everybody, "a.md", &file(1, "ab", 1, "t")));
        assert!(verify_entry(&Trust::Everybody, "a.md", &json!(null)));
    }

    #[test]
    fn an_entry_without_the_signed_fields_cannot_be_signed() {
        let key = CreatorKey::generate().unwrap();
        let mut no_size = file(3, "ab12", 10, "t");
        no_size.as_object_mut().unwrap().remove("size");
        for entry in [no_size, json!({ "sha256": "ab" }), json!({ "deleted": true }), json!(7)] {
            assert!(sign_entry(&key, MESH, "a.md", &entry).is_err(), "{entry}");
        }
    }

    #[test]
    fn a_creator_hash_round_trips() {
        let key = CreatorKey::generate().unwrap();
        let hash = encode_hash(MESH, Some(&key));
        assert_eq!(hash, format!("{MESH}.{}", key.public_hex()));
        let (mesh, trust) = parse_hash(&hash).unwrap();
        assert_eq!(mesh, MESH);
        assert!(verify_entry(&trust, "a.md", &signed(&key, "a.md", &tombstone(1))));
    }

    #[test]
    fn a_bare_mesh_id_is_everybody_mode() {
        assert_eq!(encode_hash(MESH, None), MESH);
        assert_eq!(parse_hash(MESH).unwrap(), (MESH.to_owned(), Trust::Everybody));
    }

    #[test]
    fn a_malformed_suffix_is_an_error() {
        let key = CreatorKey::generate().unwrap().public_hex();
        let off_curve = (0..=u8::MAX)
            .map(|first| {
                let mut bytes = [0u8; 32];
                bytes[0] = first;
                bytes[31] = 0x80 | 0x7f;
                bytes
            })
            .find(|bytes| VerifyingKey::from_bytes(bytes).is_err())
            .map(hex::encode)
            .expect("some 32 bytes are not a curve point");
        let bad = [
            format!("{MESH}."),
            format!("{MESH}.{}", &key[..62]),
            format!("{MESH}.{key}00"),
            format!("{MESH}.{}", key.to_uppercase()),
            format!("{MESH}.{}", "zz".repeat(32)),
            format!("{MESH}.{key}.{key}"),
            format!("{MESH}.{off_curve}"),
            format!(".{key}"),
            String::new(),
        ];
        for hash in bad {
            assert!(parse_hash(&hash).is_err(), "{hash:?}");
        }
    }
}
