# AGENTS.md — Instructions for AI Agents

agent-knowledge shares a folder with live two-way sync over habilis-network.
This file is guidance for work **on** the project. User-facing usage lives in
`README.md`.

## Development

### Requirements

- Rust 1.95.0. `rust-toolchain.toml` pins this version, so `rustup` installs
  it on the first build.
- Cargo fetches `agent-habilis/habilis-network` as a git dependency.
- Bun, for the scripts in `scripts/`.

### Build

```bash
cargo build
```

### Run

**Caution:** the sync goes both ways. An edit or a delete in `<path>` also
changes `<folder>`.

1. In terminal 1, share a folder. The command prints the hash on stdout:

   ```bash
   cargo run -- share <folder>
   ```

2. In terminal 2, mount the hash. If `<path>` does not exist, the command
   creates it:

   ```bash
   cargo run -- mount <hash> <path>
   ```

3. Edit, create, or delete a file on one side. The change appears on the
   other side in a few seconds.

4. To stop a side, press Ctrl-C.

### Testing

CI (`.github/workflows/ci.yml`) runs 5 jobs on each push and pull request to
`main`. To run the same checks locally:

```bash
cargo fmt --all -- --check                                               # fmt
cargo clippy --workspace --all-targets -- -D warnings                    # clippy
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items  # doc
cargo test --workspace --no-fail-fast                                    # test
bun test scripts/                                                        # scripts
```

`cargo fmt --all` fixes the format. `rustfmt.toml` sets the style.

- `src/scan.rs` has the unit tests.
- `tests/memory.rs` checks the session id, the registry, and the one-memory
  rule.
- `tests/plug.rs` checks the embedded skills, the skills folder, the
  detection of each agent, and the install.
- `tests/two_way.rs` runs a `share` loop and a `mount` loop in 1 process. It
  checks a create on each side, an edit on the share side, and a delete on
  the mount side. It takes about 10s.
- `tests/presence.rs` starts 2 peers with nicknames. The first peer must see
  the second join, then leave. It also checks the random nickname, a refused
  nickname, and the 📚 line format.
- `tests/relay_only.rs` blocks every direct path, so 2 peers sync only
  through the public relay. It needs internet access.
- `scripts/update-license.test.ts` checks the `LICENSE` header rewrite.

### Releasing

A merge to `main` releases if `.changeset/` holds a change file:

1. Add a change file with `knope document-change`, or by hand as
   `.changeset/<summary>.md` with `default: patch`, `minor`, or `major` in
   the front matter. Use the key `default`, not the package name.
2. Merge the PR. `auto-release.yml` runs knope. knope bumps the version,
   writes `CHANGELOG.md`, and runs `scripts/update-license.ts`. That script
   sets the version and the Change Date in `LICENSE`.
3. knope pushes the bump to `main` through the `agent-habilis-release` App,
   and tags `v<version>`.
4. `release.yml` builds 4 targets, uploads them, and bumps
   `Formula/agent-knowledge.rb` in
   [agent-habilis/homebrew-tap](https://github.com/agent-habilis/homebrew-tap).

While the version is `0.x`, knope shifts each level down 1. `major` bumps
the minor number, and `minor` or `patch` bumps the patch number.

To recover a failed release, first check if the tag `v<version>` exists:

- If the bump reached `main` but the tag does not exist, run
  `gh workflow run auto-release`. With no change file and no tag, it runs the
  knope `publish` workflow, which creates the missing tag and release.
- If the tag exists, `auto-release` has nothing to do. Rerun the failed jobs
  of the failed run with `gh run rerun <run-id> --failed`.

### Code map

- `src/main.rs`: the CLI, with the `share`, `mount`, `plug`, and `status`
  commands.
- `src/memory.rs`: the one-memory-per-session registry behind `share`,
  `mount`, and `status`.
- `src/plug.rs`: embeds `src/skills/*/SKILL.md` in the binary and writes them
  to an agent's skills folder.
- `src/skills/`: the skills that `plug` installs. Edit them here, then
  rebuild.
- `src/sync.rs`: the sync loop. Both commands run it.
- `src/scan.rs`: walks a folder and hashes each file. It also rejects unsafe
  paths from a peer.
- `src/status.rs`: the status file that a running memory writes, and the 📚
  block that `status` prints from it.
- `src/watch.rs`: the file watcher. It sends 1 tick after 300ms with no
  changes.
- `scripts/update-license.ts`: the `LICENSE` header rewrite for each release.

### How it works

- **The hash** is a habilis-network mesh id. The mesh uses all lookups: mDNS,
  DHT, and relay.
- **The file list** is an index blob per author. An index maps each path to
  the sha256, a blob ticket, and a timestamp of the file, or to a delete.
- **The mesh `state` document** is an automerge CRDT (a data type that merges
  edits from many writers). It holds one slice per author, and each slice
  points to that author's index blob.
- **The file bytes** go through the blob ticket of each entry, because a
  gossip frame holds at most 3840 bytes.
- **Echo stop:** each side keeps the last synced sha256 of each path. A
  change that matches it is the side's own echo, so the side skips it.
- **Conflicts:** for each path, the entry with the larger timestamp wins. A
  winning delete removes the local copy. The timestamp comes from the wall
  clock of each peer.
- **The status file:** the running process keeps its state in
  `<registry>/<session>.status.json`, beside its registry entry, and removes
  it when it stops.
