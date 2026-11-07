# agent-knowledge

Share a folder with live two-way sync over
[habilis-network](https://github.com/agent-habilis/habilis-network).

```bash
agent-knowledge share <folder>          # prints a hash
agent-knowledge mount <hash> <target>   # keeps <target> in sync with <folder>
agent-knowledge plug                    # installs the skills into Claude Code and Codex
```

`plug` writes the `knowledge-share` and `knowledge-mount` skills to
`~/.claude/skills` and `~/.codex/skills` (or `$CLAUDE_CONFIG_DIR/skills` and
`$CODEX_HOME/skills`), replacing older copies. Use `--agent claude` or
`--agent codex` to pick one. Restart the agent to load them.

## Development

### Requirements

- Rust 1.95.0. `rust-toolchain.toml` pins this version, so `rustup` installs it on the first build.
- Read access to the `agent-habilis/habilis-network` GitHub repo. Cargo fetches it as a git dependency.

### Build

```bash
cargo build
```

### Run

**Caution:** the sync goes both ways. An edit or a delete in `<path>` also changes `<folder>`.

1. In terminal 1, share a folder. The command prints the hash on stdout:

   ```bash
   cargo run -- share <folder>
   ```

2. In terminal 2, mount the hash. If `<path>` does not exist, the command creates it:

   ```bash
   cargo run -- mount <hash> <path>
   ```

3. Edit, create, or delete a file on one side. The change appears on the other side in a few seconds.

4. To stop a side, press Ctrl-C.

### Test

```bash
cargo test
cargo clippy
```

- `src/scan.rs` has the unit tests.
- `tests/plug.rs` checks the embedded skills, the skills folder of each agent, and the install.
- `tests/two_way.rs` runs a `share` loop and a `mount` loop in 1 process. It checks a create, an edit, and a delete in both directions. It takes about 10s.

### Code map

- `src/main.rs`: the CLI, with the `share`, `mount`, and `plug` commands.
- `src/plug.rs`: embeds `src/skills/*/SKILL.md` in the binary and writes them to an agent's skills folder.
- `src/skills/`: the skills that `plug` installs. Edit them here, then rebuild.
- `src/sync.rs`: the sync loop. Both commands run it.
- `src/scan.rs`: walks a folder and hashes each file. It also rejects unsafe paths from a peer.
- `src/watch.rs`: the file watcher. It sends 1 tick after 300ms with no changes.

### How it works

- **The hash** is a habilis-network mesh id. The mesh uses all lookups: mDNS, DHT, and relay.
- **The file list** is the mesh `state` document, an automerge CRDT (a data type that merges edits from many writers). Each entry holds the sha256 of a file and a blob ticket.
- **The file bytes** go through the blob ticket, because a gossip frame holds at most 3840 bytes.
- **Echo stop:** each side keeps the last synced sha256 of each path. A change that matches it is the side's own echo, so the side skips it.
- **Conflicts:** if 2 sides edit 1 file at the same time, the last write wins.

### Limits

- Hidden files and folders (`.git`, `.DS_Store`) do not sync.
- Empty folders do not sync.
- A rename syncs as a delete plus a create.
- Each side keeps a full copy of the folder.
