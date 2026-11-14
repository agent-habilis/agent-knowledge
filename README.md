# agent-knowledge

Share a folder with live two-way sync over
[habilis-network](https://github.com/agent-habilis/habilis-network).

```bash
agent-knowledge share <folder> [--nickname <nick>]          # prints a hash
agent-knowledge mount <hash> <target> [--nickname <nick>]   # keeps <target> in sync with <folder>
agent-knowledge plug                                        # installs the skills into Claude Code, Codex, and Pi
agent-knowledge status                                      # prints this session's memory bank folder
```

### Nicknames and notices

Each peer has a nickname in the mesh. `--nickname <nick>` sets it. Without
the flag, the mesh picks a random one, such as `baton-garden`. A nickname is
1 to 32 characters, with no whitespace and none of `/ \ < > #`.

`share` and `mount` print one line on stdout for this peer, and one for each
other peer that joins or leaves. Each line starts with 📚:

```text
📚 joined as a
📚 b joined
📚 b left
```

`share` prints the hash first, on its own line, before these lines.

### Skills

| Skill | What it does |
|---|---|
| `/knowledge-share <folder> [--nickname <nick>]` | Runs `share` in the background, prints the hash, and relays the 📚 lines. |
| `/knowledge-mount <hash> <target> [--nickname <nick>]` | Runs `mount` in the background and relays the 📚 lines. The target is the memory bank. |
| `/knowledge-write <what>` | Writes an [OKF](https://github.com/GoogleCloudPlatform/open-knowledge-format) concept to the session's memory bank. Errors if there is none. |
| `/knowledge-read [question]` | Answers from the session's memory bank. Errors if there is none. |
| `/knowledge-status` | Shows how many peers are connected, the hash, the folder path, your nickname, each peer's path and last seen, and the docs (file count, size, last sync, failed transfers, last 5 changes). Errors if there is no memory bank. |

### One memory per session

Each agent session runs at most one memory: one `share` or one `mount`. A
second one in the same session fails with `this session already runs a ...
memory`. `status` prints the folder of the running one on its first line, and
`/knowledge-write` and `/knowledge-read` use it. After the folder, it prints a
📚 block with the mesh and the docs. The running process keeps that state in
`<registry>/<session>.status.json`, beside its registry entry, and removes it
when it stops. `/knowledge-status` prints the block.

The session is `$CLAUDE_CODE_SESSION_ID` in Claude Code, `$CODEX_THREAD_ID` in
Codex, or `$AGENT_KNOWLEDGE_SESSION`. Without any of them (Pi, a plain
terminal), everything shares the session `default`. Each session's memory is
recorded in `~/.agent-knowledge/sessions/<session>.json` (or under
`$AGENT_KNOWLEDGE_HOME`); an entry whose process died is ignored.

The table uses the Claude Code form. In Codex, a skill command starts with `$`
(`$knowledge-share`). In Pi, it starts with `/skill:` (`/skill:knowledge-share`).

`plug` writes the 4 skills into each agent that it finds on this machine, and
replaces older copies:

| Agent | Skills folder | Override | Skill command |
|---|---|---|---|
| Claude Code | `~/.claude/skills` | `$CLAUDE_CONFIG_DIR/skills` | `/knowledge-share` |
| Codex | `~/.codex/skills` | `$CODEX_HOME/skills` | `$knowledge-share` |
| Pi | `~/.pi/agent/skills` | `$PI_CODING_AGENT_DIR/skills` | `/skill:knowledge-share` |

`plug` finds an agent by its config folder (for example `~/.pi/agent`). To
pick agents, use `--agent claude-code`, `--agent codex`, or `--agent pi`, one
or more times. Restart the agent to load the skills.

## Install

The skills call `agent-knowledge` from `PATH`, so install the binary before
you plug the skills:

```bash
# Homebrew (macOS and Linux, arm64 and x86_64)
brew install agent-habilis/tap/agent-knowledge

# From a clone
cargo install --path .

# Without a clone
cargo install --git https://github.com/agent-habilis/agent-knowledge

# Then plug the skills and restart Claude Code, Codex, or Pi
agent-knowledge plug
```

`cargo install` puts the binary in `~/.cargo/bin`. If the shell cannot find
`agent-knowledge`, run `source ~/.cargo/env` or open a new terminal.

The skills are embedded in the binary. After `brew upgrade`, run
`agent-knowledge plug` again to update them.

## License

[Business Source License 1.1](LICENSE). You can use agent-knowledge in
production if your company's annual gross revenue is at most 2,000,000 USD.
Each release becomes MIT 4 years after its release date.

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

CI (`.github/workflows/ci.yml`) runs 4 jobs on each push and pull request to
`main`. To run the same checks locally:

```bash
cargo fmt --all -- --check                                               # fmt
cargo clippy --workspace --all-targets -- -D warnings                    # clippy
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items  # doc
cargo test --workspace --no-fail-fast                                    # test
```

`cargo fmt --all` fixes the format. `rustfmt.toml` sets the style.

- `src/scan.rs` has the unit tests.
- `tests/memory.rs` checks the session id, the registry, and the one-memory rule.
- `tests/plug.rs` checks the embedded skills, the skills folder and the detection of each agent, and the install.
- `tests/two_way.rs` runs a `share` loop and a `mount` loop in 1 process. It checks a create, an edit, and a delete in both directions. It takes about 10s.
- `tests/presence.rs` starts 2 peers with nicknames. The first peer must see the second join, then leave. It also checks the random nickname, a refused nickname, and the 📚 line format.
- `tests/relay_only.rs` blocks every direct path, so 2 peers sync only through the public relay. It needs internet access.

### Release

A merge to `main` releases if `.changeset/` holds a change file:

1. Add a change file with `knope document-change`, or by hand as
   `.changeset/<summary>.md` with `default: patch`, `minor`, or `major` in
   the front matter. Use the key `default`, not the package name.
2. Merge the PR. `auto-release.yml` runs knope. knope bumps the version,
   writes `CHANGELOG.md`, and runs `scripts/update-license.ts`. That script
   sets the version and the Change Date in `LICENSE`.
3. knope pushes the bump to `main` and tags `v<version>`.
4. `release.yml` builds 4 targets, uploads them, and bumps
   `Formula/agent-knowledge.rb` in
   [agent-habilis/homebrew-tap](https://github.com/agent-habilis/homebrew-tap).

While the version is `0.x`, knope shifts each level down 1: `major` gives
`0.2.0`, and `minor` or `patch` gives `0.1.1`.

### Code map

- `src/main.rs`: the CLI, with the `share`, `mount`, and `plug` commands.
- `src/memory.rs`: the one-memory-per-session registry behind `share`, `mount`, and `status`.
- `src/plug.rs`: embeds `src/skills/*/SKILL.md` in the binary and writes them to an agent's skills folder.
- `src/skills/`: the skills that `plug` installs. Edit them here, then rebuild.
- `src/sync.rs`: the sync loop. Both commands run it.
- `src/scan.rs`: walks a folder and hashes each file. It also rejects unsafe paths from a peer.
- `src/status.rs`: the status file that a running memory writes, and the 📚 block that `status` prints from it.
- `src/watch.rs`: the file watcher. It sends 1 tick after 300ms with no changes.

### How it works

- **The hash** is a habilis-network mesh id. The mesh uses all lookups: mDNS, DHT, and relay.
- **The file list** is the mesh `state` document, an automerge CRDT (a data type that merges edits from many writers). Each entry holds the sha256 of a file and a blob ticket.
- **The file bytes** go through the blob ticket, because a gossip frame holds at most 3840 bytes.
- **Echo stop:** each side keeps the last synced sha256 of each path. A change that matches it is the side's own echo, so the side skips it.
- **Conflicts:** if 2 sides edit 1 file at the same time, the last write wins.

### Limits

- Hidden files and folders (`.git`, `.DS_Store`) do not sync.
- Symlinks, unreadable files, and paths that are not valid UTF-8 do not sync.
- `.gitignore` is not read: every other file in the folder syncs.
- Empty folders do not sync.
- A rename syncs as a delete plus a create.
- Each side keeps a full copy of the folder. Stopping a share or mount does not delete the copies that peers already have.
