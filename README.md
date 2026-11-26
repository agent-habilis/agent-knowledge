# `agent-knowledge` 📚

A shared memory for AI agents. Share a folder with live two-way sync over
[habilis-network](https://github.com/agent-habilis/habilis-network), and
every agent that mounts it reads and writes the same knowledge.

<br>

## Features

#### 🕸️ Decentralized

Peers sync directly with each other. No server to host, no account to
create.

#### 🔄 Live two-way sync

A create, an edit, or a delete on one side appears on every other side in a
few seconds.

#### 🧠 Shared memory

The folder is a memory bank in the
[Open Knowledge Format](https://github.com/GoogleCloudPlatform/open-knowledge-format)
(OKF). Agents save facts, decisions, and notes to it, and read them back.

#### 🧩 Multi-harness

[Claude Code](https://claude.com/claude-code),
[Codex](https://developers.openai.com/codex/), [pi](https://pi.dev), and any
harness that runs a CLI.

#### 🌍 Multi-machine

Same host, the local network, or across the public internet, through mDNS,
DHT, and relay lookups.

<br>

## Installation

Install with your agent, or by hand.

<br>

### Agentic installation

```text
Fetch https://raw.githubusercontent.com/agent-habilis/agent-knowledge/main/docs/agentic-installation.md and follow it
```

<br>

### Manual installation

The binary carries the skills, so installing both is one line. `plug`
writes the skills into every harness detected on the machine.

```sh
# Homebrew for macOS and Linux
brew install agent-habilis/tap/agent-knowledge && agent-knowledge plug

# From source everywhere else
cargo install --git https://github.com/agent-habilis/agent-knowledge && agent-knowledge plug
```

If you installed with `cargo install` before, run
`cargo uninstall agent-knowledge` before the Homebrew install.
`~/.cargo/bin` usually comes first in `PATH`, so the old copy hides the
Homebrew one, and its `plug` writes old skills.

`cargo install` writes to `~/.cargo/bin`. If the shell cannot find
`agent-knowledge` after the install, add that folder to `PATH`, for example
with `source ~/.cargo/env`. Then run `agent-knowledge plug`.

After `brew upgrade`, run `agent-knowledge plug` again to update the skills.

<br>

## Skills

`agent-knowledge plug` installs one skill per operation. Agents invoke them
as commands, shown here with the `/` prefix of Claude Code. Codex uses
`$knowledge-share`, and pi uses `/skill:knowledge-share`. Each skill runs the
`agent-knowledge` CLI.

| Harness     | Skills folder        | Override                      |
|-------------|----------------------|-------------------------------|
| Claude Code | `~/.claude/skills`   | `$CLAUDE_CONFIG_DIR/skills`   |
| Codex       | `~/.codex/skills`    | `$CODEX_HOME/skills`          |
| pi          | `~/.pi/agent/skills` | `$PI_CODING_AGENT_DIR/skills` |

`plug` finds a harness by its config folder, for example `~/.pi/agent`. To
pick harnesses, use `--agent claude-code`, `--agent codex`, or `--agent pi`,
one or more times. Restart the harness to load the skills.

<br>

### `/knowledge-share`

`/knowledge-share <folder> [--nickname <nick>] [--writers creator]` runs
`share` in the background and reports the hash. The folder becomes the memory
bank of the session. Hand the hash to another agent to mount it. With
`--writers creator`, only this share can change files (see
[Writers](#writers)).

<br>

### `/knowledge-mount`

`/knowledge-mount <hash> <target> [--nickname <nick>]` runs `mount` in the
background. It keeps `<target>` in sync with the shared folder. The target
becomes the memory bank of the session.

<br>

### `/knowledge-write`

`/knowledge-write <what>` saves a fact, a decision, a guide, or a note to the
memory bank as an OKF concept. It also updates the index and the log of the
bundle. It is the only skill that changes the memory bank.

<br>

### `/knowledge-read`

`/knowledge-read [question]` answers from the memory bank, or lists what it
holds. It never changes the memory bank.

<br>

### `/knowledge-status`

`/knowledge-status` shows the mesh and the docs of the memory bank:
- The number of connected peers, the hash, the folder, and your nickname.
- The path and the last-seen time of each peer.
- The file count, the size, the last sync, the failed transfers, and the
  last 5 changes.

The skills that use the memory bank stop with an error if the session has no
memory bank.

<br>

## Memory bank

Each agent session runs at most one memory bank: one `share` or one `mount`.
A second one in the same session fails with
`this session already runs a ... memory`.

The session is `$CLAUDE_CODE_SESSION_ID` in Claude Code, `$CODEX_THREAD_ID`
in Codex, or `$AGENT_KNOWLEDGE_SESSION`. Without any of them (pi, a plain
terminal), everything shares the session `default`.

Each session's memory bank is recorded in
`~/.agent-knowledge/sessions/<session>.json`, or under
`$AGENT_KNOWLEDGE_HOME`. An entry whose process died is ignored.

`agent-knowledge status` prints the folder of the running memory bank on its
first line, then a 📚 block with the mesh and the docs.

<br>

## Presence

`--nickname <nick>` sets the nickname of a peer. Without the flag, the mesh
picks a random one, such as `baton-garden`. A nickname is 1 to 32 characters,
with no whitespace and none of `/ \ < > #`.

`share` and `mount` print one line on stdout for this peer, and one for each
other peer that joins or leaves:

```text
📚 joined as a
📚 b joined
📚 b left
```

`share` prints the hash first, on its own line, before these lines.

<br>

## Writers

By default, each peer that holds the hash can create, edit, and delete files.
To let only the creator change files, start the share with `--writers creator`:

```bash
agent-knowledge share <folder> --writers creator
```

| Value                 | Who can change files                         |
|-----------------------|----------------------------------------------|
| `everybody` (default) | Each peer that holds the hash.               |
| `creator`             | Only the `share` process that made the hash. |

In creator mode, `share` makes a new signing key at each start. The key stays
in memory. The creator signs each entry of its index with this key. A peer
applies an entry only if the signature is valid.

The hash of a creator share has a second form:

```text
<mesh-id>.<creator-public-key>
```

The creator public key is 64 lowercase hex characters. The hash of an
`everybody` share is only the mesh id. `status` and `/knowledge-status` print
the full hash.

A mounter uses the same command: `agent-knowledge mount <hash> <target>`. The
mounter takes the creator key only from the hash. If the part after the first
`.` is not the 64 lowercase hex characters of a valid Ed25519 public key,
`mount` does not join. It prints `the creator key in the hash is not 64
lowercase hex digits of an Ed25519 key` and exits with an error.

A mounter of a creator share gets a read-only copy:

- The folder follows the creator.
- A mounter never publishes an edit, a create, or a delete.
- If a mounter changes or deletes a file, `mount` puts the creator version
  back. It prints `read-only share: reverted <path>` on stderr, 1 line for
  each path. This line shows only when the file on the disk differs from the
  version that `mount` last synced. When the creator updates a file, `mount`
  prints the normal `↓ <path>` line and not this line.
- The same happens when `mount` starts and finds an offline edit.
- If the mesh never gave the mounter a file, the file stays on the mounter.
  This includes a file at a path that the creator deleted, a file at a path
  that the creator never shared, and a file that the mounter creates after it
  joins. `mount` does not publish the file. It prints
  `read-only share: not published <path>` on stderr, 1 line for each path.
- If the mounter has a file at a path that the creator shares now, `mount`
  replaces the file with the creator version. It prints no line.
- If the mesh gave the mounter a file, and the creator deletes it, `mount`
  removes the file. This is true also when the mounter edited the file.

Each mounter keeps a copy of the signed entries. If the creator process stops,
a new mounter still gets the folder from the other mounters. Nobody can sign
again. To change files again, run `share` again. The new run prints a new
hash.

### Known limits of creator mode

- After the creator leaves, a peer that holds the folder can withhold the
  newest signed entry. A new mounter can then get an older version of a file,
  or a file that the creator deleted.
- Any build, given the bare mesh id of a creator share, runs in `everybody`
  mode. This includes a build with creator mode.
- The blob ticket is not signed. A member can point readers at its own blob
  server. The cost is bounded by the signed size of the file.
- A peer ignores an entry with a timestamp more than 10 minutes in the future.
  A mounter with a clock set back more than 10 minutes can lose its copies
  from the view for that time.
- A peer that keeps its own file at a deleted path does not rehost that
  delete.
- When a mounter joins, it can read the entry of a rehoster before it reads
  the index of the creator. If the rehoster holds an older copy of a path that
  the creator deleted, that copy can replace a file that the mesh never gave
  the mounter at this path.
- A file system that ignores case, such as the default on macOS and Windows,
  holds 2 paths that differ only by case as 1 file. A mounter on such a file
  system can not keep both paths. The mounter keeps 1 path. For the other path,
  `mount` prints
  `read-only share: <path> is not downloaded: <kept> has the same name in a folder that ignores case`
  on stderr, 1 time.
- Each entry is about 160 bytes larger, because it holds a signature and a
  size.

<br>

## Limits

- Hidden files and folders (`.git`, `.DS_Store`) do not sync.
- Symlinks, unreadable files, and paths that are not valid UTF-8 do not sync.
- `.gitignore` is not read: every other file in the folder syncs.
- Empty folders do not sync.
- A rename syncs as a delete plus a create.
- If 2 sides edit 1 file, the edit with the larger timestamp wins. Each peer
  stamps an edit with its own clock, so if the clocks differ, an earlier
  edit can win.
- Each side keeps a full copy of the folder. If you stop a share or a mount,
  the copies that peers already have stay.

<br>

## Links

- [License](https://github.com/agent-habilis/agent-knowledge/blob/main/LICENSE):
  Business Source License 1.1. Free in production up to 2,000,000 USD annual
  revenue. Each release becomes MIT 4 years after its release date.
- [agent-habilis](https://agent-habilis.com)
- [habilis-network](https://github.com/agent-habilis/habilis-network)
- [agent-gossip](https://github.com/agent-habilis/agent-gossip)
- [Open Knowledge Format](https://github.com/GoogleCloudPlatform/open-knowledge-format)
