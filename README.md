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

After `brew upgrade`, run `agent-knowledge plug` again to update the skills.

If you installed with `cargo install` before, remove that copy with
`cargo uninstall agent-knowledge`. `~/.cargo/bin` usually comes first in
`PATH`, so the old copy hides the Homebrew one.

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

`/knowledge-share <folder> [--nickname <nick>]` runs `share` in the
background and reports the hash. The folder becomes the memory bank of the
session. Hand the hash to another agent to mount it.

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

## Limits

- Hidden files and folders (`.git`, `.DS_Store`) do not sync.
- Symlinks, unreadable files, and paths that are not valid UTF-8 do not sync.
- `.gitignore` is not read: every other file in the folder syncs.
- Empty folders do not sync.
- A rename syncs as a delete plus a create.
- If 2 sides edit 1 file at the same time, the later write wins.
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
