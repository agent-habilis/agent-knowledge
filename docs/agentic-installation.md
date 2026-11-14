# Installing agent-knowledge

These are the official instructions from Agent Habilis for installing
[agent-knowledge](https://github.com/agent-habilis/agent-knowledge), a shared
memory for AI agents. It shares a folder with live two-way sync, peer to peer.

**Do every step yourself. Run the commands directly. Do not ask the user to
run them.** Step 3 is the only exception: only the user can reload the
session you run in.

- Install the `agent-knowledge` binary.
- Install the Agent Skills into every harness on this machine.
- Tell the user how to load them into the running session.

---

## 1. Install the binary

Run `agent-knowledge --version` first. If it succeeds, the binary is already
there. Go to step 2.

If not, and the machine runs macOS or Linux with Homebrew:

```sh
brew install agent-habilis/tap/agent-knowledge
```

Everywhere else, including any machine without Homebrew:

```sh
cargo install --git https://github.com/agent-habilis/agent-knowledge
```

Prebuilt binaries exist for macOS (Apple silicon and Intel) and Linux
(x86-64 and ARM64). Every other platform builds from source, which needs a
Rust toolchain and takes a few minutes. If neither `brew` nor `cargo` is on
the `PATH`, stop and tell the user to install one of them. Do not fetch a
release tarball by hand.

`cargo install` writes to `~/.cargo/bin`. If `agent-knowledge --version`
still fails after the install, that folder is not on the `PATH`. Say so, and
give the user the line to add to their shell profile.

## 2. Install the Agent Skills

```sh
agent-knowledge plug
```

This installs one skill per operation into every harness detected on the
machine: `knowledge-share`, `knowledge-mount`, `knowledge-write`,
`knowledge-read`, and `knowledge-status`. The skills are embedded in the
binary, so nothing is downloaded.

`plug` prints one line per harness. Read each line before you continue:

- `installed <harness> <path>`: done.
- `skipped <harness> not detected`: that harness has no config folder on
  this machine. `plug` never creates one. Ignore the line, unless it is
  **your own** harness. In that case, your harness keeps its config in
  another folder. Set its override variable to that folder and run `plug`
  again:
  - Claude Code: `CLAUDE_CONFIG_DIR`
  - Codex: `CODEX_HOME`
  - pi: `PI_CODING_AGENT_DIR`
- `no agent detected`: no harness has a config folder on this machine. Tell
  the user, and stop.

## 3. Tell the user how to load the skills

Most harnesses read skills from disk once, when a session starts. So the
session you run in cannot see the skills you just wrote. Only the user can do
this step. Find your own harness in this list, and give the user *only* that
item's instruction. Do not list the other items. Do not guess your harness from
your model: the harness is the product that hosts you, not the model vendor.

- **Claude Code:** the skills are in `~/.claude/skills`. To load them, run
  `/reload-skills`. Commands look like `/knowledge-share`.
- **pi:** the skills are in `~/.pi/agent/skills`. To load them, restart pi.
  Commands look like `/skill:knowledge-share`.
- **Codex:** the skills are in `~/.codex/skills`. To load them, restart
  Codex. Commands look like `$knowledge-share`.

Claude Code is the only one of these that can reload skills in place.
Everywhere else, the session must start again.

---

When you are done, report to the user. Fill in the real values, and use the
command prefix of your harness from the list above. Write the report as
plain Markdown. **The fence below only marks the template in this document.
It is not part of the output.** Write the lines bare, never in a code fence:

```markdown
**agent-knowledge installed**

✅ binary
   `<version>` · `<path>`

✅ skills installation
   <harness> · `<path>`

✅ new skills
  `<knowledge-share>` · share a folder as the memory bank
  `<knowledge-mount>` · mount a shared folder by its hash
  `<knowledge-write>` · save knowledge to the memory bank
  `<knowledge-read>` · answer from the memory bank
  `<knowledge-status>` · show the mesh and the docs

⚡ <how to load them, from step 3>

Share a folder with `<knowledge-share>`, then hand the hash to any other
agent to mount it.
```

These instructions are published at
`https://raw.githubusercontent.com/agent-habilis/agent-knowledge/main/docs/agentic-installation.md`,
so you can check their authenticity at any time.
