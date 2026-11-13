---
name: knowledge-share
description: Share a folder over habilis-network with live two-way sync by running `agent-knowledge share <folder>` in the background, then report the hash a peer mounts. Use when the user wants to share a folder, an OKF bundle, or knowledge with another agent or machine.
argument-hint: <folder> [--nickname <nick>]
---

# knowledge-share

Start `agent-knowledge share <folder>` as a background process and give the
user the hash it prints. The process keeps syncing until it is stopped.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command | Arguments |
|---|---|---|
| Claude Code | `/knowledge-share` | `$ARGUMENTS` |
| Codex | `$knowledge-share` | the text after `$knowledge-share` in the user message |
| Pi | `/skill:knowledge-share` | the text after `/skill:knowledge-share`, appended to these instructions |

When a step names the other skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## Steps

1. **Resolve the folder.** The arguments (see Agent differences) hold
   `<folder> [--nickname <nick>]`. Take `--nickname <nick>` out first, if it is
   there; the rest is the folder. If the folder is empty, ask the user which
   folder to share. Resolve it to an absolute path and confirm it exists and is
   a directory. Stop with a clear message if not. Do not invent a nickname: with
   no flag, the mesh picks a random one.

2. **Warn before sharing.** Sharing is two-way: anyone with the hash can edit,
   create, and delete files in this folder. Say this in one line. If the folder
   is the user's home directory, a whole project root, or holds secrets
   (`.env`, keys), ask for confirmation first.

3. **Check the binary.** Run `command -v agent-knowledge`. If it is not on
   `PATH`, stop and tell the user to install it:
   `cargo install --git https://github.com/caiogondim/agent-knowledge`.

4. **Run it in the background.** It runs until stopped, so never wait on it
   in the foreground.
   Add `--nickname "<nick>"` to the command only if the user gave one.
   - Claude Code: call Bash with `run_in_background: true` and run
     `agent-knowledge share "<absolute-folder>" [--nickname "<nick>"]`.
   - Codex and Pi: their shell tool waits for the command to end, so detach
     it and log to a new file:
     `log=$(mktemp -t agent-knowledge); nohup agent-knowledge share "<absolute-folder>" [--nickname "<nick>"] > "$log" 2>&1 & echo "$! $log"`.
     Note the PID and the log path, and read the log in the next step.

5. **Read the hash and the nickname.** Once the mesh is up, the process
   prints the hash as the first line on stdout, then `📚 joined as <nick>`.
   Read the background task's output until both lines appear. If the process
   exits instead, show the error output and stop. A refused nickname fails
   here, with a message that says why.

   If it exits with `this session already runs a ... memory`, report that
   error: a session runs one memory at a time. Name the running memory from
   the message and say to stop it before starting another.

6. **Report.** Tell the user:
   - the hash, in a code block so it is easy to copy,
   - the `📚 joined as <nick>` line, as printed,
   - the command a peer runs, in the peer's agent form: `/knowledge-mount <hash> <target>`
     (Claude Code), `$knowledge-mount <hash> <target>` (Codex), or
     `/skill:knowledge-mount <hash> <target>` (Pi),
   - that the share keeps running in the background, and how to stop it:
     /knowledge-unmount (agent form), which leaves the mesh and keeps the files.

7. **Relay join and leave notices.** While the share runs, the process prints
   `📚 <nick> joined` and `📚 <nick> left` on stdout as peers come and go.
   Print each such line in the chat as it is, one line per notice, and
   nothing else from the output.
   - Claude Code: watch the background task's output with Monitor, filtered
     to lines that start with `📚`.
   - Codex and Pi: read new lines of the log file whenever you take a turn,
     and print the `📚` lines that are new since the last read.

## Notes

- Hidden files and folders (`.git`, `.DS_Store`) and empty folders do not sync.
- If two sides edit the same file at once, the last write wins.
- Set `RUST_LOG=info` before the command to debug a share that never prints a
  hash.
