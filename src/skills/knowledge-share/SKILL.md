---
name: knowledge-share
description: Share a folder over habilis-network with live two-way sync by running `agent-knowledge share <folder>` in the background, then report the hash a peer mounts. Use when the user wants to share a folder, an OKF bundle, or knowledge with another agent or machine.
argument-hint: <folder>
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

1. **Resolve the folder.** Use the arguments (see Agent differences) as the
   folder. If it is empty, ask the user which folder to share. Resolve it to an
   absolute path and confirm it exists and is a directory. Stop with a clear
   message if not.

2. **Warn before sharing.** Sharing is two-way: anyone with the hash can edit,
   create, and delete files in this folder. Say this in one line. If the folder
   is the user's home directory, a whole project root, or holds secrets
   (`.env`, keys), ask for confirmation first.

3. **Check the binary.** Run `command -v agent-knowledge`. If it is not on
   `PATH`, stop and tell the user to install it:
   `cargo install --git https://github.com/caiogondim/agent-knowledge`.

4. **Run it in the background.** It runs until stopped, so never wait on it
   in the foreground.
   - Claude Code: call Bash with `run_in_background: true` and run
     `agent-knowledge share "<absolute-folder>"`.
   - Codex and Pi: their shell tool waits for the command to end, so detach
     it and log to a new file:
     `log=$(mktemp -t agent-knowledge); nohup agent-knowledge share "<absolute-folder>" > "$log" 2>&1 & echo "$! $log"`.
     Note the PID and the log path, and read the log in the next step.

5. **Read the hash.** The hash is the first line on stdout, printed once the
   mesh is up. Read the background task's output until that line appears. If
   the process exits instead, show the error output and stop.

   If it exits with `this session already runs a ... memory`, report that
   error: a session runs one memory at a time. Name the running memory from
   the message and say to stop it before starting another.

6. **Report.** Tell the user:
   - the hash, in a code block so it is easy to copy,
   - the command a peer runs, in the peer's agent form: `/knowledge-mount <hash> <target>`
     (Claude Code), `$knowledge-mount <hash> <target>` (Codex), or
     `/skill:knowledge-mount <hash> <target>` (Pi),
   - that the share keeps running in the background, and how to stop it (stop
     the background task, or `kill <pid>`).

## Notes

- Hidden files and folders (`.git`, `.DS_Store`) and empty folders do not sync.
- If two sides edit the same file at once, the last write wins.
- Set `RUST_LOG=info` before the command to debug a share that never prints a
  hash.
