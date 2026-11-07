---
name: knowledge-share
description: Share a folder over habilis-network with live two-way sync by running `agent-knowledge share <folder>` in the background, then report the hash a peer mounts. Use when the user wants to share a folder, an OKF bundle, or knowledge with another agent or machine.
argument-hint: <folder>
---

# knowledge-share

Start `agent-knowledge share <folder>` as a background process and give the
user the hash it prints. The process keeps syncing until it is stopped.

## Steps

1. **Resolve the folder.** Use `$ARGUMENTS` as the folder. If it is empty, ask
   the user which folder to share. Resolve it to an absolute path and confirm it
   exists and is a directory. Stop with a clear message if not.

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
   - Other agents: run
     `nohup agent-knowledge share "<absolute-folder>" > /tmp/agent-knowledge-share.log 2>&1 &`,
     note the PID from `$!`, and read the log file in the next step.

5. **Read the hash.** The hash is the first line on stdout, printed once the
   mesh is up. Read the background task's output until that line appears. If
   the process exits instead, show the error output and stop.

6. **Report.** Tell the user:
   - the hash, in a code block so it is easy to copy,
   - the command a peer runs: `/knowledge-mount <hash> <target>`,
   - that the share keeps running in the background, and how to stop it (stop
     the background task, or `kill <pid>`).

## Notes

- Hidden files and folders (`.git`, `.DS_Store`) and empty folders do not sync.
- If two sides edit the same file at once, the last write wins.
- Set `RUST_LOG=info` before the command to debug a share that never prints a
  hash.
