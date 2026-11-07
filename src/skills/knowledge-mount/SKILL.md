---
name: knowledge-mount
description: Mount a shared folder by hash with live two-way sync by running `agent-knowledge mount <hash> <target>` in the background. Use when the user has a hash from the knowledge-share skill (or `agent-knowledge share`) and wants that folder on this machine.
argument-hint: <hash> <target>
---

# knowledge-mount

Start `agent-knowledge mount <hash> <target>` as a background process. It keeps
`<target>` in sync with the shared folder until it is stopped.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command | Arguments |
|---|---|---|
| Claude Code | `/knowledge-mount` | `$ARGUMENTS` |
| Codex | `$knowledge-mount` | the text after `$knowledge-mount` in the user message |
| Pi | `/skill:knowledge-mount` | the text after `/skill:knowledge-mount`, appended to these instructions |

When a step names the other skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## Steps

1. **Parse the arguments.** The arguments (see Agent differences) hold `<hash>
   <target>`. If the hash is missing, ask for it. If the target is missing, ask
   for it. Do not invent a hash. Resolve the target to an absolute path.

2. **Check the target.** The command creates the target if it does not exist.
   - Missing or empty: go ahead.
   - Exists and is not empty: stop and ask. Sync is two-way, so files already
     in the target go to every peer, and a delete on any peer deletes them
     here. Suggest a new, empty folder instead.
   - Exists and is a file, not a folder: stop with a clear message.

3. **Check the binary.** Run `command -v agent-knowledge`. If it is not on
   `PATH`, stop and tell the user to install it:
   `cargo install --git https://github.com/caiogondim/agent-knowledge`.

4. **Run it in the background.** It runs until stopped, so never wait on it
   in the foreground.
   - Claude Code: call Bash with `run_in_background: true` and run
     `agent-knowledge mount "<hash>" "<absolute-target>"`.
   - Codex and Pi: their shell tool waits for the command to end, so detach
     it and log to a new file:
     `log=$(mktemp -t agent-knowledge); nohup agent-knowledge mount "<hash>" "<absolute-target>" > "$log" 2>&1 & echo "$! $log"`.
     Note the PID and the log path, and read the log in the next step.

5. **Confirm the mount.** The process prints `mounted at <target>` on stderr
   once it joins. Read the background task's output until that line appears.
   If the process exits instead, show the error output and stop. A bad hash
   fails here.

   If it exits with `this session already runs a ... memory`, report that
   error: a session runs one memory at a time. Name the running memory from
   the message and say to stop it before starting another.

6. **Report.** Tell the user:
   - where the folder is mounted, and how many files have arrived so far,
   - that edits on either side sync within a few seconds, deletes included,
   - that the mount keeps running in the background, and how to stop it (stop
     the background task, or `kill <pid>`). Stopping leaves the
     files in place; it only stops syncing.

## Notes

- Each side keeps a full copy of the folder.
- A rename syncs as a delete plus a create.
- Set `RUST_LOG=info` before the command to debug a mount that never connects.
