---
name: knowledge-mount
description: Mount a shared folder by hash with live two-way sync by running `agent-knowledge mount <hash> <target>` in the background. Use when the user has a hash from /knowledge-share (or `agent-knowledge share`) and wants that folder on this machine.
argument-hint: <hash> <target>
---

# knowledge-mount

Start `agent-knowledge mount <hash> <target>` as a background process. It keeps
`<target>` in sync with the shared folder until it is stopped.

## Steps

1. **Parse the arguments.** `$ARGUMENTS` holds `<hash> <target>`. If the hash
   is missing, ask for it. If the target is missing, ask for it. Do not invent
   a hash. Resolve the target to an absolute path.

2. **Check the target.** The command creates the target if it does not exist.
   - Missing or empty: go ahead.
   - Exists and is not empty: stop and ask. Sync is two-way, so files already
     in the target go to every peer, and a delete on any peer deletes them
     here. Suggest a new, empty folder instead.
   - Exists and is a file, not a folder: stop with a clear message.

3. **Pick the command.** The repo root is the folder that holds this skill's
   `.claude/` directory.
   - If `agent-knowledge` is on `PATH`, use `agent-knowledge`.
   - Otherwise build first in the foreground so errors show up now:
     `cargo build --release --manifest-path <repo-root>/Cargo.toml`.
     Then use `<repo-root>/target/release/agent-knowledge`.

4. **Run it in the background.** Call Bash with `run_in_background: true`:

   ```bash
   <command> mount "<hash>" "<absolute-target>"
   ```

5. **Confirm the mount.** The process prints `mounted at <target>` on stderr
   once it joins. Read the background task's output until that line appears.
   If the process exits instead, show the error output and stop. A bad hash
   fails here.

6. **Report.** Tell the user:
   - where the folder is mounted, and how many files have arrived so far,
   - that edits on either side sync within a few seconds, deletes included,
   - that the mount keeps running in the background, and how to stop it (stop
     the background task, or ask Claude to stop the mount). Stopping leaves the
     files in place; it only stops syncing.

## Notes

- Each side keeps a full copy of the folder.
- A rename syncs as a delete plus a create.
- Set `RUST_LOG=info` before the command to debug a mount that never connects.
