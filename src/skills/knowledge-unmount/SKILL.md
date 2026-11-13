---
name: knowledge-unmount
description: Stop this session's memory bank (its share or mount) by running `agent-knowledge unmount`. The peer leaves the mesh, other peers see it leave, and the local files stay as they are. Errors if the session runs no memory bank. Use when the user wants to stop sharing, unmount, disconnect, or leave the mesh.
---

# knowledge-unmount

Stop this session's share or mount. It leaves the mesh cleanly, so the other
peers print `📚 <nick> left`. The folder and its files stay as they are; they
only stop syncing.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command |
|---|---|
| Claude Code | `/knowledge-unmount` |
| Codex | `$knowledge-unmount` |
| Pi | `/skill:knowledge-unmount` |

When a step names the other skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## Steps

1. **Check the binary.** Run `command -v agent-knowledge`. If it is not on
   `PATH`, stop and tell the user to install it:
   `cargo install --git https://github.com/caiogondim/agent-knowledge`.

2. **Unmount.**

   ```bash
   agent-knowledge unmount
   ```

   It waits up to 15 seconds for the memory to leave the mesh.
   - **Exit 0:** it prints `📚 left the <kind> at <folder>; files kept`.
   - **`no memory bank in this session`:** nothing to stop. Say so, and
     mention /knowledge-share and /knowledge-mount (agent form, see Agent
     differences).
   - **`did not stop within`:** show the error. It names the pid and how to
     force the stop.

3. **Clean up the background task (Claude Code only).** The share or mount
   ran as a background Bash task, with a Monitor relaying its `📚` lines. Both
   end on their own once the process exits. If either still shows as running,
   stop it.

4. **Report.** Print the `📚 left ...` line as it is, and say that the files
   are still in the folder, no longer synced. To sync again, run
   /knowledge-mount with the same hash, or /knowledge-share for a new one.

## Notes

- `kill <pid>` and stopping the background task also leave the mesh cleanly.
  Only `kill -9` skips the leave; the other peers then see the peer go quiet
  instead of leaving.
