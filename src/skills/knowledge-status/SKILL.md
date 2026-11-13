---
name: knowledge-status
description: Show this session's memory bank (its share or mount) at a glance - who is in the mesh, how each peer is reached and when it was last seen, and what the folder holds and last synced. Read-only. Errors if the session runs no memory bank. Use when the user asks who is connected, whether sync is working, or what changed recently.
---

# knowledge-status

Print the state of this session's memory: the mesh and its peers, then the
docs folder and its recent changes.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command |
|---|---|
| Claude Code | `/knowledge-status` |
| Codex | `$knowledge-status` |
| Pi | `/skill:knowledge-status` |

When a step names the other skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## Steps

1. **Run it.**

   ```bash
   agent-knowledge status
   ```

   - **Exit 0:** the first line is the memory bank folder. The lines after it
     are the status block.
   - **Non-zero exit:** stop and show this error:
     `No memory bank in this session. Run /knowledge-mount <hash> <target> or /knowledge-share <folder> first.`
     Use the agent form of the commands (see Agent differences).
   - **`agent-knowledge` not found:** stop and tell the user to install it:
     `cargo install --git https://github.com/caiogondim/agent-knowledge`.

2. **Print the block.** Print everything after the first line exactly as it
   is, as Markdown: the `📚` lines and the tables. Add nothing in between.
   - If there is nothing after the first line, the memory just started and has
     not written its status yet. Say so, print the folder, and suggest running
     the skill again in a few seconds.

3. **Point out problems, in one line each, only if there are any:**
   - `0 connected`: no one else is in the mesh. Share the hash with a peer.
   - A peer marked `quiet`: it has not been heard from for a while, and may
     have left.
   - A `failed` count above 0: some transfers failed. Set `RUST_LOG=info`
     before restarting the share or mount to see why.

## Notes

- This skill never changes the memory bank or the running process.
- Peer ages and the `synced` time are counted from when the running process
  last wrote its status, which it does every few seconds.
