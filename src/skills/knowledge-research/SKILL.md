---
name: knowledge-research
description: Research a topic with the you.com Research API and save the report in this session's memory bank (its share or mount) as an Open Knowledge Format (OKF v0.2) concept under research/<topic>/, so every peer gets it and prints a notice. Needs AGENT_KNOWLEDGE_YOUCOM_KEY. Errors if the session runs no memory bank. Use when the user asks to research a topic, look something up on the web for the team, or add a research report to the shared memory.
argument-hint: <topic> [--effort lite|standard|deep|exhaustive]
---

# knowledge-research

Research a topic on the web with the you.com Research API and save the report
in this session's memory bank. The bank syncs it to every peer, and every
peer's share or mount prints `📚 a new research about <topic> is available`.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command | Arguments |
|---|---|---|
| Claude Code | `/knowledge-research` | `$ARGUMENTS` |
| Codex | `$knowledge-research` | the text after `$knowledge-research` in the user message |
| Pi | `/skill:knowledge-research` | the text after `/skill:knowledge-research`, appended to these instructions |

When a step names another skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## 1. Get the topic

The topic is the arguments (see Agent differences), without any `--effort`
flag. If there is no topic, ask for one. The effort is `lite` unless the user
asks for more: `standard`, `deep`, or `exhaustive` take longer.

## 2. Run the research

```bash
agent-knowledge research "<topic>" --effort <effort>
```

The command checks, in this order, and stops at the first failure:

1. `AGENT_KNOWLEDGE_YOUCOM_KEY` is set. If not, it fails with
   `AGENT_KNOWLEDGE_YOUCOM_KEY is not set`. Show that error and tell the user
   to export their you.com API key in it, then try again. Never ask the user
   to paste the key into the chat.
2. A `share` or `mount` of this session is running. If none runs, it fails
   with `no agent-knowledge share or mount is running in this session`, and
   writes nothing. Show:
   `No memory bank in this session. Run /knowledge-mount <hash> <target> or /knowledge-share <folder> first.`
   Use the agent form of the commands (see Agent differences).
3. The you.com Research API answers. On an error, show it as printed.

If `agent-knowledge` is not found, stop and tell the user to install it:
`cargo install --git https://github.com/caiogondim/agent-knowledge`.

## 3. Report

On exit 0, stdout is the path of the report. Tell the user:

- the path, and that it lives in `research/<topic>/` with the indexes and
  `log.md` updated,
- that peers receive it within a few seconds and see
  `📚 a new research about <topic> is available`,
- a two- or three-sentence summary taken from the report's `# Summary`.

Folder and file names use only lowercase letters, digits, and `-`: the topic
`Rust Async!` becomes `research/rust-async/rust-async.md`. Researching the
same topic again replaces that report.
