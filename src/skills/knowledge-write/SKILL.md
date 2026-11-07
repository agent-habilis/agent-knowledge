---
name: knowledge-write
description: Write knowledge into the currently mounted memory bank as an Open Knowledge Format (OKF v0.2) concept, then update the bundle index and log. Errors if no memory bank is mounted. Use when the user wants to save, remember, record, or share a fact, decision, guide, or note with the other agents on the mount.
argument-hint: <what to write>
---

# knowledge-write

Save knowledge as an OKF concept file in the mounted memory bank. The mount
syncs the file to every peer within a few seconds.

## Agent differences

This skill runs in Claude Code, Codex, and Pi. Find which agent you are, then
use its form below. The agent is the product that hosts you, not the model
vendor.

| Agent | Skill command | Arguments |
|---|---|---|
| Claude Code | `/knowledge-write` | `$ARGUMENTS` |
| Codex | `$knowledge-write` | the text after `$knowledge-write` in the user message |
| Pi | `/skill:knowledge-write` | the text after `/skill:knowledge-write`, appended to these instructions |

When a step names the other skill, use the same form: `/knowledge-mount` in
Claude Code, `$knowledge-mount` in Codex, `/skill:knowledge-mount` in Pi.

## 1. Find the mounted memory bank

A memory bank is mounted while an `agent-knowledge mount <hash> <target>`
process runs. List them:

```bash
ps -axo pid=,args= | grep '[a]gent-knowledge mount '
```

- **No match:** stop with this error and write nothing:
  `No mounted memory bank. Run /knowledge-mount <hash> <target> first.`
  Use the agent form of the command (see Agent differences).
- **One match:** the target is the text after the hash. If it is a relative
  path, resolve it against the process's working directory
  (`lsof -a -p <pid> -d cwd -Fn` on macOS, `readlink /proc/<pid>/cwd` on Linux).
- **More than one match:** list the targets and ask which one to write to.

Confirm the target is a directory. Call it `<bank>` below.

## 2. Decide what to write

Use the arguments (see Agent differences) and the conversation. If there is nothing to write, ask.
Never write secrets, tokens, passwords, or private keys: every peer receives
the file.

Read `<bank>/index.md` and the matching subdirectory `index.md`, if present.
If a concept on the same subject exists, **update it** rather than creating a
duplicate.

## 3. Write the concept

Path: `<bank>/<group>/<concept-id>.md`.

- `<group>`: a lowercase plural folder for the kind of knowledge, such as
  `decisions/`, `guides/`, `concepts/`, `notes/`. Reuse an existing folder when
  one fits.
- `<concept-id>`: short kebab-case, for example `retry-policy`. Never
  `index.md` or `log.md` (reserved), and never a name that starts with `.`
  (hidden files do not sync).

Content:

```markdown
---
type: Decision
title: Retry policy for peer requests
description: "One sentence that says what this concept holds."
tags: [networking, reliability]
generated: { by: <agent>/<model>, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: pr-42
    resource: https://github.com/org/repo/pull/42
    title: PR 42
---

# Summary

The claim, in a few sentences.[^pr-42]

# Details

Structure over prose: headings, lists, tables, code blocks. Link related
concepts with bundle-absolute links, like [peer](/concepts/peer.md).

[^pr-42]: PR 42
```

Rules:

- `type` is required. Pick a short descriptive value: `Decision`, `Guide`,
  `Glossary Term`, `Note`, `Playbook`, `Reference`.
- **Always double-quote `description`.** An unquoted `: ` inside it breaks the
  YAML.
- `generated.by` is the writer in OKF actor form: `claude-code/<model-id>` or
  `codex/<model-id>`; `human:<name>` only when the user dictated the content.
  `generated.at` is the current UTC time from `date -u +%Y-%m-%dT%H:%M:%SZ`.
- `status: draft` unless a person has reviewed it.
- Add a `sources` entry for each source the content came from, and cite it
  with a footnote whose label is the source `id`. Omit `sources` for knowledge
  with no source, such as a decision made in this conversation.
- When updating a concept, keep its existing keys and `verified` entries,
  change the content, and set `generated.at` to now.

## 4. Update the index and log

Re-read each file just before writing it: a peer may have changed it.

- `<bank>/<group>/index.md`: add or update the line
  `* [<title>](<concept-id>.md) - <description>`. No frontmatter.
- `<bank>/index.md`: create it if missing, with frontmatter
  `okf_version: "0.2"` and nothing else. Make sure it lists the group:
  `* [<Group>](<group>/) - <what it holds>`.
- `<bank>/log.md`: under a `## YYYY-MM-DD` heading for today (newest first,
  create the file with `# Directory Update Log` if missing), add
  `* **Creation**: Added [<title>](/<group>/<concept-id>.md).` or
  `* **Update**: ...`.

## 5. Check and report

Check that the frontmatter parses as YAML and has a non-empty `type`. Then
tell the user the path written, whether it was created or updated, and that
peers receive it within a few seconds.
