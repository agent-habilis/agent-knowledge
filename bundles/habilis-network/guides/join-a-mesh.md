---
type: Guide
title: Join a mesh from Rust
description: "The three calls that take a node from user input to a running event loop."
tags: [howto, rust, getting-started]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: arch
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/docs/architecture.md
    title: habilis-network architecture, section 6.1
    author: human:caiogondim
---

# Steps

1. `Params::resolve()` turns user input into a resolved parameter set.
2. `setup_mesh(kind, params)` builds everything and returns an opaque `EventLoopConfig`.
3. `Node::spawn(...)` starts the event loop, or `run(...)` when the caller owns the task.[^arch]

# Setup kinds

| `SetupKind` | Use when |
|---|---|
| `Create` | Starting a new [mesh](/concepts/mesh.md). Mints the seed and stretches any password. |
| `Join` | Joining an existing mesh by id, or by [invite](/concepts/invite.md). |
| `Topic` | Meeting everyone who uses the same string. See [topic mesh](/concepts/topic-mesh.md). |

# Driver modes

- `Cli`: binds a control socket and exits the process on quit.
- `InProcess`: takes typed requests over channels and returns cleanly. Use this when embedding the engine in an app such as agent-knowledge.[^arch]

[^arch]: habilis-network architecture, section 6.1
