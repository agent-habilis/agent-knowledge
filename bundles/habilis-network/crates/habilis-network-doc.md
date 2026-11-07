---
type: Rust Crate
title: habilis-network-doc
description: "Shared-state CRDT channels for habilis-network, built on automerge."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-doc
tags: [rust, crdt, automerge, state]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-doc/Cargo.toml
    title: habilis-network-doc Cargo.toml
    author: human:caiogondim
---

# Role

Gives every mesh member a replica of one JSON document.[^cargo] Members
exchange signed changes and converge on the same document. See
[shared state](/concepts/shared-state.md).

[^cargo]: habilis-network-doc Cargo.toml
