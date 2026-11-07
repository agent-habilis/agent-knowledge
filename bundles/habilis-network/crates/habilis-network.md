---
type: Rust Crate
title: habilis-network
description: "The serverless gossip-network engine itself: membership, messaging, lookups, and the event loop."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network
tags: [rust, engine, core]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network/Cargo.toml
    title: habilis-network Cargo.toml
    author: human:caiogondim
---

# Role

The core engine.[^cargo] It runs one `tokio::select!` event loop that reacts
to inputs, runs time-driven maintenance, and shuts down cleanly. Start here
for any app that joins a [mesh](/concepts/mesh.md). The lifecycle is three
calls; see [Join a mesh](/guides/join-a-mesh.md).

# Depends on

- [habilis-network-protocol](/crates/habilis-network-protocol.md) for the wire vocabulary.
- [habilis-network-doc](/crates/habilis-network-doc.md) for [shared state](/concepts/shared-state.md).

[^cargo]: habilis-network Cargo.toml
