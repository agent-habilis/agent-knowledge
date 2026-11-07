---
type: Rust Crate
title: habilis-network-protocol
description: "Wire vocabulary: messages, mesh ids, nicknames, seed-derived identity, sealing, and invites."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-protocol
tags: [rust, protocol, wire]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-protocol/Cargo.toml
    title: habilis-network-protocol Cargo.toml
    author: human:caiogondim
---

# Role

Defines what goes on the wire.[^cargo] Owns the types behind
[mesh](/concepts/mesh.md) ids, the [identity key](/concepts/identity-key.md),
[invites](/concepts/invite.md), and message sealing. Used by
[habilis-network](/crates/habilis-network.md).

[^cargo]: habilis-network-protocol Cargo.toml
