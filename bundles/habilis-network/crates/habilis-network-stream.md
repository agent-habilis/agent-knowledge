---
type: Rust Crate
title: habilis-network-stream
description: "1-1 byte streams between two peers, addressed by a hash, over QUIC or a WebRTC data channel, never through gossip."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-stream
tags: [rust, streams, quic, webrtc]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-stream/Cargo.toml
    title: habilis-network-stream Cargo.toml
    author: human:caiogondim
---

# Role

For moving bytes directly between two [peers](/concepts/peer.md) rather than
broadcasting to the whole [mesh](/concepts/mesh.md).[^cargo] Use it when a
message is too large or too private for gossip.

[^cargo]: habilis-network-stream Cargo.toml
