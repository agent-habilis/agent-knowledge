---
type: Software Library
title: habilis-network
description: "Serverless gossip-network engine where peers find each other, form a mesh, and exchange signed messages and shared state."
resource: https://github.com/agent-habilis/habilis-network
tags: [rust, p2p, gossip, iroh, automerge]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: readme
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/README.md
    title: habilis-network README
    author: human:caiogondim
    last_modified: 2026-11-06T21:05:53Z
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md, guide for AI agents
    author: human:caiogondim
    last_modified: 2026-11-06T21:05:53Z
---

# Summary

A portable, serverless gossip-network engine for native and web.[^readme]
It moves, signs, routes, and heals bytes between peers and does not know
what the bytes mean.[^agents] Transport is built on
[iroh](https://github.com/n0-computer/iroh) and shared state on
[automerge](https://automerge.org).[^readme]

# Features

- **Serverless**: no server to host, no account to create.
- **Encrypted**: every link is QUIC over TLS, and every message is signed with an [Ed25519 identity key](/concepts/identity-key.md).
- **Mixed mesh**: UDP, relay server, and WebRTC peers in the same [mesh](/concepts/mesh.md).
- **Shared state**: a CRDT document every member converges on. See [shared state](/concepts/shared-state.md).
- **Self-healing**: the mesh re-forms as peers come and go.
- **Byte streams**: direct 1-1 streams between two peers, through [habilis-network-stream](/crates/habilis-network-stream.md).
- **Embeddable**: joining a mesh is a function call, not a daemon.[^readme]

# Using it as a dependency

The crates set `publish = false`, so they are not on crates.io. Depend on
the git repository:

```toml
habilis-network = { git = "https://github.com/agent-habilis/habilis-network" }
```

The workspace targets Rust edition 2024 with `rust-version = "1.93"`. For a
wasm32 build, the consumer's own `.cargo/config.toml` needs
`rustflags = ["--cfg", 'getrandom_backend="wasm_js"']`.

See [Join a mesh](/guides/join-a-mesh.md) for the three calls that start a node.

[^readme]: habilis-network README
[^agents]: AGENTS.md, guide for AI agents
