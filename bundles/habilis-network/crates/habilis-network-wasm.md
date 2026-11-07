---
type: Rust Crate
title: habilis-network-wasm
description: "Mesh peer for the browser: the engine's membership layer behind a wasm-bindgen class."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-wasm
tags: [rust, wasm, browser]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-wasm/Cargo.toml
    title: habilis-network-wasm Cargo.toml
    author: human:caiogondim
---

# Role

Runs a [mesh](/concepts/mesh.md) peer inside a browser tab.[^cargo] A
browser node always runs in the `InProcess` driver mode. Peers reach it over
WebRTC.

[^cargo]: habilis-network-wasm Cargo.toml
