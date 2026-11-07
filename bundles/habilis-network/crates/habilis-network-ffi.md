---
type: Rust Crate
title: habilis-network-ffi
description: "A C-ABI shim so a non-Rust process can join a gossip mesh in-process."
resource: https://github.com/agent-habilis/habilis-network/tree/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-ffi
tags: [rust, ffi, c-abi, interop]
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: cargo
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/crates/habilis-network-ffi/Cargo.toml
    title: habilis-network-ffi Cargo.toml
    author: human:caiogondim
---

# Role

Exposes the engine through a C header (`include/habilis_network.h`).[^cargo]
The npm package of the same name uses it to run the engine on Bun, Deno, and
Node.

[^cargo]: habilis-network-ffi Cargo.toml
