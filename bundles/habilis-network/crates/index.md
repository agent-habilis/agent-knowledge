# Engine

* [habilis-network](habilis-network.md) - The serverless gossip-network engine itself: membership, messaging, lookups, and the event loop.
* [habilis-network-protocol](habilis-network-protocol.md) - Wire vocabulary: messages, mesh ids, nicknames, seed-derived identity, sealing, and invites.

# Data

* [habilis-network-doc](habilis-network-doc.md) - Shared-state CRDT channels for habilis-network, built on automerge.
* [habilis-network-stream](habilis-network-stream.md) - 1-1 byte streams between two peers, addressed by a hash, over QUIC or a WebRTC data channel, never through gossip.

# Other platforms

* [habilis-network-ffi](habilis-network-ffi.md) - A C-ABI shim so a non-Rust process can join a gossip mesh in-process.
* [habilis-network-wasm](habilis-network-wasm.md) - Mesh peer for the browser: the engine's membership layer behind a wasm-bindgen class.
