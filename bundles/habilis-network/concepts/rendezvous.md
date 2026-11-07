---
type: Glossary Term
title: Rendezvous
description: "The bootstrap identity every joiner recomputes from the mesh: an endpoint keypair and a ladder of ports."
tags: [bootstrap, discovery]
layer: identity
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md concept glossary
    author: human:caiogondim
---

# Definition

The bootstrap identity that every joiner computes from the
[mesh](/concepts/mesh.md): an endpoint keypair and a ladder of ports. No peer
address is stored; the rendezvous is computed again each time.[^agents] The
member that serves it is the [beacon](/concepts/beacon.md), and joiners
resolve it to an address with a [lookup](/concepts/lookup.md).

# Code

`protocol::crypto`, `Mesh::rendezvous_id`.

[^agents]: AGENTS.md concept glossary
