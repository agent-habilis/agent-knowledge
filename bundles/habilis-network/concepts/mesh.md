---
type: Glossary Term
title: Mesh
description: "A group of peers defined by a seed, a name, and a config; its id is Base58Check text."
tags: [identity, core]
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

A mesh is a `Mesh` value: a seed, a name, and a config. The mesh id is its
Base58Check text. The config is mixed into the gossip topic, so every member
shares it.[^agents]

# Related

- A [topic mesh](/concepts/topic-mesh.md) derives its seed from a string.
- Members are [peers](/concepts/peer.md); new joiners find it through the [rendezvous](/concepts/rendezvous.md).
- Access can be gated with an [invite](/concepts/invite.md).

# Code

`protocol::meshes` (`Mesh`, `MeshId`, `MeshConfig`).

[^agents]: AGENTS.md concept glossary
