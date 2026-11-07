---
type: Glossary Term
title: Topic mesh
description: "A mesh whose seed comes from a string, so every peer using the same string meets."
tags: [identity, discovery]
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

A [mesh](/concepts/mesh.md) whose seed is derived from a string, not minted at
random. The string alone fixes the mesh, so every peer that uses the same
string meets.[^agents] It has no distinguished creator, so the first peer
claims the [beacon](/concepts/beacon.md) eagerly.

# Code

`crypto::topic_seed`, `Mesh::for_topic`.

[^agents]: AGENTS.md concept glossary
