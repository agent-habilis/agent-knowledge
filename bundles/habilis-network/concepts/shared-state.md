---
type: Glossary Term
title: Shared state
description: "A JSON document every mesh member replicates with automerge, on two channels per mesh: state and meta."
tags: [state, crdt]
layer: state
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md concept glossary
    author: human:caiogondim
---

# Definition

A JSON document that the whole [mesh](/concepts/mesh.md) shares. It is an
automerge replica on each member. Members exchange signed changes, and the
same set of changes gives the same document on every member.[^agents] Each
mesh has two channels, `state` and `meta`.

# Code

[habilis-network-doc](/crates/habilis-network-doc.md), `protocol::Channel`.

[^agents]: AGENTS.md concept glossary
