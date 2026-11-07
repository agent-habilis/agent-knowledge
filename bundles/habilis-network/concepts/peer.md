---
type: Glossary Term
title: Peer
description: "A mesh member other than yourself, identified by its author key; a nickname is only a label."
tags: [membership, core]
layer: membership
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md concept glossary
    author: human:caiogondim
---

# Definition

A member of the [mesh](/concepts/mesh.md) other than yourself. One author key
(see [identity key](/concepts/identity-key.md)) is one peer. A nickname is a
label, so two live keys that use one nickname are two peers. The roster is
the set of peers.[^agents]

# States

- **surfaced**: a peer whose arrival was shown to the operator or agent.
- **quiet**: silent past `ALIVE_TIMEOUT`; it can still return.

# Code

State `peers`; `lifecycle/membership.rs`.

[^agents]: AGENTS.md concept glossary
