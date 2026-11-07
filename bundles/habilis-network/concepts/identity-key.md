---
type: Glossary Term
title: Identity key
description: "The Ed25519 key a process makes on create or join; its public key identifies the author and signs every message."
tags: [identity, crypto]
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

The Ed25519 key that a process makes when it creates or joins a
[mesh](/concepts/mesh.md). The public key is the identity of an author. The
nickname is a display label and no claim. Every message is signed with this
key, and a receiver verifies it.[^agents]

# Code

`protocol::identity`, `AuthorKey`.

[^agents]: AGENTS.md concept glossary
