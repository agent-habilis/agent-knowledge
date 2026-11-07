---
type: Glossary Term
title: Directed send
description: "A message to exactly one peer, addressed by author key in the signed to field and sealed to that peer."
tags: [messaging]
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

A message with one recipient. The frame names the recipient by author key in
the signed `to` field.[^agents] The body is sealed to the x25519 public key
of the addressee. A caller holding only a nickname asks `key_of` for the one
live key, because a nickname can map to several [peers](/concepts/peer.md).

# Code

`EventLoopState::key_of`, `EventLoopState::addressee_key`, `Messaging::SendToKey`.

[^agents]: AGENTS.md concept glossary
