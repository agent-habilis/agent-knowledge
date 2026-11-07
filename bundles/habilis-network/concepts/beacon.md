---
type: Glossary Term
title: Beacon
description: "The live member that serves the rendezvous endpoint; the role moves to another member when it dies."
tags: [bootstrap, role]
layer: role
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md concept glossary
    author: human:caiogondim
---

# Definition

The live member that binds and serves the
[rendezvous](/concepts/rendezvous.md) endpoint. The rendezvous is the
identity; the beacon is the role of the member that serves it. The role moves
to another member when the holder dies.[^agents]

# Code

`habilis-network/src/beacon`.

[^agents]: AGENTS.md concept glossary
