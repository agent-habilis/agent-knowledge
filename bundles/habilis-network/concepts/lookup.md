---
type: Glossary Term
title: Lookup
description: "A way to resolve the rendezvous id to an address: mDNS on the LAN, the mainline DHT, or a relay."
tags: [discovery, networking]
layer: lookup
generated: { by: claude-code/claude-opus-5-5, at: 2026-11-07T19:14:23Z }
status: draft
sources:
  - id: agents
    resource: https://github.com/agent-habilis/habilis-network/blob/fc36a80639a6f2f69d5f7f623e2417ffbc61cc7a/AGENTS.md
    title: AGENTS.md concept glossary
    author: human:caiogondim
---

# Definition

A way to resolve the [rendezvous](/concepts/rendezvous.md) id into a reachable
address: mDNS on the LAN, the mainline DHT, or a relay. Each lookup is
complete on its own; the others add reliability and are never a
dependency.[^agents]

# Code

`habilis-network/src/lookup`.

[^agents]: AGENTS.md concept glossary
