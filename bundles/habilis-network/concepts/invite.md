---
type: Glossary Term
title: Invite
description: "A signed, expiring ticket from the mesh creator, required to join an invite-only mesh."
tags: [identity, access]
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

An invite-only [mesh](/concepts/mesh.md) keeps its derivation secret out of
the mesh id, so a bare id reaches nothing. Joining needs an invite that the
creator minted. It carries the root key, an expiry, and the creator's
signature. Only the creating session holds the issuer key, so only that
session mints invites.[^agents]

# Code

`invite::mint`, `InviteTicket`, `Mesh::set_invite`.

[^agents]: AGENTS.md concept glossary
