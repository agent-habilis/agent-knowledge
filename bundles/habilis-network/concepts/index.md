# Identity

* [Mesh](mesh.md) - A group of peers defined by a seed, a name, and a config; its id is Base58Check text.
* [Topic mesh](topic-mesh.md) - A mesh whose seed comes from a string, so every peer using the same string meets.
* [Identity key](identity-key.md) - The Ed25519 key a process makes on create or join; its public key identifies the author and signs every message.
* [Invite](invite.md) - A signed, expiring ticket from the mesh creator, required to join an invite-only mesh.

# Membership and messaging

* [Peer](peer.md) - A mesh member other than yourself, identified by its author key; a nickname is only a label.
* [Directed send](directed-send.md) - A message to exactly one peer, addressed by author key in the signed to field and sealed to that peer.
* [Shared state](shared-state.md) - A JSON document every mesh member replicates with automerge, on two channels per mesh: state and meta.

# Bootstrap and discovery

* [Rendezvous](rendezvous.md) - The bootstrap identity every joiner recomputes from the mesh: an endpoint keypair and a ladder of ports.
* [Beacon](beacon.md) - The live member that serves the rendezvous endpoint; the role moves to another member when it dies.
* [Lookup](lookup.md) - A way to resolve the rendezvous id to an address: mDNS on the LAN, the mainline DHT, or a relay.
