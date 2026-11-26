---
default: minor
---

# Let only the creator change a share

`agent-knowledge share <folder> --writers creator` makes a read-only share. The
creator signs each entry of its index. A peer applies an entry only if the
signature is valid. A peer that is not the creator puts back the creator version
of a file that it changes, and prints `read-only share: reverted <path>`. A
file that the mesh never gave the peer stays on the peer, and the peer prints
`read-only share: not published <path>`.

The hash of a creator share is `<mesh-id>.<creator-public-key>`. `mount` takes
the creator key from the hash, and `status` prints the whole hash. The default
is `--writers everybody`, which keeps the behavior of earlier versions.
