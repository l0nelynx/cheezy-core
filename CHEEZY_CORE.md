# Cheezy Core maintenance

This repository carries the small Android/WAP patch set used by CheezyClash.
The protected integration branch is `cheezy-wap`.

Upstream code is read only from the `Alpha` branches:

- primary: `https://github.com/vernesong/mihomo.git`, mirrored as
  `source/vernesong-alpha`;
- fallback: `https://github.com/MetaCubeX/mihomo.git`, mirrored as
  `source/metacubex-alpha`.

The scheduled sync workflow validates module identity, expected source-tree
shape, common Git ancestry, and core tests before updating mirror branches. It
then opens a pull request for manual review. It never merges upstream code into
`cheezy-wap` automatically.

CheezyClash must depend on a promoted commit SHA through a Go module `replace`.
It must not depend directly on a moving branch.

## Cheezy extensions

- [WAP upstream support](docs/wap-upstream.md) documents the bounded HTTP
  `CONNECT` outbound, destination-port allowlist, diagnostics, Android
  integration contract, and normal-mode compatibility.
