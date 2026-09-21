# REALITY: ML-DSA-65 and ML-KEM

## Dependency and upstream review

The uTLS module is replaced with `github.com/legiz-ru/prizrak-utls` at
`v0.0.0-20260910220934-80ad70380fe8` (commit `80ad70380fe8` on
`v1.9.0-mod-meta-mldsa`). The immutable pin preserves the module import path
and Go 1.20 compatibility declared by the dependency. It includes the newer
browser fingerprints and the Xray-compatible REALITY ML-DSA-65 primitive.

Reviewed the latest 60 commits of Prizrak-Core `v1-19-31`, whose head was
`265a587050f952f645d09474efae8b8173a6679c` on 2026-09-21. Relevant changes:

| Commit | Change | Included here |
| --- | --- | --- |
| [5062e464](https://github.com/legiz-ru/Prizrak-Core/commit/5062e464365aa1f492a4f29b6d72a6c9adf1d7bf) | uTLS mod-meta fingerprints | Via the ML-DSA fork |
| [da5f4ba0](https://github.com/legiz-ru/Prizrak-Core/commit/da5f4ba061f3876b4e15f34bde8d73f4ecad6eb7) | Pin prizrak-utls with ML-DSA-65 | Yes |
| [2197a3ca](https://github.com/legiz-ru/Prizrak-Core/commit/2197a3cac1e960987e3844a638a281509bb265ae) | Preserve fingerprints, default REALITY to Chrome, fix time units | Yes; existing client version 26.7.28 preserved |
| [585101be](https://github.com/legiz-ru/Prizrak-Core/commit/585101be7fa203b2cea6e04d5ede07a3091f637c) | ML-DSA plus broader Xray alignment | ML-DSA client/server integration and relevant tests |
| [ebc34f4f](https://github.com/legiz-ru/Prizrak-Core/commit/ebc34f4f5275d05ebbecf261e02aaeb5e7333c4f) | Remove obsolete ML-KEM flag | Yes |

The broader `585101be` changes to spider crawling, key logging, target/password
aliases, PROXY protocol, client version restrictions and half-close handling
are not part of this port. In particular, the existing server CloseWrite
workaround is preserved. Unrelated mux, subscription-provider and release
workflow commits were reviewed by scope but not ported.

## Configure ML-DSA-65

On the client, add the verification key to the existing proxy:

```yaml
client-fingerprint: chrome
reality-opts:
  public-key: <existing X25519 public key>
  short-id: <existing short ID>
  mldsa65-verify: <ML-DSA-65 public key, base64url without padding>
```

On the server, add to the existing listener's `reality-config`:

```yaml
reality-config:
  dest: example.com:443
  private-key: <existing X25519 private key>
  short-id: ["0123456789abcdef"]
  server-names: [example.com]
  mldsa65-seed: <independent 32-byte seed, base64url without padding>
```

The public verification key is 1952 bytes before encoding. The seed is 32
bytes and must differ from the X25519 private key. Generate the matching pair
with Xray's `mldsa65` command; use its Seed on the server and Verify on the
client. These are YAML settings; this change does not add share-link imports
for ML-DSA parameters.

When `mldsa65-verify` is configured, the client requires both the existing
REALITY HMAC authentication and a valid ML-DSA-65 signature over the HMAC
transcript (Ed25519 public key, ClientHello and ServerHello). It locates the
signature by extension OID `0.0`. Missing or invalid signatures cannot fall
back to HMAC-only authentication. Omitting the verification key retains
HMAC-only authentication, including against servers that send ML-DSA signatures.

REALITY's camouflage target must have a sufficiently large TLS certificate
flight to accommodate the ML-DSA signature. The local tests use a padded
certificate chain; enabling a seed does not make every target suitable.

## Why the ML-KEM flag disappeared

ML-KEM is key establishment; ML-DSA is digital signing. They serve different
purposes and have separate configuration implications.

Previously `support-x25519mlkem768: false` (the default) stripped the hybrid
group and key share from the selected browser fingerprint. The earlier
`2197a3ca` change stopped stripping them, making that flag inert. `ebc34f4f`
then deleted only the obsolete option and duplicate flag-specific tests.
It did not remove ML-KEM support.

REALITY now preserves the chosen browser's advertised groups. The actual
TLS group is negotiated with the server. For an old REALITY server that
cannot handle hybrid key shares, explicitly select `chrome120`,
`firefox120` or `safari16`. Remove the old `support-x25519mlkem768` setting;
it no longer controls the handshake. This behavior change may require updating
old server configurations or choosing a legacy fingerprint.

`max-time-difference` on the server now uses milliseconds, matching Xray
and Prizrak-Core (the old implementation interpreted the value as microseconds).

## Verification

Tests cover fingerprint preservation, encrypted client version, missing
ML-DSA extensions, key and seed validation, default fingerprint, and time
units. Process interoperability tests exchange 128 KiB with Xray 26.7.28 in
both directions, including ML-DSA, Vision and rejection of a wrong ML-DSA key.
Set `XRAY_BINARY` to run those tests; otherwise they skip explicitly.

```sh
go test ./component/tls ./adapter/outbound ./listener/reality ./transport/vmess -run 'Reality|XrayMux' -count=1
go test ./adapter/outbound/xraymux -count=1
XRAY_BINARY=/path/to/xray go test ./listener/inbound -run 'TestVLESSRealityXray|TestInboundVless_Reality' -count=1
go build -buildvcs=false ./...
```
