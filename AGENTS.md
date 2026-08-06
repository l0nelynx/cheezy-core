# AGENTS.md

## Cursor Cloud specific instructions

### What this repo is
This is **mihomo** (a.k.a. Meta/Clash Meta kernel), a single Go binary
(`github.com/metacubex/mihomo`, entry point `main.go`). It is a rule-based
network proxy/tunnel daemon that exposes local HTTP/HTTPS/SOCKS proxy servers, a
built-in DNS server, and a RESTful API controller. It is a headless CLI/server
app — there is no GUI, so verify it with terminal/log output and the RESTful API.
This is a single product, not a monorepo. See `README.md` and `CHEEZY_CORE.md`.

### Toolchain
- Go is preinstalled (currently 1.22). `go.mod` declares `go 1.20` with **no
  `toolchain` directive**, so any Go >= 1.20 works. CI resolves the version via
  `go-version-file: go.mod`.
- Dependencies are Go modules only (no npm/pip/etc.). `go mod download` is the
  only install step; the update script runs it on startup.

### Build (standard commands, see `Makefile`)
- Plain build: `go build -o bin/mihomo .`
- gvisor TUN stack (what the `Makefile` uses via `-tags with_gvisor`):
  `go build -tags with_gvisor -o bin/mihomo .`
- `bin/` is gitignored.

### Test
- Unit tests (what the Cheezy Core CI runs, `.github/workflows/cheezy-core-ci.yml`):
  `go test ./...` from the repo root. No Docker or external services needed.
- Non-obvious: the `listener/inbound` package is CPU/RAM heavy (crypto across many
  protocols) and takes ~3-4 minutes on its own, so a full `go test ./...` run
  takes several minutes. Don't assume it hung.
- Useful skip env vars: `SKIP_INTEROP_TEST=1` skips vmess inbound interop tests
  that otherwise need an external Xray binary (`XRAY_BIN`); `SKIP_CONCURRENT_TEST=1`
  skips the concurrent stress subtests. CI sets `SKIP_INTEROP_TEST=1`.
- The separate `test/` directory is its own Go module containing the end-to-end
  protocol interop suite. It **requires a running Docker daemon** (it pulls
  Shadowsocks/V2Ray/Xray/Trojan/Snell/Hysteria images and launches them as
  containers). Docker is NOT set up in this environment and this suite is not part
  of core CI, so skip it unless you specifically set up Docker.

### Lint
- `make lint` runs `golangci-lint run ./...`. The repo's `.golangci.yaml` uses the
  **golangci-lint v1 config format**, so you must use golangci-lint **v1**
  (v2 cannot parse this config). The update script installs v1 to
  `$(go env GOPATH)/bin` (i.e. `~/go/bin`, which may not be on `PATH` — invoke it
  by full path or add that dir to `PATH`).
- Non-obvious: the current codebase has **pre-existing** staticcheck findings, so
  `golangci-lint run ./...` exits non-zero even on an unmodified tree. That is
  expected; it does not indicate a broken environment.

### Run the daemon (hello-world)
- `./bin/mihomo -d <config-dir>` (config dir holds `config.yaml`; `-t` tests the
  config without running). A minimal config with `mixed-port`,
  `external-controller`, and a `MATCH,DIRECT` rule is enough to route traffic.
- Example verification: `curl -x http://127.0.0.1:7890 https://example.com` routes
  through the proxy; `curl -H "Authorization: Bearer <secret>"
  http://127.0.0.1:9090/version` hits the RESTful API.
