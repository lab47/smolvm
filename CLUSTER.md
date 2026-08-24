# Running smolvm as a cluster

A single `smolvm serve` runs VMs on one host. A **cluster** spreads that across
machines: one public entry point that places and routes work, several machines
that actually run the VMs, and a preview proxy per machine so services inside a
sandbox are reachable over HTTP.

This guide walks through standing up a working cluster: two backends (each with
its own data directory), one frontend, and two URL proxies. It gives a
single-host "lab" version you can run on one machine to try it out, and notes
what changes for real multi-host deployments.

## The pieces

- **Backend** — a `smolvm serve` in `--cluster-role backend`. It runs the VMs
  and answers requests the frontend forwards to it. Backends *dial out* to the
  frontend; they don't listen for it.
- **Frontend** — a `smolvm serve` in `--cluster-role frontend`. It's the public
  API (the address your SDK/CLI talks to). It places new sandboxes on a backend
  and routes every follow-up request to whichever backend owns that sandbox. It
  does not run VMs itself.
- **URL proxy** — `smolvm proxy`. An HTTP reverse proxy that turns
  `<port>-<sandboxId>.<domain>` hostnames into a connection to the service
  running inside that sandbox. This is what `Sandbox.getHost(port)` produces.

They find each other over **iroh** (a peer-to-peer networking layer). A backend
is pointed at the frontend by the frontend's **endpoint id** (an ed25519 public
key the frontend prints on startup). All nodes share a **cluster secret**, which
derives the gossip topic — nodes with different secrets can't see each other.

```
                         SDK / CLI / curl
                                │  http
                          ┌─────▼─────┐
                          │ frontend  │   places + routes; runs no VMs
                          └──┬─────┬──┘
                     iroh    │     │    iroh
                     ┌───────▼─┐ ┌─▼───────┐
                     │ backend │ │ backend │   run the VMs
                     │    A    │ │    B    │
                     └────┬────┘ └────┬────┘
                     ┌────▼────┐ ┌────▼────┐
   *.a.preview ─────▶│ proxy A │ │ proxy B │◀───── *.b.preview
                     └─────────┘ └─────────┘   one proxy per backend
```

One thing to understand up front: **the control plane clusters, the preview
plane does not.** The frontend routes API calls to any backend, but a service
inside a sandbox is published on a host port *on the backend that runs it*. A
proxy reaches sandboxes at a single `--upstream-host`, so the working model is
**one proxy co-located with each backend**, each serving the sandboxes on its
own backend (see [URL proxies](#step-4--url-proxies-one-per-backend)).

## Prerequisites

On every host that will run a backend:

- **smolvm installed** — `scripts/install.sh` lays down the `smolvm` binary, the
  `libkrun` libraries it loads at runtime, and the ext4 disk templates and guest
  `agent-rootfs` it needs to boot VMs. A bare `cargo build` binary is *not*
  enough to run VMs — it builds fine but has no libkrun/templates.
- **KVM** — backends boot real VMs, so the host needs `/dev/kvm` (Linux with
  hardware virtualization, or a macOS host with the mac build).

The frontend runs no VMs, so it needs neither KVM nor the templates — just the
binary.

## Configuration reference

Most setup is environment variables. The ones that matter for a cluster:

| Variable | Who | Purpose |
|---|---|---|
| `SMOLVM_CLUSTER_SECRET` | all | Shared secret; derives the gossip topic. **Must match on every node.** (Or `--cluster-secret`.) |
| `SMOLVM_CLUSTER_BOOTSTRAP` | backends | The frontend endpoint id(s) to dial, comma-separated. (Or `--cluster-bootstrap`, repeatable.) |
| `SMOLVM_CLUSTER_BIND_ADDR` | frontend | IP (or `IP:port`) the frontend advertises so backends can reach it. Set this to a routable address on multi-host. |
| `SMOLVM_DATA_DIR` | each node | Where this node keeps its state — VMs, DB, and its cluster identity key. **Unique per node.** |
| `SMOLVM_GUEST_ROLLOUT_HOST_PORT` | backends | Loopback port for the guest rollout listener (default `10081`). **Unique per backend when several share a host.** |
| `SMOLVM_CONTROL_API_KEY` | frontend + backends | If set, the e2b `/sandboxes` API requires `X-API-Key: <this>`. Set the **same value everywhere**, or leave unset for no auth. |
| `SMOLVM_PREVIEW_DOMAIN` | proxies / SDK | Base domain used to build `getHost()` URLs. |
| `SMOLVM_PUBLISH_ADDR` | backends | Address a backend binds published sandbox ports on (default `127.0.0.1`). Set to the backend's routable IP if the proxy is on another host. |

**Stable endpoint id.** A node persists its iroh identity at
`<data-dir>/smolvm/node-credentials/cluster.key` and reuses it on restart. So a
frontend keeps the same endpoint id **as long as its data directory survives** —
which is why you should pin `SMOLVM_DATA_DIR` for the frontend, so backends'
bootstrap ids don't change out from under them. Wipe the data dir and the id
regenerates.

---

## Step 1 — Start the frontend

The frontend listens on **TCP** (it's your public API). Pin its data dir so its
endpoint id is stable, and give it the cluster secret.

```bash
# Pick a secret once and reuse it on every node.
export SMOLVM_CLUSTER_SECRET='pick-a-long-shared-secret'

# SMOLVM_CLUSTER_BIND_ADDR is the address backends dial over iroh — set it to the
# frontend's routable IP (not 0.0.0.0), which is what backends must be able to reach.
SMOLVM_DATA_DIR=/var/lib/smolvm-frontend \
SMOLVM_CLUSTER_BIND_ADDR=<frontend-routable-ip> \
smolvm serve start -l 0.0.0.0:9000 --cluster-role frontend
```

On startup it prints its endpoint id — grab it, the backends need it:

```
cluster frontend endpoint id: 2d6fec94d18feed482cf80bb889fa076e59c71113dbc79ab8a82ac3ff6086dcc
cluster frontend listening (dynamic membership) listen=0.0.0.0:9000 endpoint_id=2d6f…
```

To require auth on the e2b API, also set `SMOLVM_CONTROL_API_KEY` here (and on
every backend):

```bash
SMOLVM_CONTROL_API_KEY=secret SMOLVM_DATA_DIR=/var/lib/smolvm-frontend \
smolvm serve start -l 0.0.0.0:9000 --cluster-role frontend
```

## Step 2 — Start the backends (each with its own directory)

Each backend listens on a **loopback Unix socket** (only the frontend reaches it,
over iroh — it should not be a public TCP port), points `--cluster-bootstrap` at
the frontend's endpoint id, and shares the secret. The important part is that
**every backend gets its own `SMOLVM_DATA_DIR`** — that's where its VMs, disks,
and identity live. When two backends share one host they also need distinct
rollout ports and socket paths.

Set the frontend id once:

```bash
export FRONTEND_ID=2d6fec94d18feed482cf80bb889fa076e59c71113dbc79ab8a82ac3ff6086dcc
export SMOLVM_CLUSTER_SECRET='pick-a-long-shared-secret'   # same as the frontend
```

**Backend A:**

```bash
SMOLVM_DATA_DIR=/var/lib/smolvm-backend-a \
SMOLVM_GUEST_ROLLOUT_HOST_PORT=10081 \
smolvm serve start -l unix:///run/smolvm-a.sock \
  --cluster-role backend \
  --cluster-bootstrap "$FRONTEND_ID"
```

**Backend B** — different data dir, different rollout port, different socket:

```bash
SMOLVM_DATA_DIR=/var/lib/smolvm-backend-b \
SMOLVM_GUEST_ROLLOUT_HOST_PORT=10082 \
smolvm serve start -l unix:///run/smolvm-b.sock \
  --cluster-role backend \
  --cluster-bootstrap "$FRONTEND_ID"
```

Each backend prints its own `cluster backend endpoint id: …` and dials the
frontend. Within a couple of seconds it shows up in the frontend's roster.

> On a **multi-host** deployment, each backend is on its own machine, so the
> rollout port and socket path don't need to differ (defaults are fine) — the
> only per-node requirement is a distinct `SMOLVM_DATA_DIR` and reaching the
> frontend's `SMOLVM_CLUSTER_BIND_ADDR`. Set `SMOLVM_PUBLISH_ADDR` to the
> backend's routable IP so its proxy can reach published ports (Step 4).

## Step 3 — Verify the cluster

Point the new `cluster` CLI at the frontend (add `--api-key` if you set a control
key):

```bash
smolvm cluster status --url http://<frontend-host>:9000
```

```
cluster — 2 backend(s), 0 machine(s)

● backend 96854028b827…  vms=0 cpu=0.0 mem_free=56410MB direct age=3s
    (no placed sandboxes in the frontend registry)
● backend 0b0821abd4fb…  vms=0 cpu=0.0 mem_free=55809MB direct age=2s
    (no placed sandboxes in the frontend registry)
```

Other views: `cluster backends`, `cluster members`, `cluster machines`,
`cluster placements`. Add `--json` for raw output.

## Step 4 — URL proxies (one per backend)

A proxy resolves `<port>-<sandboxId>.<domain>` by asking a serve API for the
sandbox's published host port, then forwards to `<upstream-host>:<hostPort>`.
Because a published port lives on the backend that runs the sandbox, and a proxy
has a **single** `--upstream-host`, the reliable layout is **one proxy per
backend**, co-located with it:

- `--serve` points at the **frontend**, so the proxy can resolve (and auto-resume)
  any sandbox the cluster knows about.
- `--upstream-host` points at **that backend's** published ports — `127.0.0.1`
  when the proxy runs on the backend host, or the backend's IP otherwise.

**Proxy for backend A** (running on backend A's host):

```bash
smolvm proxy \
  --listen 0.0.0.0:8080 \
  --serve http://<frontend-host>:9000 \
  --upstream-host 127.0.0.1
```

**Proxy for backend B** (running on backend B's host):

```bash
smolvm proxy \
  --listen 0.0.0.0:8080 \
  --serve http://<frontend-host>:9000 \
  --upstream-host 127.0.0.1
```

If the frontend requires a Bearer token, pass `--api-key`. (The e2b
`X-API-Key` control key is *not* needed here — the proxy resolves over
`/api/v1/machines`, which isn't key-gated.)

**Routing preview traffic to the right proxy.** Everything after the first dot of
the hostname is ignored, so give each backend its own preview subdomain and point
its wildcard DNS at that backend's proxy:

```
*.a.preview.example.com  ->  backend A's proxy
*.b.preview.example.com  ->  backend B's proxy
```

Then create sandboxes destined for a backend with the matching
`previewDomain`/`SMOLVM_PREVIEW_DOMAIN` (e.g. `a.preview.example.com`), so
`getHost()` yields a URL that lands on the right proxy. A single wildcard across
both backends only works if you put an outer router in front that maps
`sandboxId → backend` — smolvm doesn't ship one.

## Step 5 — Create a sandbox and reach it

Talk to the **frontend** with the SDK (or curl). It places the sandbox on a
backend; follow-up calls route there automatically.

```ts
import { Sandbox } from "@smolvm/e2b";

const sbx = await Sandbox.create({
  apiUrl: "http://<frontend-host>:9000",
  apiKey: "secret",                  // only if SMOLVM_CONTROL_API_KEY is set
  template: "alpine",
  ports: [3000],                     // publish guest port 3000
  previewDomain: "a.preview.example.com",
});

// start something listening on :3000 inside the sandbox, then:
console.log(sbx.getHost(3000));      // 3000-<id>.a.preview.example.com
```

A request to `http://3000-<id>.a.preview.example.com/` hits that backend's proxy,
which resolves the sandbox through the frontend, resumes it if paused, and
forwards to the service inside.

---

## Single-host lab (everything on one machine)

To try the whole thing on one box, run all five processes locally in separate
terminals. Give each its own data dir and, for the backends, distinct rollout
ports and sockets. Use loopback everywhere.

```bash
export SMOLVM_CLUSTER_SECRET=labsecret

# 1) frontend  (note the printed endpoint id)
SMOLVM_DATA_DIR=/tmp/smol-fe SMOLVM_CLUSTER_BIND_ADDR=127.0.0.1 \
  smolvm serve start -l 127.0.0.1:9000 --cluster-role frontend

# 2) backend A   (FRONTEND_ID = the id printed above)
SMOLVM_DATA_DIR=/tmp/smol-a SMOLVM_GUEST_ROLLOUT_HOST_PORT=10081 \
  smolvm serve start -l unix:///tmp/smol-a.sock \
  --cluster-role backend --cluster-bootstrap "$FRONTEND_ID"

# 3) backend B
SMOLVM_DATA_DIR=/tmp/smol-b SMOLVM_GUEST_ROLLOUT_HOST_PORT=10082 \
  smolvm serve start -l unix:///tmp/smol-b.sock \
  --cluster-role backend --cluster-bootstrap "$FRONTEND_ID"

# 4) + 5) a proxy per backend  (both resolve via the frontend; both reach
#          127.0.0.1 since everything is on this host — use different listen ports)
smolvm proxy --listen 127.0.0.1:8081 --serve http://127.0.0.1:9000 --upstream-host 127.0.0.1
smolvm proxy --listen 127.0.0.1:8082 --serve http://127.0.0.1:9000 --upstream-host 127.0.0.1

# check it
smolvm cluster status --url http://127.0.0.1:9000
```

On a single host both backends' published ports are on `127.0.0.1`, so either
proxy can actually reach either backend — handy for testing, but not the model to
rely on across real hosts.

## Gotchas

- **Same secret, same control key, everywhere.** A mismatched
  `SMOLVM_CLUSTER_SECRET` makes nodes invisible to each other; a mismatched
  `SMOLVM_CONTROL_API_KEY` makes the frontend's internal calls to backends fail.
- **Pin the frontend's data dir.** Its endpoint id lives there; lose the dir and
  every backend's `--cluster-bootstrap` is now wrong.
- **Unique rollout ports and sockets per backend on a shared host.** The default
  rollout port is `10081`; a second backend on the same host must override
  `SMOLVM_GUEST_ROLLOUT_HOST_PORT`.
- **Backends need a real install, not just the binary.** No libkrun or templates
  means VMs fail to boot with a libkrun/rootfs error, even though the process
  starts and joins the cluster fine.
- **The frontend has no auth of its own beyond the optional control key.** Keep
  it on a trusted network or behind your own TLS/ingress; backend sockets should
  stay loopback.
