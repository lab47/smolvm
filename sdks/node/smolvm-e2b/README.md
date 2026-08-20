# @smolvm/e2b

A self-hosted, [e2b](https://e2b.dev)-shaped **sandbox** SDK backed by [smolvm](https://smolmachines.com) microVMs.

It talks to a `smolvm serve` control plane over HTTP (TCP or a Unix socket) and mirrors the ergonomics of the e2b TypeScript SDK — `Sandbox.create`, `commands.run`, `files`, `setTimeout`, `pause`/`resume`, `connect`, `kill`. Porting existing e2b code is usually just changing the import and pointing `apiUrl` at your own server.

The headline features are **auto-idle** and **warm resume**: a sandbox can auto-pause after an idle window, and resuming restores its full RAM and *running processes* — not a cold reboot.

## Install

```bash
npm install @smolvm/e2b
```

Requires a running control plane:

```bash
smolvm serve start -l unix:///run/user/$(id -u)/smolvm.sock
# or a loopback TCP port:
smolvm serve start -l 127.0.0.1:8080
```

## Quick start

```ts
import { Sandbox } from "@smolvm/e2b";

// Point at your smolvm serve (or set SMOLVM_API_URL).
const apiUrl = "unix:///run/user/1000/smolvm.sock";

const sbx = await Sandbox.create({
  template: "python:3.12",   // any OCI image
  timeoutMs: 5 * 60_000,     // auto-pause after 5 min idle
  apiUrl,
});

const r = await sbx.commands.run("python -c 'print(2 + 2)'");
console.log(r.stdout.trim()); // "4"

await sbx.files.write("/tmp/data.json", JSON.stringify({ ok: true }));
console.log(await sbx.files.read("/tmp/data.json"));

// Suspend to disk (RAM + running processes preserved), resume later.
const sandboxId = await sbx.pause();
const resumed = await Sandbox.resume(sandboxId, { apiUrl });
await resumed.kill();
```

## Connecting

Every entry point accepts connection options (or reads `SMOLVM_API_URL` / `SMOLVM_API_KEY`):

| Option | Meaning |
|---|---|
| `apiUrl` | `http://host:port`, `https://host:port`, or `unix:///path/to.sock` |
| `apiKey` | Bearer token for fleet auth (`Authorization: Bearer …`) |
| `tls` | `{ ca, cert, key }` for mTLS over TCP (fleet mode) |
| `requestTimeoutMs` | default per-request timeout |

## API

### Lifecycle
- `Sandbox.create(opts)` — create **and start** a sandbox; returns a `Sandbox`.
- `Sandbox.connect(sandboxId, opts)` — reattach to a running sandbox.
- `Sandbox.resume(sandboxId, opts)` — resume a paused sandbox (running processes survive).
- `Sandbox.list(opts)` — list all sandboxes.
- `Sandbox.kill(sandboxId, opts)` — delete by id (static).
- `sbx.pause()` — suspend to disk, free compute; returns the `sandboxId`.
- `sbx.setTimeout(ms)` — set/extend the auto-idle window (`0` disables).
- `sbx.getInfo()` / `sbx.isRunning()` — status.
- `sbx.kill()` — delete the sandbox.

### Commands — `sbx.commands`
- `run(cmd, opts?)` — run a command and await the result (`{ exitCode, stdout, stderr }`).
  - `cmd` is a shell string (run via `sh -c`) or an argv array (run directly).
  - `opts`: `cwd`, `envs`, `timeoutMs`, `stdin`, `background`, `throwOnError` (default `true`), `onStdout`, `onStderr`.
  - Pass `onStdout`/`onStderr` to **stream** output live as it arrives; the call still resolves with the full `{ exitCode, stdout, stderr }`.
  - `{ background: true }` returns `{ pid }` immediately for long-lived daemons.

```ts
await sbx.commands.run("npm run build", {
  onStdout: (chunk) => process.stdout.write(chunk),
  onStderr: (chunk) => process.stderr.write(chunk),
});
```

### Terminal — `sbx.pty`
- `create({ cmd, cols, rows, onData })` — start an interactive PTY; returns a handle.
  - `handle.sendStdin(data)`, `handle.resize(cols, rows)`, `handle.kill()`, and `await handle.exited` (exit code).

```ts
const pty = await sbx.pty.create({ cmd: "/bin/bash", onData: (d) => process.stdout.write(d) });
pty.sendStdin("ls -la\n");
// ...later
pty.sendStdin("exit\n");
await pty.exited;
```

### Files — `sbx.files`
- `write(path, data)` — `data` is a string or `Uint8Array`.
- `read(path, { format })` — `"text"` (default) → `string`, `"bytes"` → `Buffer`.
- `list(dir)`, `remove(path)`, `rename(from, to)`, `exists(path)`, `makeDir(path)`, `getInfo(path)`.

### Metrics — `sbx.getMetrics()`
Point-in-time resource sample: `{ cpuMillis, memMb, diskMb, egressBytes }`.

## Auto-idle & warm resume

`timeoutMs` on `create` (or `setTimeout(ms)`) starts an idle window. Activity (`commands`/`files`) and `setTimeout` push it forward. When it elapses, the control plane **pauses** the sandbox — a suspend-to-disk checkpoint of its full RAM + running processes. `Sandbox.resume(id)` brings it back exactly where it left off, potentially much later. This is how you keep a fleet of sandboxes cheap without losing in-progress work.

## Exposing sandbox services (`getHost`)

Expose a port a service listens on inside the sandbox, then reach it through the **preview proxy** (`smolvm proxy`):

```ts
const sbx = await Sandbox.create({
  template: "node:22",
  ports: [3000],                 // expose guest port 3000
  previewDomain: "preview.example.com",
  apiUrl,
});
// ...start your server on :3000 inside the sandbox...
const host = sbx.getHost(3000);  // "3000-<sandboxId>.preview.example.com"
// → open  https://${host}
```

Run the proxy (a standalone HTTP server) and point wildcard DNS `*.preview.example.com` at it:

```bash
smolvm proxy --listen 0.0.0.0:8080 --serve unix:///run/user/1000/smolvm.sock
```

The proxy takes the **first DNS label** of the `Host` header (`3000-<sandboxId>`) and ignores everything after the first dot, so any base domain works. It resolves the sandbox's auto-allocated host port and forwards HTTP and WebSocket traffic to it. A request to a **paused** sandbox **auto-resumes** it first (e2b behavior), then forwards — so idle sandboxes cost nothing until a request arrives.

Notes and limits:
- Ports must be declared in `create({ ports })` — smolvm can't add a port to a running sandbox.
- smolvm has no routable guest IP; the proxy reaches the guest through the published (loopback) host port, which is why the proxy runs alongside the control plane / on the same host as the published ports (or set `--upstream-host` / `SMOLVM_PUBLISH_ADDR`).

## Errors

`SmolvmError` (base), `NotFoundError` (404), `ConflictError` (409, e.g. resuming a non-paused sandbox), `AuthError` (401/403), and `CommandExitError` (non-zero exit, unless `throwOnError: false`).

## License

Apache-2.0
