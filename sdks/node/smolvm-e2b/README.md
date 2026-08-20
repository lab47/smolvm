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
  - `opts`: `cwd`, `envs`, `timeoutMs`, `stdin`, `background`, `throwOnError` (default `true`).
  - `{ background: true }` returns `{ pid }` immediately for long-lived daemons.

### Files — `sbx.files`
- `write(path, data)` — `data` is a string or `Uint8Array`.
- `read(path, { format })` — `"text"` (default) → `string`, `"bytes"` → `Buffer`.
- `list(dir)`, `remove(path)`, `rename(from, to)`, `exists(path)`.

## Auto-idle & warm resume

`timeoutMs` on `create` (or `setTimeout(ms)`) starts an idle window. Activity (`commands`/`files`) and `setTimeout` push it forward. When it elapses, the control plane **pauses** the sandbox — a suspend-to-disk checkpoint of its full RAM + running processes. `Sandbox.resume(id)` brings it back exactly where it left off, potentially much later. This is how you keep a fleet of sandboxes cheap without losing in-progress work.

## Errors

`SmolvmError` (base), `NotFoundError` (404), `ConflictError` (409, e.g. resuming a non-paused sandbox), `AuthError` (401/403), and `CommandExitError` (non-zero exit, unless `throwOnError: false`).

## License

Apache-2.0
