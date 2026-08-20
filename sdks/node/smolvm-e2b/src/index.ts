/**
 * @smolvm/e2b — a self-hosted, e2b-shaped sandbox SDK backed by smolvm microVMs.
 *
 * Talks to a `smolvm serve` control plane over HTTP (TCP or Unix socket). Mirrors
 * the ergonomics of the e2b TypeScript SDK — `Sandbox.create`, `commands.run`,
 * `files`, `setTimeout`, `pause`/`resume`, `connect`, `kill` — so existing e2b
 * code ports by changing the import and pointing `apiUrl` at your own server.
 *
 * @example
 * ```ts
 * import { Sandbox } from "@smolvm/e2b";
 *
 * const sbx = await Sandbox.create({
 *   template: "python:3.12",
 *   timeoutMs: 300_000,             // auto-idle (pause) after 5 min idle
 *   apiUrl: "unix:///run/user/1000/smolvm.sock",
 * });
 * const r = await sbx.commands.run("python -c 'print(2 + 2)'");
 * console.log(r.stdout); // "4\n"
 *
 * const sandboxId = await sbx.pause();      // suspend to disk, free compute
 * const resumed = await Sandbox.resume(sandboxId); // running processes survive
 * await resumed.kill();
 * ```
 */
export { Sandbox } from "./sandbox.js";
export { Commands } from "./commands.js";
export { Files } from "./files.js";
export { Client } from "./client.js";
export type { ConnectionOpts, RequestOpts } from "./client.js";
export {
  SmolvmError,
  NotFoundError,
  ConflictError,
  AuthError,
  CommandExitError,
} from "./errors.js";
export type {
  SandboxOpts,
  ConnectOpts,
  ResumeOpts,
  CommandResult,
  CommandOpts,
  BackgroundCommandHandle,
  FileReadOpts,
  FileEntry,
  SandboxInfo,
} from "./types.js";
