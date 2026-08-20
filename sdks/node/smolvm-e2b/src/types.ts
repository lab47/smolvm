import type { ConnectionOpts } from "./client.js";

/** Options for {@link Sandbox.create}. */
export interface SandboxOpts extends ConnectionOpts {
  /**
   * OCI image the sandbox runs (e2b calls this the "template").
   * Defaults to `"alpine"`. Use e.g. `"python:3.12"`, `"node:22"`.
   */
  template?: string;
  /** Explicit sandbox name/id. Auto-generated when omitted. */
  sandboxId?: string;
  /**
   * Auto-idle timeout in milliseconds. The sandbox auto-pauses after this long
   * of inactivity (refreshed by `commands`/`files` activity and `setTimeout`).
   * `0`/omitted = never auto-idle.
   */
  timeoutMs?: number;
  /** Environment variables for the sandbox workload. */
  envs?: Record<string, string>;
  /** vCPU count (default: server default). */
  cpus?: number;
  /** Memory in MiB (default: server default). */
  memoryMb?: number;
  /** Enable outbound network access (default: true). */
  network?: boolean;
  /** Workload command; defaults to keeping the sandbox alive (`sleep infinity`). */
  cmd?: string[];
  /** Working directory for the workload. */
  workdir?: string;
  /**
   * Guest ports to expose so a service inside the sandbox is reachable through
   * the preview proxy (`smolvm proxy`). Each becomes a `getHost(port)` URL. The
   * server auto-allocates the host port. Ports must be declared here at create
   * time (smolvm can't add them to a running sandbox).
   */
  ports?: number[];
  /**
   * Base domain for {@link Sandbox.getHost} URLs — the domain your `smolvm proxy`
   * is reachable at (wildcard `*.<previewDomain>` → the proxy). Everything after
   * the first dot is ignored by the proxy, so any value routes. Defaults to
   * `$SMOLVM_PREVIEW_DOMAIN` or `"localhost"`.
   */
  previewDomain?: string;
}

/** Options for reconnecting to a running sandbox. */
export interface ConnectOpts extends ConnectionOpts {
  /** Base domain for `getHost` URLs (see {@link SandboxOpts.previewDomain}). */
  previewDomain?: string;
}

/** Options for resuming a paused sandbox. */
export interface ResumeOpts extends ConnectionOpts {
  /** New auto-idle timeout (ms) to arm on resume. */
  timeoutMs?: number;
  /** Base domain for `getHost` URLs (see {@link SandboxOpts.previewDomain}). */
  previewDomain?: string;
}

/** Result of a completed command. */
export interface CommandResult {
  /** Process exit code. */
  exitCode: number;
  /** Captured standard output (UTF-8). */
  stdout: string;
  /** Captured standard error (UTF-8). */
  stderr: string;
}

/** Handle to a background command started with `{ background: true }`. */
export interface BackgroundCommandHandle {
  /** PID of the spawned process inside the sandbox, if known. */
  pid?: number;
}

/** Options for {@link Commands.run}. */
export interface CommandOpts {
  /** Working directory. */
  cwd?: string;
  /** Extra environment variables for this command. */
  envs?: Record<string, string>;
  /** Timeout in milliseconds. */
  timeoutMs?: number;
  /** Data to pipe to the command's stdin. */
  stdin?: string;
  /** Spawn detached and return immediately with the PID. */
  background?: boolean;
  /** Throw {@link CommandExitError} on non-zero exit (default: true). */
  throwOnError?: boolean;
}

/** Options for {@link Files.read}. */
export interface FileReadOpts {
  /** `"text"` (default) returns a string; `"bytes"` returns a Buffer. */
  format?: "text" | "bytes";
}

/** One entry from {@link Files.list}. */
export interface FileEntry {
  name: string;
  path: string;
  type: "file" | "dir";
}

/** Sandbox status as reported by the control plane. */
export interface SandboxInfo {
  sandboxId: string;
  /** `"running" | "paused" | "stopped" | "created" | ...` */
  state: string;
  cpus: number;
  memoryMb: number;
  pid?: number;
  createdAt: number;
}

/** Raw machine JSON from the API (camelCase). Internal. */
export interface MachineInfoJson {
  name: string;
  state: string;
  cpus: number;
  memoryMb: number;
  pid?: number;
  createdAt: number;
}
