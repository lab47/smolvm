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
   * Auto-idle timeout in milliseconds. The sandbox auto-idles after this long
   * of inactivity (refreshed by `commands`/`files` activity and `setTimeout`).
   * `0`/omitted = never auto-idle.
   */
  timeoutMs?: number;
  /**
   * What happens when the idle timeout elapses: `"pause"` (default, resumable
   * suspend-to-disk), `"stop"` (cold stop, disk kept), or `"kill"` (delete).
   */
  onTimeout?: "pause" | "stop" | "kill";
  /** Arbitrary key/value labels to find this sandbox later via `Sandbox.list`. */
  metadata?: Record<string, string>;
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

/**
 * A live handle to a background process (from `commands.run({ background:true })`
 * or `commands.connect(pid)`). Lets you stream its output, await its exit, send
 * it stdin, and kill it.
 */
export interface CommandHandle {
  /** PID of the process inside the sandbox. */
  readonly pid: number;
  /** Await the process's exit; resolves with the full captured output + code. */
  wait(): Promise<CommandResult>;
  /** Signal the process (`signal` defaults to `TERM`). */
  kill(signal?: string): Promise<boolean>;
  /** Write bytes to the process's stdin. */
  sendStdin(data: string | Uint8Array): Promise<void>;
  /** Stop streaming/awaiting from this handle; the process keeps running. */
  disconnect(): void;
}

/** Options for streaming a background process's output. */
export interface BackgroundStreamOpts {
  onStdout?: (data: string) => void;
  onStderr?: (data: string) => void;
  /** Poll interval for tailing output/exit, in ms (default 300). */
  pollMs?: number;
}

/** A process running inside the sandbox, from {@link Commands.list}. */
export interface ProcessInfo {
  pid: number;
  /** Full command line (argv joined by spaces). */
  cmd: string;
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
  /** Called with each chunk of stdout as it streams (switches to streaming). */
  onStdout?: (data: string) => void;
  /** Called with each chunk of stderr as it streams (switches to streaming). */
  onStderr?: (data: string) => void;
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

/** A filesystem change reported by {@link Files.watchDir}. */
export interface FileEvent {
  type: "create" | "modify" | "remove";
  name: string;
  path: string;
}

/** Handle to a running {@link Files.watchDir}; call `stop()` to end it. */
export interface FileWatcher {
  stop(): void;
}

/** Metadata for a single path, from {@link Files.getInfo}. */
export interface FileInfo {
  name: string;
  path: string;
  type: "file" | "dir";
  /** Size in bytes. */
  size: number;
  /** Octal permission bits, e.g. `"644"`. */
  mode: string;
  /** Last modification time. */
  modifiedAt: Date;
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
  /** User labels attached at create. */
  metadata: Record<string, string>;
}

/** Filters for {@link Sandbox.list}. */
export interface ListOpts extends ConnectionOpts {
  /** Only return sandboxes whose metadata contains all of these key/values. */
  metadata?: Record<string, string>;
}

/** A point-in-time resource sample for a sandbox. */
export interface SandboxMetrics {
  /** Consumed CPU time in milliseconds (a counter; resets on restart). */
  cpuMillis?: number;
  /** Resident memory of the sandbox VMM process, in MiB. */
  memMb?: number;
  /** Host disk consumed by the sandbox's data dir, in MiB. */
  diskMb?: number;
  /** Cumulative guest-outbound bytes since boot. */
  egressBytes?: number;
}

/** Raw machine JSON from the API (camelCase). Internal. */
export interface MachineInfoJson {
  name: string;
  state: string;
  cpus: number;
  memoryMb: number;
  pid?: number;
  createdAt: number;
  cpuMillis?: number;
  rssMb?: number;
  diskUsedMb?: number;
  egressBytes?: number;
  metadata?: Record<string, string>;
}
