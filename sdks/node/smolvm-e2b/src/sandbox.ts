import { Client, type ConnectionOpts } from "./client.js";
import { Commands } from "./commands.js";
import { Files } from "./files.js";
import { Pty } from "./pty.js";
import type {
  ConnectOpts,
  ListOpts,
  MachineInfoJson,
  ResumeOpts,
  SandboxInfo,
  SandboxMetrics,
  SandboxOpts,
} from "./types.js";

/** e2b `/sandboxes` list item / detail object (the subset we consume). */
interface SandboxJson {
  sandboxID: string;
  templateID?: string;
  startedAt?: string;
  cpuCount?: number;
  memoryMB?: number;
  state?: string;
  metadata?: Record<string, string>;
}

/** e2b create / resume response (the subset we consume). */
interface SandboxCreateJson {
  sandboxID: string;
}

function toInfo(s: SandboxJson): SandboxInfo {
  return {
    sandboxId: s.sandboxID,
    state: s.state ?? "paused",
    cpus: s.cpuCount ?? 0,
    memoryMb: s.memoryMB ?? 0,
    createdAt: s.startedAt ? Math.floor(Date.parse(s.startedAt) / 1000) : 0,
    metadata: s.metadata ?? {},
  };
}

// Control plane speaks the e2b `/sandboxes` REST shape; the data plane (exec,
// files, pty) keeps bridging over smolvm's `/api/v1/machines/{id}/...`.
const sandboxesBase = "/sandboxes";
const machinesBase = "/api/v1/machines";
const id = (s: string) => encodeURIComponent(s);

/**
 * A self-hosted, e2b-style sandbox backed by a smolvm microVM.
 *
 * @example
 * ```ts
 * const sbx = await Sandbox.create({ template: "python:3.12", timeoutMs: 300_000 });
 * const r = await sbx.commands.run("python -c 'print(2+2)'");
 * console.log(r.stdout); // "4"
 * const paused = await sbx.pause();
 * const again = await Sandbox.resume(paused);
 * await again.kill();
 * ```
 */
export class Sandbox {
  /** The sandbox's stable id (its smolvm machine name). */
  readonly sandboxId: string;
  /** Run commands inside the sandbox. */
  readonly commands: Commands;
  /** Read/write files inside the sandbox. */
  readonly files: Files;
  /** Open interactive terminal (PTY) sessions inside the sandbox. */
  readonly pty: Pty;

  private client: Client;
  private previewDomain: string;
  private previewPort?: number;

  private constructor(
    sandboxId: string,
    client: Client,
    previewDomain?: string,
    previewPort?: number,
  ) {
    this.sandboxId = sandboxId;
    this.client = client;
    this.previewDomain =
      previewDomain ?? process.env.SMOLVM_PREVIEW_DOMAIN ?? "localhost";
    this.previewPort =
      previewPort ??
      (process.env.SMOLVM_PREVIEW_PORT ? Number(process.env.SMOLVM_PREVIEW_PORT) : undefined);
    this.commands = new Commands(client, sandboxId);
    this.files = new Files(client, this.commands, sandboxId);
    this.pty = new Pty(client, sandboxId);
  }

  /**
   * Hostname (authority) to reach a service running inside the sandbox on `port`,
   * through the preview proxy — `<port>-<sandboxId>.<previewDomain>`, with
   * `:previewPort` appended when the proxy is on a non-standard port. Prefix with
   * `http://` or `https://` to form a URL. The port must have been declared in
   * `Sandbox.create({ ports })`. Mirrors e2b's `getHost`.
   */
  getHost(port: number): string {
    const host = `${port}-${this.sandboxId}.${this.previewDomain}`;
    const p = this.previewPort;
    return p && p !== 80 && p !== 443 ? `${host}:${p}` : host;
  }

  /** Create and start a new sandbox. */
  static async create(opts: SandboxOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    // The server resolves `templateID` (built-template alias → its artifact, else
    // an OCI image, else "alpine") and auto-starts — no client-side resolve/start.
    const body: Record<string, unknown> = {
      templateID: opts.template,
      timeout: opts.timeoutMs ? Math.ceil(opts.timeoutMs / 1000) : undefined,
      metadata: opts.metadata,
      envVars: opts.envs,
      allow_internet_access: opts.network,
      // e2b's autoPause is a bool; only "kill" tears the sandbox down on idle,
      // "pause"/"stop"/unset keep it resumable.
      autoPause: opts.onTimeout ? opts.onTimeout !== "kill" : undefined,
      // smolvm extensions (ignored by a stock e2b server, honored by smolvm):
      name: opts.sandboxId,
      cpus: opts.cpus,
      memoryMb: opts.memoryMb,
      ports: opts.ports,
      cmd: opts.cmd,
      workdir: opts.workdir,
    };
    const created = await client.requestJson<SandboxCreateJson>("POST", sandboxesBase, {
      json: body,
      timeoutMs: 300_000,
    });
    return new Sandbox(created.sandboxID, client, opts.previewDomain, opts.previewPort);
  }

  /** Reconnect to an already-running sandbox by id (no state change). */
  static async connect(sandboxId: string, opts: ConnectOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    // Verify it exists (throws NotFoundError otherwise).
    await client.requestJson<SandboxJson>("GET", `${sandboxesBase}/${id(sandboxId)}`);
    return new Sandbox(sandboxId, client, opts.previewDomain, opts.previewPort);
  }

  /** Resume a paused sandbox, restoring its running processes. */
  static async resume(sandboxId: string, opts: ResumeOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    const body = opts.timeoutMs
      ? { timeout: Math.ceil(opts.timeoutMs / 1000) }
      : undefined;
    await client.requestJson<SandboxCreateJson>(
      "POST",
      `${sandboxesBase}/${id(sandboxId)}/resume`,
      { json: body, timeoutMs: 300_000 },
    );
    return new Sandbox(sandboxId, client, opts.previewDomain, opts.previewPort);
  }

  /** List sandboxes known to the control plane, optionally filtered by metadata. */
  static async list(opts: ListOpts = {}): Promise<SandboxInfo[]> {
    const client = new Client(opts);
    let path = "/v2/sandboxes";
    if (opts.metadata && Object.keys(opts.metadata).length > 0) {
      // The server filter is a `key=value&key2=value2` string in the `metadata` param.
      const filter = Object.entries(opts.metadata)
        .map(([k, v]) => `${k}=${v}`)
        .join("&");
      path += `?metadata=${encodeURIComponent(filter)}`;
    }
    const items = await client.requestJson<SandboxJson[]>("GET", path);
    return (items ?? []).map(toInfo);
  }

  /** Delete a sandbox by id without needing an instance. */
  static async kill(sandboxId: string, opts: ConnectionOpts = {}): Promise<void> {
    const client = new Client(opts);
    await client.request("DELETE", `${sandboxesBase}/${id(sandboxId)}`);
  }

  /**
   * Pause the sandbox: checkpoint its full RAM + running processes to disk and
   * free its compute. Returns the sandbox id — resume later with
   * {@link Sandbox.resume}. The running processes continue on resume.
   */
  async pause(): Promise<string> {
    await this.client.request("POST", `${sandboxesBase}/${id(this.sandboxId)}/pause`, {
      timeoutMs: 300_000,
    });
    return this.sandboxId;
  }

  /**
   * Set/extend the auto-idle window (e2b `setTimeout`). The sandbox auto-pauses
   * `timeoutMs` from now unless refreshed again. `0` disables auto-idle.
   */
  async setTimeout(timeoutMs: number): Promise<void> {
    await this.client.request("POST", `${sandboxesBase}/${id(this.sandboxId)}/timeout`, {
      json: { timeout: Math.ceil(timeoutMs / 1000) },
    });
  }

  /** Current status of the sandbox. */
  async getInfo(): Promise<SandboxInfo> {
    const s = await this.client.requestJson<SandboxJson>(
      "GET",
      `${sandboxesBase}/${id(this.sandboxId)}`,
    );
    return toInfo(s);
  }

  /** Whether the sandbox is currently running. */
  async isRunning(): Promise<boolean> {
    return (await this.getInfo()).state === "running";
  }

  /**
   * Current resource usage of the sandbox (a point-in-time sample). Empty fields
   * mean the value isn't available (e.g. a paused/stopped sandbox has no live
   * process to sample). Reads smolvm's richer machine info — the e2b surface
   * carries no live metrics.
   */
  async getMetrics(): Promise<SandboxMetrics> {
    const m = await this.client.requestJson<MachineInfoJson>(
      "GET",
      `${machinesBase}/${id(this.sandboxId)}`,
    );
    return {
      cpuMillis: m.cpuMillis,
      memMb: m.rssMb,
      diskMb: m.diskUsedMb,
      egressBytes: m.egressBytes,
    };
  }

  /** Permanently delete the sandbox and its data. */
  async kill(): Promise<void> {
    await this.client.request("DELETE", `${sandboxesBase}/${id(this.sandboxId)}`);
  }
}
