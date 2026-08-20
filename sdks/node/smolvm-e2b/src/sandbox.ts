import { Client, type ConnectionOpts } from "./client.js";
import { Commands } from "./commands.js";
import { Files } from "./files.js";
import { Pty } from "./pty.js";
import type {
  ConnectOpts,
  MachineInfoJson,
  ResumeOpts,
  SandboxInfo,
  SandboxOpts,
} from "./types.js";

function toInfo(m: MachineInfoJson): SandboxInfo {
  return {
    sandboxId: m.name,
    state: m.state,
    cpus: m.cpus,
    memoryMb: m.memoryMb,
    pid: m.pid,
    createdAt: m.createdAt,
  };
}

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

  private constructor(sandboxId: string, client: Client, previewDomain?: string) {
    this.sandboxId = sandboxId;
    this.client = client;
    this.previewDomain =
      previewDomain ?? process.env.SMOLVM_PREVIEW_DOMAIN ?? "localhost";
    this.commands = new Commands(client, sandboxId);
    this.files = new Files(client, this.commands, sandboxId);
    this.pty = new Pty(client, sandboxId);
  }

  /**
   * Hostname to reach a service running inside the sandbox on `port`, through the
   * preview proxy — `<port>-<sandboxId>.<previewDomain>`. Prefix with `http://`
   * or `https://` to form a URL. The port must have been declared in
   * `Sandbox.create({ ports })`. Mirrors e2b's `getHost`.
   */
  getHost(port: number): string {
    return `${port}-${this.sandboxId}.${this.previewDomain}`;
  }

  /** Create and start a new sandbox. */
  static async create(opts: SandboxOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    const body: Record<string, unknown> = {
      name: opts.sandboxId,
      image: opts.template ?? "alpine",
      network: opts.network ?? true,
      cpus: opts.cpus,
      memoryMb: opts.memoryMb,
      timeoutSecs: opts.timeoutMs ? Math.ceil(opts.timeoutMs / 1000) : undefined,
      cmd: opts.cmd ?? ["sleep", "infinity"],
      workdir: opts.workdir,
      env: opts.envs
        ? Object.entries(opts.envs).map(([name, value]) => ({ name, value }))
        : [],
      // host 0 → the server auto-allocates a free host port; the preview proxy
      // resolves the guest→host mapping from the machine info.
      ports: opts.ports?.map((guest) => ({ host: 0, guest })),
    };
    const created = await client.requestJson<MachineInfoJson>("POST", machinesBase, {
      json: body,
      timeoutMs: 300_000,
    });
    // create() does not auto-start; bring it up (arms the auto-idle deadline).
    await client.requestJson<MachineInfoJson>("POST", `${machinesBase}/${id(created.name)}/start`, {
      timeoutMs: 300_000,
    });
    return new Sandbox(created.name, client, opts.previewDomain);
  }

  /** Reconnect to an already-running sandbox by id (no state change). */
  static async connect(sandboxId: string, opts: ConnectOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    // Verify it exists (throws NotFoundError otherwise).
    await client.requestJson<MachineInfoJson>("GET", `${machinesBase}/${id(sandboxId)}`);
    return new Sandbox(sandboxId, client, opts.previewDomain);
  }

  /** Resume a paused sandbox, restoring its running processes. */
  static async resume(sandboxId: string, opts: ResumeOpts = {}): Promise<Sandbox> {
    const client = new Client(opts);
    await client.requestJson<MachineInfoJson>("POST", `${machinesBase}/${id(sandboxId)}/resume`, {
      timeoutMs: 300_000,
    });
    const sbx = new Sandbox(sandboxId, client, opts.previewDomain);
    if (opts.timeoutMs) await sbx.setTimeout(opts.timeoutMs);
    return sbx;
  }

  /** List all sandboxes known to the control plane. */
  static async list(opts: ConnectionOpts = {}): Promise<SandboxInfo[]> {
    const client = new Client(opts);
    const res = await client.requestJson<{ machines: MachineInfoJson[] }>("GET", machinesBase);
    return (res.machines ?? []).map(toInfo);
  }

  /** Delete a sandbox by id without needing an instance. */
  static async kill(sandboxId: string, opts: ConnectionOpts = {}): Promise<void> {
    const client = new Client(opts);
    await client.request("DELETE", `${machinesBase}/${id(sandboxId)}?force=true`);
  }

  /**
   * Pause the sandbox: checkpoint its full RAM + running processes to disk and
   * free its compute. Returns the sandbox id — resume later with
   * {@link Sandbox.resume}. The running processes continue on resume.
   */
  async pause(): Promise<string> {
    await this.client.requestJson<MachineInfoJson>(
      "POST",
      `${machinesBase}/${id(this.sandboxId)}/pause`,
      { timeoutMs: 300_000 },
    );
    return this.sandboxId;
  }

  /**
   * Set/extend the auto-idle window (e2b `setTimeout`). The sandbox auto-pauses
   * `timeoutMs` from now unless refreshed again. `0` disables auto-idle.
   */
  async setTimeout(timeoutMs: number): Promise<void> {
    await this.client.requestJson<MachineInfoJson>(
      "POST",
      `${machinesBase}/${id(this.sandboxId)}/timeout`,
      { json: { timeoutSecs: Math.ceil(timeoutMs / 1000) } },
    );
  }

  /** Current status of the sandbox. */
  async getInfo(): Promise<SandboxInfo> {
    const m = await this.client.requestJson<MachineInfoJson>(
      "GET",
      `${machinesBase}/${id(this.sandboxId)}`,
    );
    return toInfo(m);
  }

  /** Whether the sandbox is currently running. */
  async isRunning(): Promise<boolean> {
    return (await this.getInfo()).state === "running";
  }

  /**
   * Current resource usage of the sandbox (a point-in-time sample). Empty fields
   * mean the value isn't available (e.g. a paused/stopped sandbox has no live
   * process to sample).
   */
  async getMetrics(): Promise<import("./types.js").SandboxMetrics> {
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
    await this.client.request("DELETE", `${machinesBase}/${id(this.sandboxId)}?force=true`);
  }
}
