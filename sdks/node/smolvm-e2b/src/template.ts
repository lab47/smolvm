import { Client, type ConnectionOpts } from "./client.js";
import { NotFoundError, SmolvmError } from "./errors.js";
import { Sandbox } from "./sandbox.js";

/** A built, locally-stored template artifact. */
export interface TemplateInfo {
  alias: string;
  /** Server path to the `.smolmachine` sidecar. */
  path: string;
  sizeBytes: number;
  createdAt: number;
}

/** Options for {@link Template.build}. */
export interface TemplateBuildOpts extends ConnectionOpts {
  /** Alias to store the template under (letters, digits, `-`, `_`, `.`). */
  alias: string;
  /** Base OCI image to provision from, e.g. `"python:3.12"`. */
  base: string;
  /** Setup commands run once during the build (shell string or argv). */
  setup?: (string | string[])[];
  /** vCPUs / memory for the build sandbox. */
  cpus?: number;
  memoryMb?: number;
  /** Environment for the setup commands. */
  envs?: Record<string, string>;
  /** Called with build output/log lines. */
  onLog?: (line: string) => void;
}

interface TemplateJson {
  alias: string;
  path: string;
  sizeBytes: number;
  createdAt: number;
}

const enc = encodeURIComponent;
const base = "/api/v1/templates";

/**
 * Build and manage reusable sandbox templates. A template is a pre-provisioned
 * image (base + setup steps) snapshotted once; `Sandbox.create({ template })`
 * then boots from it fast, with the setup already applied.
 */
export class Template {
  /** Build a template: provision a sandbox from `base`, run `setup`, snapshot it. */
  static async build(opts: TemplateBuildOpts): Promise<TemplateInfo> {
    const client = new Client(opts);
    const log = opts.onLog ?? (() => {});
    log(`creating build sandbox from ${opts.base}`);
    const sbx = await Sandbox.create({
      template: opts.base,
      apiUrl: opts.apiUrl,
      apiKey: opts.apiKey,
      tls: opts.tls,
      cpus: opts.cpus,
      memoryMb: opts.memoryMb,
      envs: opts.envs,
      network: true,
    });
    try {
      for (const cmd of opts.setup ?? []) {
        log(`+ ${Array.isArray(cmd) ? cmd.join(" ") : cmd}`);
        await sbx.commands.run(cmd, { envs: opts.envs, onStdout: log, onStderr: log });
      }
      // pack requires a stopped machine.
      log("stopping build sandbox");
      await client.request("POST", `/api/v1/machines/${enc(sbx.sandboxId)}/stop`, {
        timeoutMs: 120_000,
      });
      log(`packing template '${opts.alias}'`);
      const info = await client.requestJson<TemplateJson>(
        "POST",
        `/api/v1/machines/${enc(sbx.sandboxId)}/pack`,
        { json: { alias: opts.alias }, timeoutMs: 600_000 },
      );
      log(`built '${info.alias}' (${info.sizeBytes} bytes)`);
      return info;
    } finally {
      // The template artifact is standalone; tear down the build sandbox.
      await Sandbox.kill(sbx.sandboxId, opts).catch(() => {});
    }
  }

  /** List built templates. */
  static async list(opts: ConnectionOpts = {}): Promise<TemplateInfo[]> {
    const client = new Client(opts);
    const res = await client.requestJson<{ templates: TemplateJson[] }>("GET", base);
    return res.templates ?? [];
  }

  /** Resolve a template alias, or `null` if there's no such template. */
  static async get(alias: string, opts: ConnectionOpts = {}): Promise<TemplateInfo | null> {
    const client = new Client(opts);
    try {
      return await client.requestJson<TemplateJson>("GET", `${base}/${enc(alias)}`);
    } catch (e) {
      // Not a template (missing) or not a valid alias (e.g. an OCI ref like
      // "python:3.12") — the caller treats those as "use as an image".
      if (e instanceof NotFoundError) return null;
      if (e instanceof SmolvmError && / 400/.test(e.message)) return null;
      throw e;
    }
  }

  /** Delete a built template. */
  static async remove(alias: string, opts: ConnectionOpts = {}): Promise<void> {
    const client = new Client(opts);
    await client.request("DELETE", `${base}/${enc(alias)}`);
  }
}
