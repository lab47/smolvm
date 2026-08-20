import type { Client } from "./client.js";
import { CommandExitError } from "./errors.js";
import type { BackgroundCommandHandle, CommandOpts, CommandResult } from "./types.js";

interface ExecResponseJson {
  exitCode: number;
  stdout: string;
  stderr: string;
  pid?: number;
}

function envList(envs?: Record<string, string>): Array<{ name: string; value: string }> {
  if (!envs) return [];
  return Object.entries(envs).map(([name, value]) => ({ name, value }));
}

/** Runs commands inside a sandbox (the e2b `sandbox.commands` surface). */
export class Commands {
  constructor(
    private client: Client,
    private sandboxId: string,
  ) {}

  /**
   * Run a command inside the sandbox and wait for it to finish.
   *
   * @param cmd A shell string (run via `sh -c`) or an argv array (run directly).
   */
  async run(cmd: string | string[], opts?: CommandOpts & { background?: false }): Promise<CommandResult>;
  async run(cmd: string | string[], opts: CommandOpts & { background: true }): Promise<BackgroundCommandHandle>;
  async run(
    cmd: string | string[],
    opts: CommandOpts = {},
  ): Promise<CommandResult | BackgroundCommandHandle> {
    const command = Array.isArray(cmd) ? cmd : ["sh", "-c", cmd];
    const body = {
      command,
      env: envList(opts.envs),
      workdir: opts.cwd,
      timeoutSecs: opts.timeoutMs !== undefined ? Math.ceil(opts.timeoutMs / 1000) : undefined,
      stdin: opts.stdin,
      background: opts.background ?? false,
    };
    const res = await this.client.requestJson<ExecResponseJson>(
      "POST",
      `/api/v1/machines/${encodeURIComponent(this.sandboxId)}/exec`,
      { json: body, timeoutMs: opts.timeoutMs ? opts.timeoutMs + 5_000 : undefined },
    );

    if (opts.background) {
      // The control plane returns the detached PID either as a `pid` field or
      // embedded in stdout as "pid=<N>".
      let pid = res.pid;
      if (pid === undefined) {
        const m = /pid=(\d+)/.exec(res.stdout ?? "");
        if (m) pid = Number(m[1]);
      }
      return { pid };
    }
    const result: CommandResult = {
      exitCode: res.exitCode,
      stdout: res.stdout ?? "",
      stderr: res.stderr ?? "",
    };
    if (result.exitCode !== 0 && (opts.throwOnError ?? true)) {
      throw new CommandExitError(result.exitCode, result.stdout, result.stderr);
    }
    return result;
  }
}
