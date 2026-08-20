import { randomBytes } from "node:crypto";

import type { Commands } from "./commands.js";
import { NotFoundError } from "./errors.js";
import type { BackgroundStreamOpts, CommandHandle, CommandResult } from "./types.js";

/** Where the in-guest supervisor keeps per-process state (tmpfs). */
const ROOT = "/tmp/.smolproc";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const shq = (s: string) => `'${s.replace(/'/g, `'\\''`)}'`;

/**
 * In-guest launcher. Given `$SMOLPROC_DIR` and `$SMOLPROC_CMD`, it runs the
 * command under a detached (setsid) monitor that: redirects stdin from a FIFO
 * (held open by a keepalive writer so `sendStdin` works), captures stdout/stderr
 * to files, records the exit code, then tears the keepalive down. It prints the
 * process PID and returns immediately.
 */
const LAUNCHER = `
d="$SMOLPROC_DIR"; mkdir -p "$d"; : > "$d/out"; : > "$d/err"; rm -f "$d/code"
mkfifo "$d/in" 2>/dev/null || true
setsid sh -c 'exec sleep 2147483647 > "$SMOLPROC_DIR/in"' >/dev/null 2>&1 &
echo $! > "$d/inkeep"
setsid sh -c 'sh -c "$SMOLPROC_CMD" < "$SMOLPROC_DIR/in" > "$SMOLPROC_DIR/out" 2> "$SMOLPROC_DIR/err" & pp=$!; echo $pp > "$SMOLPROC_DIR/pid"; wait $pp; echo $? > "$SMOLPROC_DIR/code"; kill $(cat "$SMOLPROC_DIR/inkeep") 2>/dev/null' >/dev/null 2>&1 &
i=0; while [ ! -s "$d/pid" ] && [ $i -lt 400 ]; do i=$((i+1)); done
cat "$d/pid"
`;

/** Start a managed background process and return a handle. */
export async function startBackground(
  commands: Commands,
  cmd: string,
  opts: { envs?: Record<string, string>; cwd?: string } & BackgroundStreamOpts,
): Promise<CommandHandle> {
  const dir = `${ROOT}/${randomBytes(8).toString("hex")}`;
  const res = await commands.run(["sh", "-c", LAUNCHER], {
    envs: { ...opts.envs, SMOLPROC_DIR: dir, SMOLPROC_CMD: cmd },
    cwd: opts.cwd,
    throwOnError: true,
  });
  const pid = Number(res.stdout.trim());
  if (!Number.isFinite(pid) || pid <= 0) {
    throw new Error(`failed to start background process: ${res.stdout || res.stderr}`);
  }
  const h = new BackgroundProcess(commands, dir, pid);
  h.beginStreaming(opts);
  return h;
}

/** Reattach to a managed background process by pid. */
export async function connectBackground(
  commands: Commands,
  pid: number,
  opts: BackgroundStreamOpts = {},
): Promise<CommandHandle> {
  const res = await commands.run(
    ["sh", "-c", `grep -l "^${pid}$" ${ROOT}/*/pid 2>/dev/null | head -1`],
    { throwOnError: false },
  );
  const pidfile = res.stdout.trim();
  if (!pidfile) throw new NotFoundError(`no managed background process with pid ${pid}`);
  const dir = pidfile.replace(/\/pid$/, "");
  // Resume the stream from the current end of output.
  const sizes = await commands.run(
    ["sh", "-c", `wc -c < ${shq(dir)}/out 2>/dev/null; wc -c < ${shq(dir)}/err 2>/dev/null`],
    { throwOnError: false },
  );
  const [o, e] = sizes.stdout.trim().split(/\s+/).map(Number);
  const h = new BackgroundProcess(commands, dir, pid, o || 0, e || 0);
  h.beginStreaming(opts);
  return h;
}

class BackgroundProcess implements CommandHandle {
  private active = false;
  private pollMs = 300;
  private onStdout?: (d: string) => void;
  private onStderr?: (d: string) => void;
  private waiters: Array<(r: CommandResult) => void> = [];
  private finished?: CommandResult;

  constructor(
    private commands: Commands,
    private dir: string,
    readonly pid: number,
    private outOff = 0,
    private errOff = 0,
  ) {}

  beginStreaming(opts: BackgroundStreamOpts): void {
    this.onStdout = opts.onStdout;
    this.onStderr = opts.onStderr;
    if (opts.pollMs) this.pollMs = opts.pollMs;
    if (this.active) return;
    this.active = true;
    void this.loop();
  }

  private async loop(): Promise<void> {
    while (this.active) {
      const done = await this.drainOnce();
      if (done) {
        await this.drainOnce(); // final flush of any tail bytes
        await this.finish();
        return;
      }
      await sleep(this.pollMs);
    }
  }

  /** One poll: fetch new stdout/stderr bytes and check for exit. Returns true if exited. */
  private async drainOnce(): Promise<boolean> {
    const d = shq(this.dir);
    const status = await this.commands.run(
      ["sh", "-c", `printf '%s %s %s' "$(wc -c < ${d}/out 2>/dev/null || echo 0)" "$(wc -c < ${d}/err 2>/dev/null || echo 0)" "$(cat ${d}/code 2>/dev/null)"`],
      { throwOnError: false },
    );
    const [outSizeStr, errSizeStr, codeStr] = status.stdout.trim().split(/\s+/);
    const outSize = Number(outSizeStr) || 0;
    const errSize = Number(errSizeStr) || 0;
    if (outSize > this.outOff && this.onStdout) {
      const chunk = await this.readRange("out", this.outOff, outSize - this.outOff);
      if (chunk) this.onStdout(chunk);
    }
    this.outOff = Math.max(this.outOff, outSize);
    if (errSize > this.errOff && this.onStderr) {
      const chunk = await this.readRange("err", this.errOff, errSize - this.errOff);
      if (chunk) this.onStderr(chunk);
    }
    this.errOff = Math.max(this.errOff, errSize);
    return codeStr !== undefined && codeStr !== "";
  }

  private async readRange(file: string, off: number, n: number): Promise<string> {
    const d = shq(this.dir);
    const res = await this.commands.run(
      ["sh", "-c", `tail -c +${off + 1} ${d}/${file} 2>/dev/null | head -c ${n}`],
      { throwOnError: false },
    );
    return res.stdout;
  }

  private async finish(): Promise<void> {
    this.active = false;
    const d = shq(this.dir);
    const res = await this.commands.run(
      ["sh", "-c", `cat ${d}/code 2>/dev/null; echo; echo '---SMOLOUT---'; cat ${d}/out 2>/dev/null; echo '---SMOLERR---'; cat ${d}/err 2>/dev/null`],
      { throwOnError: false },
    );
    const out = res.stdout;
    const code = Number(out.split("\n", 1)[0].trim()) || 0;
    const oStart = out.indexOf("---SMOLOUT---\n");
    const eStart = out.indexOf("---SMOLERR---\n");
    const stdout = oStart >= 0 && eStart >= 0 ? out.slice(oStart + 14, eStart) : "";
    const stderr = eStart >= 0 ? out.slice(eStart + 14) : "";
    this.finished = { exitCode: code, stdout, stderr };
    // Best-effort cleanup of the per-process dir.
    await this.commands.run(["sh", "-c", `rm -rf ${d}`], { throwOnError: false }).catch(() => {});
    for (const w of this.waiters.splice(0)) w(this.finished);
  }

  wait(): Promise<CommandResult> {
    if (this.finished) return Promise.resolve(this.finished);
    // Ensure the poll loop is running so it can complete.
    if (!this.active) {
      this.active = true;
      void this.loop();
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  async kill(signal = "TERM"): Promise<boolean> {
    return this.commands.kill(this.pid, signal);
  }

  async sendStdin(data: string | Uint8Array): Promise<void> {
    const text = typeof data === "string" ? data : Buffer.from(data).toString("binary");
    await this.commands.run(["sh", "-c", `cat > ${shq(this.dir)}/in`], {
      stdin: text,
      throwOnError: false,
    });
  }

  disconnect(): void {
    this.active = false;
  }
}
