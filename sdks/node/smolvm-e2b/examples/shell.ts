/**
 * Start a sandbox and drop into an interactive shell inside it over a PTY.
 *
 *   SMOLVM_API_KEY=<key> npm run example:shell
 *   SMOLVM_API_KEY=<key> npm run example:shell -- --template alpine --keep
 *
 * Env:
 *   SMOLVM_API_URL   control-plane URL (default https://api.sbx.eu0.lab47.dev)
 *   SMOLVM_API_KEY   required — control-plane X-API-Key
 *
 * Flags:
 *   --template <id>  template/image to boot (default "default")
 *   --cmd <line>     shell command line to run (default "bash -l", falls back
 *                    to "/bin/sh" on images without bash)
 *   --keep           leave the sandbox running on exit instead of killing it
 *
 * Controls: the terminal is raw, so Ctrl-C, arrow keys, tab, etc. all go to the
 * remote shell. Type `exit` (or Ctrl-D) to end the session. Ctrl-] force-detaches
 * if the remote shell ever hangs.
 */
import { Sandbox } from "../src/index.js";

function flag(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
}
const has = (name: string) => process.argv.includes(`--${name}`);

const apiKey = process.env.SMOLVM_API_KEY;
if (!apiKey) {
  console.error("Set SMOLVM_API_KEY to your cluster's control-plane X-API-Key.");
  process.exit(1);
}
const opts = { apiUrl: process.env.SMOLVM_API_URL ?? "https://api.sbx.eu0.lab47.dev", apiKey };
const template = flag("template") ?? "default";
const cmd = flag("cmd") ?? "bash -l";
const keep = has("keep");

const out = process.stdout;
const err = process.stderr;

async function main() {
  err.write(`connecting to ${opts.apiUrl} …\n`);
  const sbx = await Sandbox.create({
    ...opts,
    template,
    network: true,
    timeoutMs: 0, // never auto-idle; this script owns the lifecycle
  });
  err.write(`sandbox ${sbx.sandboxId} up — starting "${cmd}" (Ctrl-] to force-detach)\n`);

  const cols = out.columns ?? 80;
  const rows = out.rows ?? 24;

  const pty = await sbx.pty.create({
    cmd,
    cols,
    rows,
    onData: (d) => out.write(d),
  });

  const stdin = process.stdin;
  const wasRaw = stdin.isRaw ?? false;
  if (stdin.isTTY) stdin.setRawMode(true);
  stdin.resume();

  const onInput = (chunk: Buffer) => {
    // Ctrl-] (0x1d): local escape hatch to force-detach.
    if (chunk.includes(0x1d)) {
      err.write("\r\n[detached]\r\n");
      cleanup(130);
      return;
    }
    pty.sendStdin(chunk);
  };
  const onResize = () => pty.resize(out.columns ?? cols, out.rows ?? rows);

  stdin.on("data", onInput);
  out.on("resize", onResize);

  let cleaned = false;
  async function cleanup(code: number) {
    if (cleaned) return;
    cleaned = true;
    stdin.off("data", onInput);
    out.off("resize", onResize);
    if (stdin.isTTY) stdin.setRawMode(wasRaw);
    stdin.pause();
    try {
      pty.kill();
    } catch {
      /* already closed */
    }
    if (keep) {
      err.write(`\nsandbox ${sbx.sandboxId} left running (--keep). Kill it with the SDK or CLI.\n`);
    } else {
      err.write(`\nkilling sandbox ${sbx.sandboxId} …\n`);
      await Sandbox.kill(sbx.sandboxId, opts).catch(() => {});
    }
    process.exit(code);
  }

  pty.exited.then((code) => cleanup(code));
  process.on("SIGTERM", () => cleanup(143));
}

main().catch((e) => {
  err.write(`\nshell failed: ${e instanceof Error ? e.message : String(e)}\n`);
  process.exit(1);
});
