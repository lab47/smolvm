import type { Client } from "./client.js";

/** Options for {@link Pty.create}. */
export interface PtyOpts {
  /** Command line to run, executed via `sh -c` (so args and shell syntax work,
   * e.g. `bash -lc '...'`). Defaults to an interactive `/bin/sh`. */
  cmd?: string;
  /** Terminal width in columns (default 80). */
  cols?: number;
  /** Terminal height in rows (default 24). */
  rows?: number;
  /** Called with each chunk of terminal output (stdout+stderr, merged). */
  onData?: (data: Buffer) => void;
}

/** A live interactive terminal session. */
export interface PtyHandle {
  /** Send bytes to the terminal's stdin. */
  sendStdin(data: string | Uint8Array): void;
  /** Resize the terminal. */
  resize(cols: number, rows: number): void;
  /** Close the session (sends EOF and disconnects). */
  kill(): void;
  /** Resolves with the process exit code when the session ends. */
  readonly exited: Promise<number>;
}

/** Opens interactive PTY sessions inside a sandbox (the e2b `sandbox.pty` surface). */
export class Pty {
  constructor(
    private client: Client,
    private sandboxId: string,
  ) {}

  /** Start an interactive terminal. Returns a handle to drive it. */
  async create(opts: PtyOpts = {}): Promise<PtyHandle> {
    const q = new URLSearchParams();
    if (opts.cmd) q.set("cmd", opts.cmd);
    q.set("cols", String(opts.cols ?? 80));
    q.set("rows", String(opts.rows ?? 24));
    const path = `/api/v1/machines/${encodeURIComponent(this.sandboxId)}/exec/interactive?${q}`;

    const ws = await this.client.openWebSocket(path);
    let resolveExit!: (code: number) => void;
    const exited = new Promise<number>((r) => {
      resolveExit = r;
    });
    ws.on({
      onBinary: (d) => opts.onData?.(d),
      onText: (t) => {
        try {
          const m = JSON.parse(t) as { type?: string; code?: number };
          if (m.type === "exit") resolveExit(m.code ?? 0);
        } catch {
          /* non-JSON text is not expected on this stream; ignore */
        }
      },
      onClose: () => resolveExit(130),
    });

    return {
      sendStdin: (data) =>
        ws.sendBinary(typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data)),
      resize: (cols, rows) => ws.sendText(JSON.stringify({ type: "resize", cols, rows })),
      kill: () => ws.close(),
      exited,
    };
  }
}
