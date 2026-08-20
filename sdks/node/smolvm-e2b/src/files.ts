import type { Client } from "./client.js";
import type { Commands } from "./commands.js";
import type { FileEntry, FileEvent, FileInfo, FileReadOpts, FileWatcher } from "./types.js";

/** Reads and writes files inside a sandbox (the e2b `sandbox.files` surface).
 *
 * `write`/`read` use the control plane's binary file endpoints. `list`, `remove`
 * and `rename` are implemented over `commands` (there are no dedicated endpoints
 * for them), so they need a sandbox whose workload is running. */
export class Files {
  constructor(
    private client: Client,
    private commands: Commands,
    private sandboxId: string,
  ) {}

  private filePath(path: string): string {
    // The route is /{id}/files/{*path}; the guest path is absolute, and each
    // segment is encoded but the separators are preserved.
    const clean = path.replace(/^\/+/, "");
    const encoded = clean.split("/").map(encodeURIComponent).join("/");
    return `/api/v1/machines/${encodeURIComponent(this.sandboxId)}/files/${encoded}`;
  }

  /** Write data to `path` inside the sandbox (creating parent dirs as needed). */
  async write(path: string, data: string | Uint8Array): Promise<void> {
    await this.client.request("PUT", this.filePath(path), {
      body: typeof data === "string" ? Buffer.from(data, "utf8") : data,
    });
  }

  /** Read `path` as UTF-8 text (default) or raw bytes. */
  async read(path: string, opts?: { format?: "text" }): Promise<string>;
  async read(path: string, opts: { format: "bytes" }): Promise<Buffer>;
  async read(path: string, opts: FileReadOpts = {}): Promise<string | Buffer> {
    const buf = await this.client.request("GET", this.filePath(path), { raw: true });
    return opts.format === "bytes" ? buf : buf.toString("utf8");
  }

  /** List the entries in a directory inside the sandbox. */
  async list(path: string): Promise<FileEntry[]> {
    // -p appends "/" to directories; parse names + type from that.
    const res = await this.commands.run(["sh", "-c", `ls -1Ap ${shq(path)}`], {
      throwOnError: true,
    });
    return res.stdout
      .split("\n")
      .filter((l) => l.length > 0)
      .map((entry) => {
        const isDir = entry.endsWith("/");
        const name = isDir ? entry.slice(0, -1) : entry;
        return {
          name,
          path: `${path.replace(/\/+$/, "")}/${name}`,
          type: isDir ? "dir" : "file",
        } as FileEntry;
      });
  }

  /** Remove a file or directory (recursively) inside the sandbox. */
  async remove(path: string): Promise<void> {
    await this.commands.run(["rm", "-rf", path], { throwOnError: true });
  }

  /** Rename/move a path inside the sandbox. */
  async rename(from: string, to: string): Promise<void> {
    await this.commands.run(["mv", from, to], { throwOnError: true });
  }

  /** Whether a path exists inside the sandbox. */
  async exists(path: string): Promise<boolean> {
    const res = await this.commands.run(["sh", "-c", `test -e ${shq(path)}`], {
      throwOnError: false,
    });
    return res.exitCode === 0;
  }

  /** Create a directory (and any missing parents) inside the sandbox. */
  async makeDir(path: string): Promise<void> {
    await this.commands.run(["mkdir", "-p", path], { throwOnError: true });
  }

  /**
   * Watch a directory inside the sandbox for `create`/`modify`/`remove`.
   *
   * `mode` (default `"auto"`) chooses the mechanism:
   * - `"inotify"` — real-time, pushed over the streaming exec channel (needs
   *   `inotifywait`/`inotify-tools` in the sandbox; pass `{ install: true }` to
   *   `apk`/`apt` it in on demand).
   * - `"poll"` — snapshot-diff every `intervalMs` (default 1s); no dependencies.
   * - `"auto"` — inotify if `inotifywait` is present, otherwise poll.
   *
   * Call `.stop()` on the returned watcher to end it.
   */
  async watchDir(
    path: string,
    onEvent: (event: FileEvent) => void,
    opts: {
      intervalMs?: number;
      recursive?: boolean;
      mode?: "auto" | "inotify" | "poll";
      install?: boolean;
    } = {},
  ): Promise<FileWatcher> {
    const mode = opts.mode ?? "auto";
    if (mode !== "poll") {
      let hasInotify = await this.hasInotifywait();
      if (!hasInotify && opts.install) {
        await this.commands.run(
          [
            "sh",
            "-c",
            "apk add --no-cache inotify-tools >/dev/null 2>&1 || " +
              "{ apt-get update >/dev/null 2>&1 && apt-get install -y inotify-tools >/dev/null 2>&1; } || true",
          ],
          { throwOnError: false },
        );
        hasInotify = await this.hasInotifywait();
      }
      if (hasInotify) return this.watchDirInotify(path, onEvent, opts);
      if (mode === "inotify") {
        throw new Error(
          "inotifywait not found in the sandbox — install inotify-tools (or pass { install: true }), or use mode: 'poll'",
        );
      }
    }
    return this.watchDirPolling(path, onEvent, opts);
  }

  private async hasInotifywait(): Promise<boolean> {
    const r = await this.commands.run(["sh", "-c", "command -v inotifywait"], {
      throwOnError: false,
    });
    return r.exitCode === 0;
  }

  /** Real-time watch: stream `inotifywait -m` output over the SSE exec channel. */
  private async watchDirInotify(
    path: string,
    onEvent: (event: FileEvent) => void,
    opts: { recursive?: boolean },
  ): Promise<FileWatcher> {
    const controller = new AbortController();
    const recursive = opts.recursive === false ? [] : ["-r"];
    // close_write (not modify) so one write yields one "modify" event.
    const cmd = [
      "inotifywait",
      "-m",
      ...recursive,
      "-e",
      "create,close_write,delete,move",
      "--format",
      "%e|%w|%f",
      path,
    ];
    let ready: () => void;
    const readyP = new Promise<void>((r) => (ready = r));
    let buf = "";
    void this.client
      .stream(
        "POST",
        `/api/v1/machines/${encodeURIComponent(this.sandboxId)}/exec/stream`,
        { json: { command: cmd }, signal: controller.signal, timeoutMs: 0 },
        (event, data) => {
          if (event === "stderr") {
            if (data.includes("Watches established")) ready();
            return;
          }
          if (event !== "stdout") return;
          buf += data;
          let nl: number;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl);
            buf = buf.slice(nl + 1);
            const ev = parseInotifyLine(line);
            if (ev) onEvent(ev);
          }
        },
      )
      .catch(() => {})
      .finally(() => ready());
    // Don't report ready until inotify has armed its watches (else early events
    // are missed); cap the wait so a quiet/odd guest still returns.
    await Promise.race([readyP, new Promise((r) => setTimeout(r, 3000))]);
    return { stop: () => controller.abort() };
  }

  private async watchDirPolling(
    path: string,
    onEvent: (event: FileEvent) => void,
    opts: { intervalMs?: number; recursive?: boolean },
  ): Promise<FileWatcher> {
    const interval = opts.intervalMs ?? 1000;
    const snapshot = async (): Promise<Map<string, string>> => {
      const depth = opts.recursive === false ? "-maxdepth 1" : "";
      const res = await this.commands.run(
        ["sh", "-c", `find ${shq(path)} ${depth} -exec stat -c '%n|%Y|%s' {} + 2>/dev/null`],
        { throwOnError: false },
      );
      const map = new Map<string, string>();
      for (const line of res.stdout.split("\n")) {
        const i = line.indexOf("|");
        if (i > 0) map.set(line.slice(0, i), line.slice(i + 1)); // path -> "mtime|size"
      }
      return map;
    };

    let prev = await snapshot();
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const base = (p: string) => p.replace(/\/+$/, "").split("/").pop() ?? p;
    const tick = async () => {
      if (stopped) return;
      try {
        const cur = await snapshot();
        for (const [p, sig] of cur) {
          const old = prev.get(p);
          if (old === undefined) onEvent({ type: "create", path: p, name: base(p) });
          else if (old !== sig) onEvent({ type: "modify", path: p, name: base(p) });
        }
        for (const p of prev.keys()) {
          if (!cur.has(p)) onEvent({ type: "remove", path: p, name: base(p) });
        }
        prev = cur;
      } catch {
        /* transient exec error; try again next tick */
      }
      if (!stopped) timer = setTimeout(tick, interval);
    };
    timer = setTimeout(tick, interval);
    return {
      stop() {
        stopped = true;
        clearTimeout(timer);
      },
    };
  }

  /** Stat a path inside the sandbox: size, type, octal mode, and mtime. */
  async getInfo(path: string): Promise<FileInfo> {
    const res = await this.commands.run(
      ["sh", "-c", `stat -c '%s|%F|%a|%Y' ${shq(path)}`],
      { throwOnError: true },
    );
    const [size, kind, mode, mtime] = res.stdout.trim().split("|");
    const type: "file" | "dir" = kind.includes("directory") ? "dir" : "file";
    const name = path.replace(/\/+$/, "").split("/").pop() ?? path;
    return {
      name,
      path,
      type,
      size: Number(size),
      mode,
      modifiedAt: new Date(Number(mtime) * 1000),
    };
  }
}

/** Single-quote a string for safe use in a `sh -c` command. */
function shq(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/** Parse one `inotifywait --format '%e|%w|%f'` line into a FileEvent, or null
 * for status lines / unmapped events. */
function parseInotifyLine(line: string): FileEvent | null {
  const parts = line.split("|");
  if (parts.length < 2) return null; // "Setting up watches...", "Watches established."
  const events = parts[0].split(",");
  const dir = parts[1];
  const file = parts[2] ?? "";
  let type: FileEvent["type"];
  if (events.includes("CREATE") || events.includes("MOVED_TO")) type = "create";
  else if (events.includes("DELETE") || events.includes("MOVED_FROM")) type = "remove";
  else if (events.includes("CLOSE_WRITE") || events.includes("MODIFY")) type = "modify";
  else return null;
  const cleanDir = dir.replace(/\/+$/, "");
  const name = file || cleanDir.split("/").pop() || cleanDir;
  const path = file ? `${dir.endsWith("/") ? dir : dir + "/"}${file}` : cleanDir;
  return { type, name, path };
}
