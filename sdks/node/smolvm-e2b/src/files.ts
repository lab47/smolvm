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
   * Watch a directory inside the sandbox for changes, invoking `onEvent` on each
   * create/modify/remove.
   *
   * NOTE: this is **polling-based** (default every 1s), not inotify — smolvm has
   * no guest→host filesystem event stream yet, so it diffs directory snapshots
   * over `commands`. Fine for build-on-change and similar; not sub-second. Call
   * `.stop()` on the returned watcher to end it.
   */
  async watchDir(
    path: string,
    onEvent: (event: FileEvent) => void,
    opts: { intervalMs?: number; recursive?: boolean } = {},
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
