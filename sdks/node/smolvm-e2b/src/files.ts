import type { Client } from "./client.js";
import type { Commands } from "./commands.js";
import type { FileEntry, FileInfo, FileReadOpts } from "./types.js";

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
