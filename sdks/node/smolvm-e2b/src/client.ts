/**
 * Minimal, dependency-free HTTP transport for the smolvm control-plane API
 * (`smolvm serve`). Talks to either a TCP endpoint (`http(s)://host:port`) or a
 * Unix domain socket (`unix:///path/to.sock`) — the smolvm serve default.
 */
import http from "node:http";
import https from "node:https";

import { SmolvmError, errorFromResponse } from "./errors.js";

/** Connection options for a smolvm serve endpoint. */
export interface ConnectionOpts {
  /**
   * Base URL of the smolvm serve API. Either a TCP URL (`http://127.0.0.1:8080`)
   * or a Unix socket (`unix:///run/user/1000/smolvm.sock`). Defaults to
   * `$SMOLVM_API_URL`, else the serve default Unix socket if discoverable, else
   * `http://127.0.0.1:8080`.
   */
  apiUrl?: string;
  /** Bearer token sent as `Authorization: Bearer <token>` (fleet auth). */
  apiKey?: string;
  /** Per-request timeout in milliseconds (default 60000; long ops override it). */
  requestTimeoutMs?: number;
  /** mTLS client material (fleet mode over TCP). Passed to Node's https agent. */
  tls?: { ca?: string | Buffer; cert?: string | Buffer; key?: string | Buffer };
}

interface ParsedTarget {
  socketPath?: string;
  protocol: "http:" | "https:";
  host?: string;
  port?: number;
}

function parseApiUrl(apiUrl: string): ParsedTarget {
  if (apiUrl.startsWith("unix://")) {
    // unix:///abs/path.sock  ->  socketPath = /abs/path.sock
    return { socketPath: apiUrl.slice("unix://".length), protocol: "http:" };
  }
  const u = new URL(apiUrl);
  if (u.protocol !== "http:" && u.protocol !== "https:") {
    throw new SmolvmError(`unsupported apiUrl protocol: ${u.protocol}`);
  }
  return {
    protocol: u.protocol,
    host: u.hostname,
    port: u.port ? Number(u.port) : u.protocol === "https:" ? 443 : 80,
  };
}

function defaultApiUrl(): string {
  if (process.env.SMOLVM_API_URL) return process.env.SMOLVM_API_URL;
  const runtimeDir = process.env.XDG_RUNTIME_DIR;
  if (runtimeDir) return `unix://${runtimeDir}/smolvm.sock`;
  return "http://127.0.0.1:8080";
}

export interface RequestOpts {
  /** JSON body to send (sets content-type application/json). */
  json?: unknown;
  /** Raw body to send (Buffer/Uint8Array/string), e.g. file uploads. */
  body?: Buffer | Uint8Array | string;
  /** Content-Type for a raw `body`. */
  contentType?: string;
  /** Expect a raw binary response (returns Buffer) instead of JSON. */
  raw?: boolean;
  /** Override the client's default per-request timeout for this call. */
  timeoutMs?: number;
  /** Extra headers. */
  headers?: Record<string, string>;
}

/** Thin JSON/binary HTTP client bound to one smolvm serve endpoint. */
export class Client {
  private target: ParsedTarget;
  private apiKey?: string;
  private defaultTimeoutMs: number;
  private tls?: ConnectionOpts["tls"];

  constructor(opts: ConnectionOpts = {}) {
    this.target = parseApiUrl(opts.apiUrl ?? defaultApiUrl());
    this.apiKey = opts.apiKey ?? process.env.SMOLVM_API_KEY;
    this.defaultTimeoutMs = opts.requestTimeoutMs ?? 60_000;
    this.tls = opts.tls;
  }

  async requestJson<T>(method: string, path: string, opts: RequestOpts = {}): Promise<T> {
    const buf = await this.request(method, path, opts);
    if (buf.length === 0) return undefined as unknown as T;
    return JSON.parse(buf.toString("utf8")) as T;
  }

  async request(method: string, path: string, opts: RequestOpts = {}): Promise<Buffer> {
    const headers: Record<string, string> = { accept: "application/json", ...opts.headers };
    let payload: Buffer | undefined;
    if (opts.json !== undefined) {
      payload = Buffer.from(JSON.stringify(opts.json), "utf8");
      headers["content-type"] = "application/json";
    } else if (opts.body !== undefined) {
      payload = Buffer.isBuffer(opts.body)
        ? opts.body
        : typeof opts.body === "string"
          ? Buffer.from(opts.body, "utf8")
          : Buffer.from(opts.body);
      headers["content-type"] = opts.contentType ?? "application/octet-stream";
    }
    if (payload) headers["content-length"] = String(payload.length);
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;

    const isHttps = this.target.protocol === "https:";
    const transport = isHttps ? https : http;
    const reqOpts: http.RequestOptions = {
      method,
      path,
      headers,
      // Unix socket vs TCP.
      socketPath: this.target.socketPath,
      host: this.target.host,
      port: this.target.port,
      // A Host header is required; any value works for the Unix socket.
      ...(this.target.socketPath ? { headers: { ...headers, host: "localhost" } } : {}),
    };
    if (isHttps && this.tls) Object.assign(reqOpts, this.tls);

    const timeoutMs = opts.timeoutMs ?? this.defaultTimeoutMs;

    return new Promise<Buffer>((resolve, reject) => {
      const req = transport.request(reqOpts, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) {
            resolve(buf);
          } else {
            reject(errorFromResponse(status, buf, `${method} ${path}`));
          }
        });
      });
      req.on("error", (e) => reject(new SmolvmError(`request failed: ${e.message}`)));
      req.setTimeout(timeoutMs, () => {
        req.destroy(new SmolvmError(`request timed out after ${timeoutMs}ms: ${method} ${path}`));
      });
      if (payload) req.write(payload);
      req.end();
    });
  }
}
