/** Base error for all @smolvm/e2b failures. */
export class SmolvmError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SmolvmError";
  }
}

/** The sandbox (machine) does not exist (HTTP 404). */
export class NotFoundError extends SmolvmError {
  constructor(message: string) {
    super(message);
    this.name = "NotFoundError";
  }
}

/** The request conflicts with the sandbox's current state (HTTP 409),
 * e.g. resuming a sandbox that isn't paused. */
export class ConflictError extends SmolvmError {
  constructor(message: string) {
    super(message);
    this.name = "ConflictError";
  }
}

/** Authentication/authorization failure (HTTP 401/403). */
export class AuthError extends SmolvmError {
  constructor(message: string) {
    super(message);
    this.name = "AuthError";
  }
}

/** A command finished with a non-zero exit code (thrown by `commands.run`
 * unless `{ throwOnError: false }` is passed). */
export class CommandExitError extends SmolvmError {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  constructor(exitCode: number, stdout: string, stderr: string) {
    super(`command exited with code ${exitCode}${stderr ? `: ${stderr.trim()}` : ""}`);
    this.name = "CommandExitError";
    this.exitCode = exitCode;
    this.stdout = stdout;
    this.stderr = stderr;
  }
}

/** Build the right error subclass from an HTTP status + body. */
export function errorFromResponse(status: number, body: Buffer, context: string): SmolvmError {
  let detail = body.toString("utf8");
  try {
    const parsed = JSON.parse(detail) as { error?: string; message?: string };
    detail = parsed.error ?? parsed.message ?? detail;
  } catch {
    // non-JSON body; use as-is
  }
  const msg = `${context} -> ${status}${detail ? `: ${detail}` : ""}`;
  switch (status) {
    case 401:
    case 403:
      return new AuthError(msg);
    case 404:
      return new NotFoundError(msg);
    case 409:
      return new ConflictError(msg);
    default:
      return new SmolvmError(msg);
  }
}
