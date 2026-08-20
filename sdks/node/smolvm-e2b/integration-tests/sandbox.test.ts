/**
 * End-to-end integration tests for @smolvm/e2b against a live `smolvm serve`.
 *
 * Requires SMOLVM_API_URL to point at a running control plane, e.g.:
 *   smolvm serve start -l unix:///tmp/smolvm.sock
 *   SMOLVM_API_URL=unix:///tmp/smolvm.sock npm run test:integration
 *
 * Skipped automatically when SMOLVM_API_URL is unset.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { CommandExitError, Sandbox } from "../src/index.js";

const apiUrl = process.env.SMOLVM_API_URL;
const d = apiUrl ? describe : describe.skip;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

d("Sandbox e2e", () => {
  let sbx: Sandbox;

  beforeAll(async () => {
    sbx = await Sandbox.create({ template: "alpine", apiUrl });
  }, 300_000);

  afterAll(async () => {
    try {
      await Sandbox.kill(sbx?.sandboxId, { apiUrl });
    } catch {
      // already gone
    }
  });

  it("runs commands and captures output", async () => {
    const r = await sbx.commands.run("echo hello-sdk && echo oops >&2");
    expect(r.exitCode).toBe(0);
    expect(r.stdout.trim()).toBe("hello-sdk");
    expect(r.stderr.trim()).toBe("oops");
  });

  it("throws CommandExitError on non-zero exit", async () => {
    await expect(sbx.commands.run("exit 7")).rejects.toBeInstanceOf(CommandExitError);
    const r = await sbx.commands.run("exit 3", { throwOnError: false });
    expect(r.exitCode).toBe(3);
  });

  it("writes and reads files", async () => {
    await sbx.files.write("/tmp/foo.txt", "bar-baz");
    expect(await sbx.files.read("/tmp/foo.txt")).toBe("bar-baz");
    const bytes = await sbx.files.read("/tmp/foo.txt", { format: "bytes" });
    expect(Buffer.isBuffer(bytes)).toBe(true);
    expect(bytes.toString()).toBe("bar-baz");
  });

  it("sets an auto-idle timeout", async () => {
    await expect(sbx.setTimeout(600_000)).resolves.toBeUndefined();
  });

  it(
    "pauses and warm-resumes with running processes intact",
    async () => {
      // A background daemon writing a counter into tmpfs (RAM). Only a WARM
      // resume preserves the process and the tmpfs contents.
      await sbx.commands.run(
        ["sh", "-c", "i=0; while true; do i=$((i+1)); echo $i > /dev/shm/c; sleep 1; done"],
        { background: true },
      );
      await sleep(4000);
      const before = Number((await sbx.commands.run("cat /dev/shm/c")).stdout.trim());
      expect(before).toBeGreaterThan(0);

      const sandboxId = await sbx.pause();
      expect((await sbx.getInfo()).state).toBe("paused");

      const resumed = await Sandbox.resume(sandboxId, { apiUrl });
      const after1 = Number((await resumed.commands.run("cat /dev/shm/c")).stdout.trim());
      await sleep(3000);
      const after2 = Number((await resumed.commands.run("cat /dev/shm/c")).stdout.trim());

      // The tmpfs value survived (not a cold boot) and the process kept running.
      expect(after1).toBeGreaterThanOrEqual(before);
      expect(after2).toBeGreaterThan(after1);
      sbx = resumed; // afterAll cleans this one up
    },
    120_000,
  );

  it("lists sandboxes", async () => {
    const all = await Sandbox.list({ apiUrl });
    expect(all.some((s) => s.sandboxId === sbx.sandboxId)).toBe(true);
  });
});
