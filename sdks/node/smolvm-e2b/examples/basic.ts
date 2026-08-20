/**
 * Basic @smolvm/e2b usage. Run against a `smolvm serve` control plane:
 *
 *   smolvm serve start -l unix:///tmp/smolvm.sock
 *   SMOLVM_API_URL=unix:///tmp/smolvm.sock npm run example:basic
 */
import { Sandbox } from "../src/index.js";

async function main() {
  // Create a sandbox that auto-pauses after 5 minutes idle.
  const sbx = await Sandbox.create({ template: "python:3.12", timeoutMs: 5 * 60_000 });
  console.log("sandbox:", sbx.sandboxId);

  const r = await sbx.commands.run("python -c 'print(2 + 2)'");
  console.log("2 + 2 =", r.stdout.trim());

  await sbx.files.write("/tmp/hello.txt", "hello from the host");
  console.log("read back:", await sbx.files.read("/tmp/hello.txt"));

  // Suspend to disk (RAM + running processes preserved) and resume later.
  const id = await sbx.pause();
  console.log("paused:", id);

  const resumed = await Sandbox.resume(id);
  console.log("resumed, still 4 =", (await resumed.commands.run("python -c 'print(2+2)'")).stdout.trim());

  await resumed.kill();
  console.log("killed");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
