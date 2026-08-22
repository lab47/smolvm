/**
 * Start a sandbox, check its public egress IP (like `curl ifconfig.co`), and
 * leave it to auto-idle after 1 minute. Run against a `smolvm serve` control
 * plane (the host needs KVM to boot the VM):
 *
 *   # local unix socket
 *   smolvm serve start -l unix:///tmp/smolvm.sock
 *   SMOLVM_API_URL=unix:///tmp/smolvm.sock npm run example:ifconfig
 *
 *   # a TCP endpoint, with the e2b control-plane API key
 *   SMOLVM_API_URL=http://127.0.0.1:8099 SMOLVM_API_KEY=secret \
 *     npm run example:ifconfig
 */
import { Sandbox } from "../src/index.js";

async function main() {
  // network: true so the guest can reach the internet; timeoutMs 60_000 arms a
  // 1-minute auto-idle — the sandbox auto-pauses a minute after the last activity.
  // A VM boot occasionally misses agent-readiness on the first try, so retry once.
  let sbx: Sandbox | undefined;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      sbx = await Sandbox.create({
        template: "alpine",
        network: true,
        timeoutMs: 60_000,
        metadata: { example: "ifconfig" },
      });
      break;
    } catch (e) {
      if (attempt === 2) throw e;
      console.error("create failed, retrying once:", (e as Error).message);
    }
  }
  if (!sbx) throw new Error("sandbox create failed");
  console.log("sandbox:", sbx.sandboxId);

  // Alpine ships no curl; add it on demand (falling back to busybox wget), then
  // ask ifconfig.co for just the IP. sh -c runs the whole line in the guest.
  const r = await sbx.commands.run(
    "command -v curl >/dev/null 2>&1 || apk add --no-cache curl >/dev/null 2>&1; " +
      "curl -fsS https://ifconfig.co/ip || wget -qO- https://ifconfig.co/ip",
    { timeoutMs: 30_000, throwOnError: false },
  );

  if (r.exitCode === 0 && r.stdout.trim()) {
    console.log("egress IP:", r.stdout.trim());
  } else {
    console.error("could not fetch egress IP (exit", r.exitCode + ")");
    if (r.stderr.trim()) console.error(r.stderr.trim());
  }

  console.log(
    `\nsandbox ${sbx.sandboxId} left running; it auto-pauses ~1 min after the last activity.`,
  );
  console.log(`stop it early with:  smolvm machine rm -f ${sbx.sandboxId}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
