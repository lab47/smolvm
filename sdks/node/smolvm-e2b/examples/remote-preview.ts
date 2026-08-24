/**
 * Exercise a remote smolvm cluster end-to-end through its public TLS endpoint:
 * create a sandbox, run a web server inside it, and fetch it back through the
 * preview proxy (`getHost`). Defaults target the eu0.lab47.dev deployment.
 *
 *   SMOLVM_API_KEY=<key> npm run example:preview
 *
 * Override the target with env vars:
 *   SMOLVM_API_URL         (default https://api.sbx.eu0.lab47.dev)
 *   SMOLVM_PREVIEW_DOMAIN  (default sbx.eu0.lab47.dev)
 *   SMOLVM_API_KEY         (the control-plane X-API-Key)
 *
 * Pass `--kill` to delete the sandbox at the end instead of leaving it running.
 */
import { Sandbox } from "../src/index.js";

const apiKey = process.env.SMOLVM_API_KEY;
if (!apiKey) {
  console.error("Set SMOLVM_API_KEY to your cluster's control-plane X-API-Key.");
  process.exit(1);
}
const opts = {
  apiUrl: process.env.SMOLVM_API_URL ?? "https://api.sbx.eu0.lab47.dev",
  apiKey,
  previewDomain: process.env.SMOLVM_PREVIEW_DOMAIN ?? "sbx.eu0.lab47.dev",
};
const PORT = 8000;
const keepAlive = !process.argv.includes("--kill");

// A tiny HTTP responder using busybox `nc` (this rootfs has no httpd applet).
// Serves a fixed body with a correct Content-Length, forever.
const serverScript = `
BODY="hello from smolvm sandbox $(hostname) — $(date -u)
"
LEN=$(printf '%s' "$BODY" | wc -c)
while true; do
  printf 'HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nContent-Length: %s\\r\\nConnection: close\\r\\n\\r\\n%s' "$LEN" "$BODY" \\
    | nc -l -p ${PORT} 2>/dev/null || nc -l ${PORT} 2>/dev/null || break
done
`;

async function main() {
  console.log(`target: ${opts.apiUrl}  (preview domain ${opts.previewDomain})`);

  // VM boots occasionally miss agent-readiness on the first try; retry once.
  let sbx: Sandbox | undefined;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      sbx = await Sandbox.create({
        ...opts,
        template: "alpine",
        ports: [PORT],
        timeoutMs: 10 * 60_000, // auto-idle after 10 min so it cleans itself up
        metadata: { example: "remote-preview" },
      });
      break;
    } catch (e) {
      if (attempt === 2) throw e;
      console.error("create failed, retrying:", (e as Error).message);
    }
  }
  if (!sbx) throw new Error("sandbox create failed");
  console.log("sandbox:", sbx.sandboxId);

  // Sanity: the data plane (exec) works.
  const info = await sbx.commands.run("echo ok && cat /etc/os-release | head -1");
  console.log("exec:", info.stdout.trim().replace(/\n/g, " | "));

  // Launch the HTTP server detached so run() returns immediately.
  const b64 = Buffer.from(serverScript).toString("base64");
  await sbx.commands.run(
    `printf '%s' '${b64}' | base64 -d > /tmp/srv.sh; ` +
      `setsid sh /tmp/srv.sh </dev/null >/dev/null 2>&1 & echo launched`,
  );

  const url = `https://${sbx.getHost(PORT)}`;
  console.log("preview URL:", url);

  // Fetch it back through the public TLS preview proxy.
  await new Promise((r) => setTimeout(r, 1500));
  try {
    const res = await fetch(url);
    const body = (await res.text()).trim();
    console.log(`preview fetch: ${res.status} — "${body}"`);
  } catch (e) {
    console.error("preview fetch failed:", (e as Error).message);
  }

  if (keepAlive) {
    console.log("\nSandbox left running (auto-idles in ~10 min). Try it:");
    console.log(`  open ${url}`);
    console.log(`  curl ${url}`);
    console.log(`  smolvm cluster status --url ${opts.apiUrl} --api-key <key>`);
    console.log(`  (delete: re-run with --kill, or DELETE ${opts.apiUrl}/sandboxes/${sbx.sandboxId})`);
  } else {
    await sbx.kill();
    console.log("killed:", sbx.sandboxId);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
