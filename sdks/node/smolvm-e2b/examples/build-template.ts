/**
 * Build the "default" smolvm template: Ubuntu 26.04 + latest Go, Rust (rustup), Node,
 * Bun, and Ruby (via rbenv + ruby-build for easy version switches). Provisions a
 * build sandbox, runs the setup steps, then snapshots it into a named template
 * so `Sandbox.create({ template: "default" })` boots it fast.
 *
 *   SMOLVM_API_KEY=<key> npm run example:build-template
 *
 * Env:
 *   SMOLVM_API_URL   (default https://api.sbx.eu0.lab47.dev)
 *   SMOLVM_API_KEY   (required — control-plane X-API-Key)
 *   TEMPLATE_ALIAS   (default "default")
 *   TEMPLATE_BASE    (default "ubuntu:26.04")
 */
import { Sandbox, Client } from "../src/index.js";

const apiKey = process.env.SMOLVM_API_KEY;
if (!apiKey) {
  console.error("Set SMOLVM_API_KEY to your cluster's control-plane X-API-Key.");
  process.exit(1);
}
const opts = { apiUrl: process.env.SMOLVM_API_URL ?? "https://api.sbx.eu0.lab47.dev", apiKey };
const ALIAS = process.env.TEMPLATE_ALIAS ?? "default";
const BASE = process.env.TEMPLATE_BASE ?? "ubuntu:26.04";

const MIN = 60_000;
// [label, script, timeoutMs] — each runs as `bash -lc`, so /etc/profile.d PATH
// entries written by earlier steps are picked up by later ones.
const steps: [string, string, number][] = [
  [
    "apt: base + build deps",
    String.raw`set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  ca-certificates curl git unzip xz-utils build-essential pkg-config \
  autoconf bison libssl-dev libyaml-dev libreadline-dev zlib1g-dev \
  libncurses-dev libffi-dev libgdbm-dev libgmp-dev libdb-dev uuid-dev
echo apt-done`,
    10 * MIN,
  ],
  [
    "Go (latest)",
    String.raw`set -e
GO_VER=$(curl -fsSL "https://go.dev/VERSION?m=text" | head -1)
echo "installing $GO_VER"
curl -fsSL "https://go.dev/dl/$GO_VER.linux-amd64.tar.gz" -o /tmp/go.tgz
rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz
echo 'export PATH=/usr/local/go/bin:$PATH' > /etc/profile.d/10-go.sh
/usr/local/go/bin/go version`,
    6 * MIN,
  ],
  [
    "Rust (rustup, system-wide in /opt/rust)",
    String.raw`set -e
export RUSTUP_HOME=/opt/rust CARGO_HOME=/opt/rust
curl -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile default
printf 'export RUSTUP_HOME=/opt/rust\nexport CARGO_HOME=/opt/rust\nexport PATH=/opt/rust/bin:$PATH\n' > /etc/profile.d/20-rust.sh
/opt/rust/bin/rustc --version && /opt/rust/bin/cargo --version`,
    10 * MIN,
  ],
  [
    "Node (latest, official binary)",
    String.raw`set -e
TB=$(curl -fsSL https://nodejs.org/dist/latest/ | grep -oE 'node-v[0-9.]+-linux-x64\.tar\.xz' | head -1)
echo "installing $TB"
curl -fsSL "https://nodejs.org/dist/latest/$TB" -o /tmp/node.tar.xz
mkdir -p /opt/node && tar -C /opt/node --strip-components=1 -xf /tmp/node.tar.xz && rm /tmp/node.tar.xz
echo 'export PATH=/opt/node/bin:$PATH' > /etc/profile.d/30-node.sh
export PATH=/opt/node/bin:$PATH   # npm's #!/usr/bin/env node needs node on PATH
node --version && npm --version`,
    6 * MIN,
  ],
  [
    "Bun (latest)",
    String.raw`set -e
export BUN_INSTALL=/opt/bun
curl -fsSL https://bun.sh/install | bash
printf 'export BUN_INSTALL=/opt/bun\nexport PATH=/opt/bun/bin:$PATH\n' > /etc/profile.d/40-bun.sh
/opt/bun/bin/bun --version`,
    6 * MIN,
  ],
  [
    "Ruby (rbenv + ruby-build; compiles from source)",
    String.raw`set -e
export RBENV_ROOT=/opt/rbenv
git clone --depth 1 https://github.com/rbenv/rbenv.git $RBENV_ROOT
git clone --depth 1 https://github.com/rbenv/ruby-build.git $RBENV_ROOT/plugins/ruby-build
printf 'export RBENV_ROOT=/opt/rbenv\nexport PATH=/opt/rbenv/bin:/opt/rbenv/shims:$PATH\neval "$(rbenv init - bash 2>/dev/null)" || true\n' > /etc/profile.d/50-rbenv.sh
RUBY_VER=$($RBENV_ROOT/plugins/ruby-build/bin/ruby-build --definitions | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | tail -1)
echo "installing ruby $RUBY_VER (this is the slow step)"
MAKE_OPTS="-j$(nproc)" $RBENV_ROOT/bin/rbenv install -s $RUBY_VER
$RBENV_ROOT/bin/rbenv global $RUBY_VER
$RBENV_ROOT/bin/rbenv rehash
$RBENV_ROOT/shims/ruby --version && $RBENV_ROOT/shims/gem --version`,
    35 * MIN,
  ],
  [
    "cleanup + verify",
    String.raw`set -e
apt-get clean && rm -rf /var/lib/apt/lists/*
echo '=== installed toolchain (login shell) ==='
bash -lc 'go version; rustc --version; node --version; npm --version; bun --version; ruby --version; rbenv --version'`,
    3 * MIN,
  ],
];

async function main() {
  const t0 = Date.now();
  console.log(`building template '${ALIAS}' from ${BASE} on ${opts.apiUrl}`);

  const sbx = await Sandbox.create({
    ...opts,
    template: BASE,
    network: true,
    cpus: 8,
    memoryMb: 8192,
    timeoutMs: 0, // never auto-idle during the build
  });
  console.log("build sandbox:", sbx.sandboxId);

  try {
    for (const [label, script, timeoutMs] of steps) {
      console.log(`\n===> ${label}`);
      const r = await sbx.commands.run(["bash", "-lc", script], {
        timeoutMs,
        onStdout: (s) => process.stdout.write(s),
        onStderr: (s) => process.stderr.write(s),
      });
      if (r.exitCode !== 0) throw new Error(`step failed: "${label}" (exit ${r.exitCode})`);
    }

    // pack requires a stopped machine.
    const client = new Client(opts);
    console.log("\n===> stopping build sandbox");
    await client.request("POST", `/api/v1/machines/${encodeURIComponent(sbx.sandboxId)}/stop`, {
      timeoutMs: 2 * MIN,
    });
    console.log(`===> packing template '${ALIAS}' (snapshotting the rootfs)`);
    const info = await client.requestJson<{ alias: string; sizeBytes: number; path: string }>(
      "POST",
      `/api/v1/machines/${encodeURIComponent(sbx.sandboxId)}/pack`,
      { json: { alias: ALIAS }, timeoutMs: 20 * MIN },
    );
    const mins = ((Date.now() - t0) / 60000).toFixed(1);
    console.log(
      `\n✅ built template '${info.alias}' — ${(info.sizeBytes / 1e9).toFixed(2)} GB in ${mins} min`,
    );
    console.log(`   use it: Sandbox.create({ template: "${info.alias}", apiUrl, apiKey })`);
  } finally {
    await Sandbox.kill(sbx.sandboxId, opts).catch(() => {});
  }
}

main().catch((e) => {
  console.error("\nbuild failed:", e instanceof Error ? e.message : e);
  process.exit(1);
});
