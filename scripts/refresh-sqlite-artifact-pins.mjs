// Maintainer-only bootstrap/verification, NEVER invoked by the runtime installer.
// GitHub API asset digests are compared with separately downloaded asset bytes.
// Electron's header service publishes separate SHASUMS: its exact bytes/hash,
// release ID and every checked artifact hash are recorded for review and pinned
// statically. Runtime installations do not fetch or trust mutable checksums.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

const require = createRequire(import.meta.url);
const tarStream = require("tar-stream");
const lock = JSON.parse(
  readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"),
);
const version = lock.packages["node_modules/better-sqlite3"].version;
const hosts = new Set([
  "api.github.com",
  "github.com",
  "release-assets.githubusercontent.com",
  "objects.githubusercontent.com",
  "artifacts.electronjs.org",
]);
const maxBytes = 32 * 1024 * 1024;
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sri = (hex) => `sha256-${Buffer.from(hex, "hex").toString("base64")}`;

async function download(url, limit = maxBytes, allow404 = false) {
  const signal = AbortSignal.timeout(60_000);
  for (let redirects = 0; redirects <= 5; redirects++) {
    const target = new URL(url);
    if (target.protocol !== "https:" || !hosts.has(target.hostname))
      throw new Error(`Untrusted bootstrap URL: ${url}`);
    const response = await fetch(url, {
      redirect: "manual",
      signal,
      headers: {
        "User-Agent": "RapiDB-SQLite-pin-maintenance",
        Accept: "application/vnd.github+json",
      },
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      await response.body?.cancel();
      url = new URL(response.headers.get("location"), url).toString();
      continue;
    }
    if (response.status === 404 && allow404) {
      await response.body?.cancel();
      return null;
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`HTTP ${response.status}: ${url}`);
    }
    if (Number(response.headers.get("content-length")) > limit) {
      await response.body?.cancel();
      throw new Error(`Oversized bootstrap artifact: ${url}`);
    }
    const chunks = [];
    let size = 0;
    for await (const chunk of response.body) {
      size += chunk.length;
      if (size > limit) throw new Error(`Oversized bootstrap artifact: ${url}`);
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }
  throw new Error(`Too many redirects: ${url}`);
}

async function release(repo, tag, allow404 = false) {
  const apiUrl = `https://api.github.com/repos/${repo}/releases/tags/${tag}`;
  const bytes = await download(apiUrl, 2 * 1024 * 1024, allow404);
  if (!bytes) return { apiUrl, status: 404, repo, tag };
  const metadata = JSON.parse(bytes);
  if (metadata.tag_name !== tag || !Number.isSafeInteger(metadata.id))
    throw new Error(`Invalid release metadata: ${apiUrl}`);
  return {
    apiUrl,
    status: 200,
    repo,
    tag,
    id: metadata.id,
    assets: metadata.assets,
  };
}

async function verifyAsset(releaseInfo, asset) {
  if (
    !/^sha256:[a-f0-9]{64}$/.test(asset.digest ?? "") ||
    !Number.isSafeInteger(asset.id)
  )
    throw new Error(`Missing API digest/ID: ${asset.name}`);
  const prefix = `https://github.com/${releaseInfo.repo}/releases/download/${releaseInfo.tag}/`;
  if (asset.browser_download_url !== `${prefix}${asset.name}`)
    throw new Error(`Unexpected asset URL: ${asset.name}`);
  const bytes = await download(asset.browser_download_url);
  const hash = sha256(bytes);
  if (`sha256:${hash}` !== asset.digest || bytes.length !== asset.size)
    throw new Error(`API/bytes mismatch: ${asset.name}`);
  return {
    url: asset.browser_download_url,
    integrity: sri(hash),
    size: bytes.length,
    releaseId: releaseInfo.id,
    assetId: asset.id,
    apiDigest: asset.digest,
  };
}

async function headerAbi(bytes, version) {
  const extract = tarStream.extract();
  let abi;
  let root;
  let entries = 0;
  const complete = new Promise((resolvePromise, rejectPromise) => {
    extract.on("error", rejectPromise);
    extract.on("finish", resolvePromise);
    extract.on("entry", (header, stream, next) => {
      if (++entries > 10_000)
        return extract.destroy(new Error("Too many header entries"));
      const chunks = [];
      stream.on("data", (chunk) => chunks.push(chunk));
      stream.on("end", () => {
        if (header.name.endsWith("/include/node/node_version.h")) {
          abi = /#define NODE_MODULE_VERSION\s+(\d+)/.exec(
            Buffer.concat(chunks).toString(),
          )?.[1];
          root = header.name.slice(0, -"/include/node/node_version.h".length);
        }
        next();
      });
    });
  });
  extract.end(gunzipSync(bytes, { maxOutputLength: 128 * 1024 * 1024 }));
  await complete;
  if (!abi || !["node_headers", `node-v${version}`].includes(root))
    throw new Error(`Cannot establish header ABI/root for Electron ${version}`);
  return { abi, root };
}

export async function collectSQLitePins() {
  const official = await release("WiseLibs/better-sqlite3", `v${version}`);
  const patched = await release(
    "DmitriiKholkin/RapiDB",
    "rapidb-patched-sqlite",
    true,
  );
  const pins = {
    schemaVersion: 2,
    betterSqlite3Version: version,
    officialRelease: { ...official, assets: undefined },
    patchedRelease: { ...patched, assets: undefined },
    prebuilts: {},
    headers: {},
  };
  for (const [kind, info] of [
    ["official", official],
    ["patched", patched],
  ]) {
    const assetPattern = new RegExp(
      `^better-sqlite3-v${version.replaceAll(".", "\\.")}-(?:electron|node)-v\\d+-[a-z0-9]+-[a-z0-9]+\\.tar\\.gz$`,
    );
    const selected = (info.assets ?? []).filter((asset) =>
      assetPattern.test(asset.name),
    );
    let cursor = 0;
    await Promise.all(
      Array.from({ length: 6 }, async () => {
        while (cursor < selected.length) {
          const asset = selected[cursor++];
          pins.prebuilts[`${kind}/${asset.name}`] = await verifyAsset(
            info,
            asset,
          );
        }
      }),
    );
    console.log(
      `${kind}: API and downloaded bytes verified for ${selected.length} assets (release status ${info.status})`,
    );
  }
  // Baseline headers are selected by Electron major + ABI, not by a mutable
  // latest endpoint. All minor/patch runtimes of that ABI build against this
  // pinned baseline; new majors/ABIs require an explicit reviewed refresh.
  for (const headerVersion of ["42.0.0", "43.0.0", "44.0.0"]) {
    const info = await release("electron/electron", `v${headerVersion}`, true);
    if (info.status !== 200) {
      console.log(`Unsupported headers ${headerVersion}: no public release`);
      continue;
    }
    const base = `https://artifacts.electronjs.org/headers/dist/v${headerVersion}/`;
    const checksumUrl = `${base}SHASUMS256.txt`;
    const checksums = await download(checksumUrl, 64 * 1024);
    const records = new Map(
      checksums
        .toString()
        .trim()
        .split("\n")
        .map((line) => {
          const match = /^([a-f0-9]{64})\s+\*?(\S+)$/.exec(line);
          if (!match)
            throw new Error(`Malformed headers SHASUMS: ${checksumUrl}`);
          return [match[2], match[1]];
        }),
    );
    const artifact = async (name) => {
      const hash = records.get(name);
      if (!hash) throw new Error(`Missing headers checksum: ${name}`);
      const bytes = await download(`${base}${name}`);
      if (sha256(bytes) !== hash)
        throw new Error(`Header artifact/SHASUMS mismatch: ${name}`);
      return {
        bytes,
        pin: {
          url: `${base}${name}`,
          integrity: sri(hash),
          size: bytes.length,
        },
      };
    };
    const headers = await artifact(`node-v${headerVersion}-headers.tar.gz`);
    const { abi, root: archiveRoot } = await headerAbi(
      headers.bytes,
      headerVersion,
    );
    const libraries = {};
    for (const [arch, name] of [
      ["x64", "x64/node.lib"],
      ["arm64", "arm64/node.lib"],
    ])
      libraries[arch] = (await artifact(name)).pin;
    pins.headers[`${headerVersion.split(".")[0]}-${abi}`] = {
      version: headerVersion,
      abi,
      archiveRoot,
      releaseId: info.id,
      releaseApiUrl: info.apiUrl,
      checksumUrl,
      checksumIntegrity: sri(sha256(checksums)),
      archive: headers.pin,
      windowsLibraries: libraries,
    };
    console.log(
      `Electron ${headerVersion} (ABI ${abi}): headers and Windows import libraries verified against captured upstream SHASUMS`,
    );
  }
  pins.prebuilts = Object.fromEntries(
    Object.entries(pins.prebuilts).sort(([a], [b]) => a.localeCompare(b)),
  );
  return pins;
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const outputFlag = process.argv.indexOf("--output");
  const checkFlag = process.argv.indexOf("--check");
  if (outputFlag < 0 && checkFlag < 0)
    throw new Error(
      "Use --output <candidate.json> or --check <committed-manifest.json>. No remote writes are performed.",
    );
  const pins = await collectSQLitePins();
  const serialized = `${JSON.stringify(pins, null, 2)}\n`;
  if (checkFlag >= 0) {
    const expected = JSON.parse(
      readFileSync(process.argv[checkFlag + 1], "utf8"),
    );
    if (JSON.stringify(expected) !== JSON.stringify(pins))
      throw new Error(
        "Upstream pins changed: review a --output candidate; do not silently accept it.",
      );
    console.log(
      "All committed SQLite pins agree with upstream metadata and independently downloaded bytes.",
    );
  } else {
    writeFileSync(process.argv[outputFlag + 1], serialized);
    console.log(
      "Candidate written. A separate reviewer must verify IDs/digests/bytes before shipping.",
    );
  }
}
