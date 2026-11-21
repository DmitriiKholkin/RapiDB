import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { builtinModules, createRequire } from "node:module";
import { posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { gunzipSync } from "node:zlib";
import { readElfRequirements } from "./sqlite-elf-requirements.mjs";

const require = createRequire(import.meta.url);
const tarStream = require("tar-stream");
const REGISTRY = "registry.npmjs.org";
const TARGETS = [
  "darwin-arm64",
  "darwin-x64",
  "linux-arm64",
  "linux-x64",
  "linuxmusl-arm64",
  "linuxmusl-x64",
  "win32-arm64",
  "win32-x64",
];
const MAX_TARBALL_BYTES = 32 * 1024 * 1024;
const MAX_EXPANDED_BYTES = 128 * 1024 * 1024;
const INSTALLABLE_FILE =
  /^(?:package\.json|LICENSE|lib\/(?:[a-z0-9-]+\/)?[a-z0-9-]+\.js)$/;
// Package files that are reviewed as unused at runtime and never installed.
// Every other file must be pinned (INSTALLABLE_FILE or a prebuild) or the
// refresh fails for review.
const IGNORED_PACKAGE_FILE = /^(?:README\.md|binding\.gyp|deps\/.+|src\/.+)$/;
const PREBUILD_FILE = /^prebuilds\/[^/]+\.node$/;
// Non-literal require() calls reviewed in better-sqlite3's lib/binding.js;
// they load only the nativeBinding path RapiDB passes or node-gyp outputs.
const REVIEWED_DYNAMIC_REQUIRES = new Map([
  ["lib/binding.js", new Set(["filename", "getPrebuildPath() || filename"])],
]);
const GLIBC_TARGETS = new Set(["linux-x64", "linux-arm64"]);
const MUSL_TARGETS = new Set(["linuxmusl-x64", "linuxmusl-arm64"]);
// Shared libraries a Linux prebuild may depend on; anything else is reviewed.
const GLIBC_ALLOWED_LIBRARIES = new Set([
  "libc.so.6",
  "libm.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libgcc_s.so.1",
  "libstdc++.so.6",
  "ld-linux-x86-64.so.2",
  "ld-linux-aarch64.so.1",
]);
const MUSL_ALLOWED_LIBRARIES = new Set([
  "libc.musl-x86_64.so.1",
  "libc.musl-aarch64.so.1",
  "libgcc_s.so.1",
  "libstdc++.so.6",
]);
const TARGET_MACHINES = { x64: "x64", arm64: "arm64" };
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sha512 = (bytes) =>
  `sha512-${createHash("sha512").update(bytes).digest("base64")}`;

function lockedPackage() {
  const lock = JSON.parse(
    readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"),
  );
  const entry = lock.packages["node_modules/better-sqlite3"];
  if (!entry?.version || !entry.integrity || !entry.resolved)
    throw new Error("better-sqlite3 is not locked in package-lock.json");
  return entry;
}

async function download(url, limit) {
  const target = new URL(url);
  if (target.protocol !== "https:" || target.hostname !== REGISTRY)
    throw new Error(`Untrusted bootstrap URL: ${url}`);
  const response = await fetch(url, {
    redirect: "error",
    signal: AbortSignal.timeout(120_000),
    headers: { "User-Agent": "RapiDB-SQLite-pin-maintenance" },
  });
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

async function readTarball(bytes) {
  const files = new Map();
  const extract = tarStream.extract();
  const complete = new Promise((resolvePromise, rejectPromise) => {
    extract.on("error", rejectPromise);
    extract.on("finish", resolvePromise);
    extract.on("entry", (header, stream, next) => {
      if (header.type !== "file" || !header.name.startsWith("package/")) {
        extract.destroy(
          new Error(`Unexpected tarball entry: ${header.type} ${header.name}`),
        );
        return;
      }
      const path = header.name.slice("package/".length);
      if (files.has(path)) {
        extract.destroy(new Error(`Duplicate tarball entry: ${path}`));
        return;
      }
      const chunks = [];
      stream.on("data", (chunk) => chunks.push(chunk));
      stream.on("end", () => {
        files.set(path, Buffer.concat(chunks));
        next();
      });
    });
  });
  extract.end(gunzipSync(bytes, { maxOutputLength: MAX_EXPANDED_BYTES }));
  await complete;
  return files;
}

// Fails on any package file that is neither pinned nor explicitly ignored.
export function classifySQLitePackageFiles(paths) {
  const pinned = [];
  const prebuilds = [];
  const unexpected = [];
  for (const path of [...paths].sort()) {
    if (INSTALLABLE_FILE.test(path)) pinned.push(path);
    else if (PREBUILD_FILE.test(path)) prebuilds.push(path);
    else if (!IGNORED_PACKAGE_FILE.test(path)) unexpected.push(path);
  }
  if (unexpected.length > 0)
    throw new Error(
      `Unreviewed better-sqlite3 package files: ${unexpected.join(", ")}; pin them or add them to the reviewed ignore list.`,
    );
  return { pinned, prebuilds };
}

function requireArgument(source, open) {
  let depth = 0;
  for (let index = open; index < source.length; index += 1) {
    if (source[index] === "(") depth += 1;
    else if (source[index] === ")" && --depth === 0)
      return source.slice(open + 1, index).trim();
  }
  return null;
}

// Every require() in the installed JavaScript must load a Node.js builtin or
// a relative path that resolves to a pinned file (or a pinned prebuild). Any
// other require, or an unreviewed dynamic one, fails the refresh.
export function auditSQLiteRequires(files, pinnedPaths, prebuildPaths = []) {
  const known = new Set([...pinnedPaths, ...prebuildPaths]);
  const builtins = new Set(builtinModules);
  const problems = [];
  for (const path of [...pinnedPaths].sort()) {
    if (!path.endsWith(".js")) continue;
    const source = files.get(path)?.toString("utf8") ?? "";
    if (
      /\bimport\s*\(|\bprocess\.dlopen\b|\bcreateRequire\b|\bmodule\.require\b/.test(
        source,
      )
    )
      problems.push(`${path}: unreviewed module loading`);
    for (const match of source.matchAll(/\brequire\s*\(/g)) {
      const argument = requireArgument(
        source,
        match.index + match[0].length - 1,
      );
      const literal = /^(['"])([^'"\\]+)\1$/.exec(argument ?? "");
      if (!literal) {
        if (!REVIEWED_DYNAMIC_REQUIRES.get(path)?.has(argument ?? ""))
          problems.push(`${path}: dynamic require(${argument})`);
        continue;
      }
      const specifier = literal[2];
      if (specifier.startsWith("node:") || builtins.has(specifier)) continue;
      if (!specifier.startsWith("./") && !specifier.startsWith("../")) {
        problems.push(`${path}: require(${JSON.stringify(specifier)})`);
        continue;
      }
      const base = posix.normalize(posix.join(posix.dirname(path), specifier));
      const candidates = [
        base,
        `${base}.js`,
        `${base}.json`,
        `${base}/index.js`,
      ];
      if (base.startsWith("../") || !candidates.some((c) => known.has(c)))
        problems.push(
          `${path}: require(${JSON.stringify(specifier)}) does not resolve to a pinned file`,
        );
    }
  }
  if (problems.length > 0)
    throw new Error(
      `Unreviewed require() in better-sqlite3: ${problems.join("; ")}.`,
    );
}

// Reads the glibc/libstdc++ symbol versions a Linux prebuild needs from its
// ELF .gnu.version_r section and checks its library dependencies.
export function linuxPrebuildRequirements(target, bytes) {
  const elf = readElfRequirements(bytes);
  const arch = target.split("-").pop();
  if (elf.machine !== TARGET_MACHINES[arch])
    throw new Error(`${target}: ELF machine ${elf.machine} does not match.`);
  const allowed = GLIBC_TARGETS.has(target)
    ? GLIBC_ALLOWED_LIBRARIES
    : MUSL_ALLOWED_LIBRARIES;
  const unexpected = elf.needed.filter((library) => !allowed.has(library));
  if (unexpected.length > 0)
    throw new Error(
      `${target}: unreviewed shared library dependencies ${unexpected.join(", ")}.`,
    );
  if (GLIBC_TARGETS.has(target)) {
    if (!elf.minGlibc || !elf.minGlibcxx)
      throw new Error(
        `${target}: cannot establish the minimum glibc/libstdc++ version.`,
      );
    return { minGlibc: elf.minGlibc, minGlibcxx: elf.minGlibcxx };
  }
  if (Object.keys(elf.versions).length > 0)
    throw new Error(
      `${target}: a musl prebuild unexpectedly requires versioned symbols.`,
    );
  return {};
}

export async function collectSQLitePins({ locked = lockedPackage() } = {}) {
  const version = locked.version;
  const tarballUrl = `https://${REGISTRY}/better-sqlite3/-/better-sqlite3-${version}.tgz`;
  const registryMetadataUrl = `https://${REGISTRY}/better-sqlite3/${version}`;
  if (locked.resolved !== tarballUrl)
    throw new Error(`Unexpected locked tarball URL: ${locked.resolved}`);
  const metadata = JSON.parse(
    (await download(registryMetadataUrl, 1024 * 1024)).toString("utf8"),
  );
  if (
    metadata.name !== "better-sqlite3" ||
    metadata.version !== version ||
    metadata.dist?.tarball !== tarballUrl ||
    metadata.dist?.integrity !== locked.integrity
  )
    throw new Error(
      "Registry metadata does not match the package-lock.json tarball URL and integrity.",
    );
  if (
    metadata.scripts?.install ||
    metadata.scripts?.preinstall ||
    metadata.scripts?.postinstall
  )
    throw new Error(
      "better-sqlite3 declares install scripts; review required.",
    );
  const bytes = await download(tarballUrl, MAX_TARBALL_BYTES);
  if (sha512(bytes) !== locked.integrity)
    throw new Error("Tarball bytes do not match the locked sha512 integrity.");
  const files = await readTarball(bytes);
  const packageJson = JSON.parse(files.get("package.json")?.toString("utf8"));
  if (packageJson.name !== "better-sqlite3" || packageJson.version !== version)
    throw new Error("Tarball package.json does not match the locked version.");
  const napi = /NAPI_VERSION=(\d+)/.exec(
    files.get("binding.gyp")?.toString("utf8") ?? "",
  )?.[1];
  if (!napi) throw new Error("Cannot establish the addon Node-API version.");
  const classified = classifySQLitePackageFiles(files.keys());
  const prebuilds = classified.prebuilds;
  const expected = TARGETS.map((target) => `prebuilds/${target}.node`).sort();
  if (!isDeepStrictEqual(prebuilds, expected))
    throw new Error(
      `Unexpected prebuild set: ${prebuilds.join(", ")}; review required.`,
    );
  const pins = {
    schemaVersion: 3,
    betterSqlite3Version: version,
    nodeApiVersion: Number(napi),
    registryMetadataUrl,
    tarball: {
      url: tarballUrl,
      integrity: locked.integrity,
      size: bytes.length,
    },
    files: {},
    prebuilds: {},
  };
  for (const path of classified.pinned) {
    const data = files.get(path);
    pins.files[path] = { sha256: sha256(data), size: data.length };
  }
  if (!pins.files["lib/index.js"] || !pins.files["package.json"])
    throw new Error("Tarball is missing lib/index.js or package.json.");
  auditSQLiteRequires(files, classified.pinned, prebuilds);
  for (const target of TARGETS) {
    const path = `prebuilds/${target}.node`;
    const data = files.get(path);
    pins.prebuilds[target] = {
      path,
      sha256: sha256(data),
      size: data.length,
      ...(GLIBC_TARGETS.has(target) || MUSL_TARGETS.has(target)
        ? linuxPrebuildRequirements(target, data)
        : {}),
    };
  }
  console.log(
    `better-sqlite3 ${version}: registry metadata, lockfile integrity and tarball bytes agree; ${Object.keys(pins.files).length} JavaScript/metadata files and ${TARGETS.length} prebuilds pinned (Node-API ${napi}).`,
  );
  return pins;
}

export async function runSQLitePinMaintenance(
  args,
  collect = collectSQLitePins,
) {
  const [mode, path] = args;
  if (
    args.length !== 2 ||
    (mode !== "--output" && mode !== "--check") ||
    !path?.trim() ||
    path.startsWith("-")
  )
    throw new Error(
      "Usage: node scripts/refresh-sqlite-artifact-pins.mjs --output <candidate.json> or --check <committed-manifest.json>. No remote writes are performed.",
    );

  const expected =
    mode === "--check" ? JSON.parse(readFileSync(path, "utf8")) : undefined;
  const pins = await collect();
  const serialized = `${JSON.stringify(pins, null, 2)}\n`;
  if (mode === "--check") {
    if (!isDeepStrictEqual(expected, JSON.parse(serialized)))
      throw new Error(
        "Upstream pins changed: review a --output candidate; do not silently accept it.",
      );
    console.log(
      "All committed SQLite pins agree with registry metadata and independently downloaded bytes.",
    );
  } else {
    writeFileSync(path, serialized);
    console.log(
      "Candidate written. A separate reviewer must verify the version, integrity and hashes before shipping.",
    );
  }
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  try {
    await runSQLitePinMaintenance(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
