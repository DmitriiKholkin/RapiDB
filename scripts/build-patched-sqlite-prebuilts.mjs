#!/usr/bin/env node

/**
 * Builds patched better-sqlite3 prebuilts for Electron 42 across all platforms.
 *
 * Usage:
 *   node scripts/build-patched-sqlite-prebuilts.mjs [--platform darwin-arm64,darwin-x64,linux-x64,linux-arm64]
 *
 * Output:
 *   /tmp/rapidb-patched-prebuilts/better-sqlite3-v12.10.0-electron-v146-{platform}-{arch}.tar.gz
 *
 * Requirements:
 *   - macOS: native build (no Docker needed for darwin targets)
 *   - Linux targets: Docker Desktop with multi-platform support
 *   - For linux-arm64: `docker run --privileged --rm tonistiigi/binfmt --install arm64`
 */

import { execSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";

const BETTER_SQLITE3_VERSION = "12.10.0";
const ELECTRON_VERSION = "42.2.0";
const ELECTRON_ABI = "146";
const OUTPUT_DIR = "/tmp/rapidb-patched-prebuilts";

const PATCH_LINES = {
  macros: {
    search:
      "#define OnlyAddon static_cast<Addon*>(info.Data().As<v8::External>()->Value())",
    replace: [
      "#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 146",
      "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value), 0)",
      "#define EXTERNAL_VALUE(value) (value)->Value(0)",
      "#else",
      "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value))",
      "#define EXTERNAL_VALUE(value) (value)->Value()",
      "#endif",
      "#define OnlyAddon static_cast<Addon*>(EXTERNAL_VALUE(info.Data().As<v8::External>()))",
    ].join("\\n"),
  },
  betterSqlite3: {
    search: "v8::Local<v8::External> data = v8::External::New(isolate, addon);",
    replace: "v8::Local<v8::External> data = EXTERNAL_NEW(isolate, addon);",
  },
  helpers: {
    search: "\\t\\tfunc,\\n\\t\\t0,\\n\\t\\tdata",
    replace: "\\t\\tfunc,\\n\\t\\tnullptr,\\n\\t\\tdata",
  },
};

// ---------- helpers ----------

function run(cmd, opts = {}) {
  console.log(`  ▸ ${cmd}`);
  execSync(cmd, { stdio: "inherit", ...opts });
}

function tarballName(platform, arch) {
  return `better-sqlite3-v${BETTER_SQLITE3_VERSION}-electron-v${ELECTRON_ABI}-${platform}-${arch}.tar.gz`;
}

// ---------- macOS native build ----------

function buildNative(platform, arch) {
  const label = `${platform}-${arch}`;
  const tarball = tarballName(platform, arch);
  const outPath = join(OUTPUT_DIR, tarball);

  if (existsSync(outPath)) {
    console.log(`⏭  ${label} — already exists, skipping`);
    return;
  }

  console.log(`\n🔨 Building ${label} (native)…`);

  const tmpDir = join(OUTPUT_DIR, `.tmp-${label}`);
  rmSync(tmpDir, { recursive: true, force: true });
  mkdirSync(tmpDir, { recursive: true });

  // Download and extract source
  run(
    `npm pack better-sqlite3@${BETTER_SQLITE3_VERSION} --pack-destination "${tmpDir}"`,
  );
  run(
    `tar xzf "${tmpDir}/better-sqlite3-${BETTER_SQLITE3_VERSION}.tgz" -C "${tmpDir}" --strip-components=1`,
  );

  // Apply patch
  applyPatch(tmpDir);

  // Build
  run(
    [
      "npx node-gyp rebuild",
      `--target=${ELECTRON_VERSION}`,
      `--arch=${arch}`,
      `--target_platform=${platform}`,
      "--dist-url=https://electronjs.org/headers",
      "--runtime=electron",
    ].join(" "),
    { cwd: tmpDir },
  );

  // Package
  const binaryPath = join(tmpDir, "build", "Release", "better_sqlite3.node");
  if (!existsSync(binaryPath)) {
    console.error(`❌ ${label} — build did not produce better_sqlite3.node`);
    return;
  }

  run(`tar czf "${outPath}" -C "${tmpDir}" build/Release/better_sqlite3.node`);
  rmSync(tmpDir, { recursive: true, force: true });
  console.log(`✅ ${label} → ${outPath}`);
}

// ---------- Docker build for Linux ----------

function buildDocker(platform, arch) {
  const label = `${platform}-${arch}`;
  const tarball = tarballName(platform, arch);
  const outPath = join(OUTPUT_DIR, tarball);

  if (existsSync(outPath)) {
    console.log(`⏭  ${label} — already exists, skipping`);
    return;
  }

  console.log(`\n🐳 Building ${label} (Docker)…`);

  const dockerArch = arch === "arm64" ? "linux/arm64" : "linux/amd64";

  // Write the build script to a temp file, then pipe into Docker via stdin
  // (single-file bind mounts are unreliable on Docker Desktop for Mac)
  const scriptPath = join(
    process.env.TMPDIR || "/tmp",
    `rapidb-build-${label}.sh`,
  );

  // Use a JS patcher embedded as a heredoc to avoid shell/tab escaping issues
  const patcherJs = [
    'const fs = require("fs");',
    'const path = require("path");',
    "",
    "// macros.cpp",
    'const macrosPath = path.join("/build", "src", "util", "macros.cpp");',
    'let macros = fs.readFileSync(macrosPath, "utf8");',
    "macros = macros.replace(",
    '  "#define OnlyAddon static_cast<Addon*>(info.Data().As<v8::External>()->Value())",',
    "  [",
    '    "#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 146",',
    '    "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value), 0)",',
    '    "#define EXTERNAL_VALUE(value) (value)->Value(0)",',
    '    "#else",',
    '    "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value))",',
    '    "#define EXTERNAL_VALUE(value) (value)->Value()",',
    '    "#endif",',
    '    "#define OnlyAddon static_cast<Addon*>(EXTERNAL_VALUE(info.Data().As<v8::External>()))",',
    '  ].join("\\n"),',
    ");",
    'fs.writeFileSync(macrosPath, macros, "utf8");',
    "",
    "// better_sqlite3.cpp",
    'const mainPath = path.join("/build", "src", "better_sqlite3.cpp");',
    'let mainCpp = fs.readFileSync(mainPath, "utf8");',
    "mainCpp = mainCpp.replace(",
    '  "v8::Local<v8::External> data = v8::External::New(isolate, addon);",',
    '  "v8::Local<v8::External> data = EXTERNAL_NEW(isolate, addon);",',
    ");",
    '// MSVC compat: Electron 42 V8 headers use __builtin_frame_address (GCC only)',
    'if (!mainCpp.includes("__builtin_frame_address")) {',
    '  mainCpp = "#ifdef _MSC_VER\\n#define __builtin_frame_address(x) ((void*)0)\\n#endif\\n" + mainCpp;',
    "}",
    'fs.writeFileSync(mainPath, mainCpp, "utf8");',
    "",
    "// helpers.cpp",
    'const helpersPath = path.join("/build", "src", "util", "helpers.cpp");',
    'let helpers = fs.readFileSync(helpersPath, "utf8");',
    "helpers = helpers.replace(",
    '  "\\t\\tfunc,\\n\\t\\t0,\\n\\t\\tdata",',
    '  "\\t\\tfunc,\\n\\t\\tnullptr,\\n\\t\\tdata",',
    ");",
    'fs.writeFileSync(helpersPath, helpers, "utf8");',
    "",
    'console.log("Patch applied successfully");',
  ].join("\n");

  const script = [
    "#!/bin/bash",
    "set -e",
    "apt-get update -qq && apt-get install -y -qq python3 g++ make >/dev/null 2>&1",
    "rm -rf /var/lib/apt/lists/*",
    "mkdir -p /build && cd /build",
    `npm pack better-sqlite3@${BETTER_SQLITE3_VERSION} 2>/dev/null`,
    `tar xzf better-sqlite3-${BETTER_SQLITE3_VERSION}.tgz --strip-components=1`,
    `node -e '${patcherJs}'`,
    "npx node-gyp rebuild \\",
    `  --target=${ELECTRON_VERSION} \\`,
    `  --arch=${arch} \\`,
    `  --target_platform=${platform} \\`,
    "  --dist-url=https://electronjs.org/headers \\",
    "  --runtime=electron",
    // Signal build completion (no tar here — we copy via docker cp below)
    "echo BUILD_COMPLETE",
  ].join("\n");
  writeFileSync(scriptPath, script, "utf8");

  // Use docker create + cp script + start + cp binary out (no --rm!)
  const containerId = execSync(
    [
      "docker create",
      `--platform ${dockerArch}`,
      "node:22-bookworm",
      "bash /opt/build.sh",
    ].join(" "),
    { encoding: "utf8" },
  ).trim();

  try {
    run(`docker cp "${scriptPath}" "${containerId}:/opt/build.sh"`);
    run(`docker start -a "${containerId}"`);

    // Copy the built binary out of the container
    const binaryContainerPath = `${containerId}:/build/build/Release/better_sqlite3.node`;
    const tmpBinary = join(OUTPUT_DIR, `better_sqlite3-${label}.node`);
    execSync(`docker cp "${binaryContainerPath}" "${tmpBinary}"`, {
      stdio: "pipe",
    });

    // Create the tarball on the host
    const tmpDir = join(OUTPUT_DIR, `.tar-${label}`);
    mkdirSync(join(tmpDir, "build", "Release"), { recursive: true });
    cpSync(tmpBinary, join(tmpDir, "build", "Release", "better_sqlite3.node"));
    run(
      `tar czf "${outPath}" -C "${tmpDir}" build/Release/better_sqlite3.node`,
    );
    rmSync(tmpDir, { recursive: true, force: true });
    rmSync(tmpBinary, { force: true });
  } finally {
    execSync(`docker rm -f "${containerId}"`, { stdio: "ignore" });
  }

  rmSync(scriptPath, { force: true });

  if (existsSync(outPath)) {
    console.log(`✅ ${label} → ${outPath}`);
  } else {
    console.error(`❌ ${label} — tarball was not produced`);
  }
}

// ---------- Patch helpers ----------

function applyPatch(dir) {
  // macros.cpp
  const macrosPath = join(dir, "src", "util", "macros.cpp");
  let macros = readFileSync(macrosPath, "utf8");
  macros = macros.replace(
    PATCH_LINES.macros.search,
    PATCH_LINES.macros.replace.replace(/\\n/g, "\n"),
  );
  writeFileSync(macrosPath, macros, "utf8");

  // better_sqlite3.cpp
  const mainPath = join(dir, "src", "better_sqlite3.cpp");
  let mainCpp = readFileSync(mainPath, "utf8");
  mainCpp = mainCpp.replace(
    PATCH_LINES.betterSqlite3.search,
    PATCH_LINES.betterSqlite3.replace,
  );
  writeFileSync(mainPath, mainCpp, "utf8");

  // helpers.cpp
  const helpersPath = join(dir, "src", "util", "helpers.cpp");
  let helpers = readFileSync(helpersPath, "utf8");
  helpers = helpers.replace(
    PATCH_LINES.helpers.search.replace(/\\t/g, "\t").replace(/\\n/g, "\n"),
    PATCH_LINES.helpers.replace.replace(/\\t/g, "\t").replace(/\\n/g, "\n"),
  );
  writeFileSync(helpersPath, helpers, "utf8");
}

function buildPatchCommand() {
  // sed-based patch for Docker (no Node.js fs available inside raw bash)
  return [
    // macros.cpp
    `sed -i 's|#define OnlyAddon static_cast<Addon\\*>(info.Data().As<v8::External>()->Value())|#if defined(NODE_MODULE_VERSION) \\&\\& NODE_MODULE_VERSION >= 146\\n#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value), 0)\\n#define EXTERNAL_VALUE(value) (value)->Value(0)\\n#else\\n#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value))\\n#define EXTERNAL_VALUE(value) (value)->Value()\\n#endif\\n#define OnlyAddon static_cast<Addon\\*>(EXTERNAL_VALUE(info.Data().As<v8::External>()))|' src/util/macros.cpp`,
    // better_sqlite3.cpp
    `sed -i 's|v8::Local<v8::External> data = v8::External::New(isolate, addon);|v8::Local<v8::External> data = EXTERNAL_NEW(isolate, addon);|' src/better_sqlite3.cpp`,
    // helpers.cpp — replace the 0 argument with nullptr in SetNativeDataProperty
    `sed -i '/SetNativeDataProperty/,/);/s/\\t\\t0,/\\t\\tnullptr,/' src/util/helpers.cpp`,
  ].join(" && ");
}

// ---------- Main ----------

const allPlatforms = ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64"];
const requested = process.argv
  .find((a) => a.startsWith("--platform="))
  ?.split("=")[1]
  ?.split(",");

const platforms = requested ?? allPlatforms;

mkdirSync(OUTPUT_DIR, { recursive: true });

console.log(
  `📦 Building patched better-sqlite3 prebuilts for Electron ${ELECTRON_VERSION}`,
);
console.log(`   Output: ${OUTPUT_DIR}/`);
console.log(`   Platforms: ${platforms.join(", ")}\n`);

for (const target of platforms) {
  const [platform, arch] = target.split("-");
  if (platform === "darwin") {
    buildNative(platform, arch);
  } else if (platform === "linux") {
    buildDocker(platform, arch);
  } else {
    console.log(
      `⚠️  ${target} — Windows builds require CI (GitHub Actions). Skipping.`,
    );
  }
}

console.log(`\n📁 All tarballs in ${OUTPUT_DIR}/:`);
run(`ls -lh ${OUTPUT_DIR}/*.tar.gz 2>/dev/null || echo "(none)"`);
console.log(
  `\n📤 Upload to GitHub Releases:\n   gh release create rapidb-patched-sqlite ${OUTPUT_DIR}/*.tar.gz --title "Patched better-sqlite3 prebuilts for Electron 42"`,
);
