import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  SQLITE_ARTIFACTS,
  sqliteArtifactPolicyId,
  sqlitePackagePlan,
  sqliteTarballUrl,
} from "../../src/extension/utils/sqliteArtifactPins";
import { SQLITE_NATIVE_TARGETS } from "../../src/extension/utils/sqliteNativeTarget";
import {
  nodeApiVersionOfNode,
  oldestVSCodeWithNodeApi,
  VSCODE_NODE_VERSIONS,
} from "../../src/extension/utils/vscodeNodeApi";

const packageJson = JSON.parse(
  await readFile(new URL("../../package.json", import.meta.url), "utf8"),
);
const lock = JSON.parse(
  await readFile(new URL("../../package-lock.json", import.meta.url), "utf8"),
);
const localPackage = dirname(require.resolve("better-sqlite3/package.json"));

const elfScriptUrl = new URL(
  "../../scripts/sqlite-elf-requirements.mjs",
  import.meta.url,
).href;

function sha256(path: string) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

describe("SQLite artifact manifest coverage", () => {
  it("pins one better-sqlite3 version across package, lockfile and the npm tarball", () => {
    const version = SQLITE_ARTIFACTS.betterSqlite3Version;
    const locked = lock.packages["node_modules/better-sqlite3"];
    expect(packageJson.devDependencies["better-sqlite3"]).toBe(version);
    expect(packageJson.dependencies["better-sqlite3"]).toBeUndefined();
    expect(locked.version).toBe(version);
    expect(locked.resolved).toBe(sqliteTarballUrl(version));
    expect(locked.integrity).toBe(SQLITE_ARTIFACTS.tarball.integrity);
    expect(SQLITE_ARTIFACTS.tarball.url).toBe(
      `https://registry.npmjs.org/better-sqlite3/-/better-sqlite3-${version}.tgz`,
    );
    expect(SQLITE_ARTIFACTS.registryMetadataUrl).toBe(
      `https://registry.npmjs.org/better-sqlite3/${version}`,
    );
    expect(SQLITE_ARTIFACTS.schemaVersion).toBe(3);
    expect(Object.keys(SQLITE_ARTIFACTS)).not.toEqual(
      expect.arrayContaining(["headers", "patchedRelease", "officialRelease"]),
    );
  });

  it("pins exactly the eight Node-API prebuild targets with distinct SHA-256 hashes", () => {
    expect(Object.keys(SQLITE_ARTIFACTS.prebuilds).sort()).toEqual(
      [...SQLITE_NATIVE_TARGETS].sort(),
    );
    const hashes = new Set<string>();
    for (const target of SQLITE_NATIVE_TARGETS) {
      const pin = SQLITE_ARTIFACTS.prebuilds[target];
      expect(pin.path).toBe(`prebuilds/${target}.node`);
      expect(pin.sha256).toMatch(/^[0-9a-f]{64}$/);
      expect(pin.size).toBeGreaterThan(1024 * 1024);
      expect(pin.size).toBeLessThan(16 * 1024 * 1024);
      hashes.add(pin.sha256);
    }
    expect(hashes.size).toBe(SQLITE_NATIVE_TARGETS.length);
  });

  it.each(
    SQLITE_NATIVE_TARGETS,
  )("builds a complete reviewed install plan for %s", (target) => {
    const plan = sqlitePackagePlan(target);
    expect(plan.target).toBe(target);
    expect(plan.files.map((file) => file.installPath)).toEqual(
      expect.arrayContaining([
        "package.json",
        "LICENSE",
        "lib/index.js",
        "lib/binding.js",
        "lib/database.js",
        "build/Release/better_sqlite3.node",
      ]),
    );
    const binary = plan.files.find(
      (file) => file.installPath === "build/Release/better_sqlite3.node",
    );
    expect(binary).toMatchObject({
      archivePath: `prebuilds/${target}.node`,
      sha256: SQLITE_ARTIFACTS.prebuilds[target].sha256,
    });
    expect(
      plan.files.filter((file) => file.archivePath.startsWith("prebuilds/")),
    ).toHaveLength(1);
    expect(sqliteArtifactPolicyId(target)).toMatch(/^pins-v6-[0-9a-f]{16}$/);
  });

  it("keys every target's cache by its own pin, not by other targets", () => {
    const ids = SQLITE_NATIVE_TARGETS.map((target) =>
      sqliteArtifactPolicyId(target),
    );
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("matches the lockfile-verified local package byte for byte for every pinned file", () => {
    for (const [path, pin] of Object.entries(SQLITE_ARTIFACTS.files)) {
      expect([path, sha256(join(localPackage, path))]).toEqual([
        path,
        pin.sha256,
      ]);
    }
    for (const pin of Object.values(SQLITE_ARTIFACTS.prebuilds)) {
      expect([pin.path, sha256(join(localPackage, pin.path))]).toEqual([
        pin.path,
        pin.sha256,
      ]);
    }
    const binding = readFileSync(join(localPackage, "binding.gyp"), "utf8");
    expect(binding).toContain(
      `NAPI_VERSION=${SQLITE_ARTIFACTS.nodeApiVersion}`,
    );
  });

  it("does not require more Node-API than the oldest supported VS Code provides", () => {
    const range = /^\^(\d+\.\d+\.\d+)$/.exec(packageJson.engines.vscode);
    if (!range) throw new Error("engines.vscode must be a caret range");
    const oldest = VSCODE_NODE_VERSIONS.find(
      (entry) => entry.vscode === range[1],
    );
    // A raised engines.vscode needs its Node.js version in vscodeNodeApi.ts.
    expect(oldest).toBeDefined();
    const nodeApi = nodeApiVersionOfNode(oldest?.node ?? "0.0.0");
    expect(SQLITE_ARTIFACTS.nodeApiVersion).toBeLessThanOrEqual(nodeApi);
    expect(oldestVSCodeWithNodeApi(SQLITE_ARTIFACTS.nodeApiVersion)).toBe(
      range[1].replace(/\.0$/, ""),
    );
  });

  it.each([
    ["22.13.1", 9],
    ["22.14.0", 10],
    ["22.15.1", 10],
    ["23.5.0", 9],
    ["23.6.0", 10],
    ["24.0.0", 10],
    ["20.2.0", 8],
    ["20.3.0", 9],
    ["18.16.0", 8],
    ["16.0.0", 8],
    ["14.16.0", 0],
  ])("maps Node.js %s to Node-API %i", (node, napi) => {
    expect(nodeApiVersionOfNode(node)).toBe(napi);
  });

  it("pins glibc/libstdc++ minimums that match the glibc prebuilds' ELF symbol versions", async () => {
    const { readElfRequirements } = await import(elfScriptUrl);
    for (const target of SQLITE_NATIVE_TARGETS) {
      const pin = SQLITE_ARTIFACTS.prebuilds[target];
      if (!target.startsWith("linux")) {
        expect(pin.minGlibc).toBeUndefined();
        continue;
      }
      const elf = readElfRequirements(
        readFileSync(join(localPackage, pin.path)),
      );
      if (target.startsWith("linuxmusl-")) {
        expect(elf.minGlibc).toBeNull();
        expect(pin.minGlibc).toBeUndefined();
        expect(pin.minGlibcxx).toBeUndefined();
      } else {
        expect([target, pin.minGlibc, pin.minGlibcxx]).toEqual([
          target,
          elf.minGlibc,
          elf.minGlibcxx,
        ]);
        expect(sqlitePackagePlan(target).libraryRequirements).toEqual({
          minGlibc: elf.minGlibc,
          minGlibcxx: elf.minGlibcxx,
        });
      }
    }
  });
});
