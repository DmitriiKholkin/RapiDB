import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { gzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  cleanupTempDirectories,
  createProjectTempDir,
} from "../runtime/tempDirectories";
import {
  buildElfSharedObject,
  ELF_MACHINE,
  glibcPrebuild,
  muslPrebuild,
} from "./elfFixture";

const scriptUrl = new URL(
  "../../scripts/refresh-sqlite-artifact-pins.mjs",
  import.meta.url,
).href;
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
const version = "13.9.0";
const tarballUrl = `https://registry.npmjs.org/better-sqlite3/-/better-sqlite3-${version}.tgz`;
const metadataUrl = `https://registry.npmjs.org/better-sqlite3/${version}`;

type Entry = {
  name: string;
  data?: string | Buffer;
  type?: string;
  linkname?: string;
};

function prebuildBytes(target: string): string | Buffer {
  const arch = target.endsWith("arm64") ? "arm64" : "x64";
  if (target.startsWith("linuxmusl-")) return muslPrebuild(arch);
  if (target.startsWith("linux-")) return glibcPrebuild(arch);
  return `addon ${target}`;
}

function tarball(entries: Entry[]): Buffer {
  const chunks: Buffer[] = [];
  for (const entry of entries) {
    const header = Buffer.alloc(512);
    const data = Buffer.from(entry.data ?? "");
    header.write(entry.name, 0, 100);
    header.write("0000644\0", 100);
    header.write(`${data.length.toString(8).padStart(11, "0")}\0`, 124);
    header.fill(32, 148, 156);
    header.write(entry.type ?? "0", 156);
    if (entry.linkname) header.write(entry.linkname, 157, 100);
    header.write("ustar\0", 257);
    const sum = header.reduce((total, byte) => total + byte, 0);
    header.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148);
    chunks.push(header, data, Buffer.alloc((512 - (data.length % 512)) % 512));
  }
  return gzipSync(Buffer.concat([...chunks, Buffer.alloc(1024)]));
}

function packageEntries(
  options: {
    targets?: string[];
    extra?: Entry[];
    napi?: string;
    prebuilds?: Record<string, string | Buffer>;
  } = {},
): Entry[] {
  return [
    {
      name: "package/package.json",
      data: JSON.stringify({ name: "better-sqlite3", version }),
    },
    { name: "package/LICENSE", data: "MIT" },
    { name: "package/lib/index.js", data: "module.exports = 1;" },
    { name: "package/lib/methods/pragma.js", data: "module.exports = 2;" },
    { name: "package/src/addon.cpp", data: "// not installed" },
    {
      name: "package/binding.gyp",
      data: `{ 'defines': ['NAPI_VERSION=${options.napi ?? "10"}'] }`,
    },
    ...(options.targets ?? TARGETS).map((target) => ({
      name: `package/prebuilds/${target}.node`,
      data: options.prebuilds?.[target] ?? prebuildBytes(target),
    })),
    ...(options.extra ?? []),
  ];
}

const sri = (bytes: Buffer) =>
  `sha512-${createHash("sha512").update(bytes).digest("base64")}`;

async function bootstrapFixture(
  options: {
    entries?: Entry[];
    served?: Buffer;
    metadata?: Record<string, unknown>;
    resolved?: string;
  } = {},
) {
  const bytes = tarball(options.entries ?? packageEntries());
  const served = options.served ?? bytes;
  const metadata = {
    name: "better-sqlite3",
    version,
    dist: { tarball: tarballUrl, integrity: sri(bytes) },
    ...options.metadata,
  };
  const locked = {
    version,
    resolved: options.resolved ?? tarballUrl,
    integrity: sri(bytes),
  };
  const program = `
    const urls = [];
    const served = Buffer.from(${JSON.stringify(served.toString("base64"))}, "base64");
    globalThis.fetch = async (url, init) => {
      urls.push(String(url));
      if (init?.redirect !== "error") throw new Error("redirects must be refused");
      if (String(url) === ${JSON.stringify(metadataUrl)}) return new Response(${JSON.stringify(JSON.stringify(metadata))});
      if (String(url) === ${JSON.stringify(tarballUrl)}) return new Response(served);
      throw new Error('Unexpected bootstrap request: ' + url);
    };
    const { collectSQLitePins } = await import(${JSON.stringify(scriptUrl)});
    try { const pins = await collectSQLitePins({ locked: ${JSON.stringify(locked)} }); console.log('RESULT:' + JSON.stringify({pins, urls})); }
    catch (error) { console.log('RESULT:' + JSON.stringify({error: error.message, urls})); }
  `;
  const { stdout } = await promisify(execFile)(process.execPath, [
    "--input-type=module",
    "-e",
    program,
  ]);
  return {
    bytes,
    ...(JSON.parse(stdout.split("RESULT:")[1]) as {
      error?: string;
      urls: string[];
      pins?: {
        schemaVersion: number;
        betterSqlite3Version: string;
        nodeApiVersion: number;
        tarball: { url: string; integrity: string; size: number };
        files: Record<string, { sha256: string; size: number }>;
        prebuilds: Record<
          string,
          {
            path: string;
            sha256: string;
            size: number;
            minGlibc?: string;
            minGlibcxx?: string;
          }
        >;
      };
    }),
  };
}

describe("SQLite maintainer pin bootstrap", () => {
  it("pins the tarball, JavaScript files and all eight prebuilds only after registry, lockfile and bytes agree", async () => {
    const result = await bootstrapFixture();
    expect(result.error).toBeUndefined();
    expect(result.urls).toEqual([metadataUrl, tarballUrl]);
    const pins = result.pins;
    if (!pins) throw new Error("Expected pins");
    expect(pins).toMatchObject({
      schemaVersion: 3,
      betterSqlite3Version: version,
      nodeApiVersion: 10,
      tarball: {
        url: tarballUrl,
        integrity: sri(result.bytes),
        size: result.bytes.length,
      },
    });
    expect(Object.keys(pins.files).sort()).toEqual(
      [
        "LICENSE",
        "lib/index.js",
        "lib/methods/pragma.js",
        "package.json",
      ].sort(),
    );
    expect(Object.keys(pins.prebuilds).sort()).toEqual([...TARGETS].sort());
    const linux = glibcPrebuild("x64");
    expect(pins.prebuilds["linux-x64"]).toEqual({
      path: "prebuilds/linux-x64.node",
      sha256: createHash("sha256").update(linux).digest("hex"),
      size: linux.length,
      minGlibc: "2.34",
      minGlibcxx: "3.4.29",
    });
    expect(pins.prebuilds["linux-arm64"]).toMatchObject({
      minGlibc: "2.34",
      minGlibcxx: "3.4.29",
    });
    for (const target of [
      "linuxmusl-x64",
      "linuxmusl-arm64",
      "darwin-arm64",
      "win32-x64",
    ]) {
      expect(pins.prebuilds[target]).not.toHaveProperty("minGlibc");
      expect(pins.prebuilds[target]).not.toHaveProperty("minGlibcxx");
    }
  });

  it("records the highest GLIBC and GLIBCXX versions a glibc prebuild needs", async () => {
    const result = await bootstrapFixture({
      entries: packageEntries({
        prebuilds: {
          "linux-x64": glibcPrebuild("x64", "2.36", "3.4.31"),
          "linux-arm64": glibcPrebuild("arm64", "2.17", "3.4.21"),
        },
      }),
    });
    expect(result.error).toBeUndefined();
    expect(result.pins?.prebuilds["linux-x64"]).toMatchObject({
      minGlibc: "2.36",
      minGlibcxx: "3.4.31",
    });
    expect(result.pins?.prebuilds["linux-arm64"]).toMatchObject({
      // libm's GLIBC_2.29 is the highest glibc version across all libraries.
      minGlibc: "2.29",
      minGlibcxx: "3.4.21",
    });
  });

  it.each([
    ["a Linux prebuild that is not ELF", { "linux-x64": "not an ELF file" }],
    [
      "an ELF prebuild for the wrong machine",
      { "linux-x64": glibcPrebuild("arm64") },
    ],
    [
      "a glibc prebuild without symbol versions",
      {
        "linux-x64": buildElfSharedObject({
          machine: ELF_MACHINE.x64,
          needed: ["libc.so.6"],
        }),
      },
    ],
    [
      "an unreviewed shared library dependency",
      {
        "linux-x64": buildElfSharedObject({
          machine: ELF_MACHINE.x64,
          needed: ["libc.so.6", "libssl.so.3"],
          versions: {
            "libc.so.6": ["GLIBC_2.34"],
            "libstdc++.so.6": ["GLIBCXX_3.4.29"],
          },
        }),
      },
    ],
    [
      "a musl prebuild with glibc symbol versions",
      {
        "linuxmusl-x64": buildElfSharedObject({
          machine: ELF_MACHINE.x64,
          needed: ["libc.musl-x86_64.so.1", "libstdc++.so.6"],
          versions: { "libstdc++.so.6": ["GLIBCXX_3.4.29"] },
        }),
      },
    ],
  ])("refuses %s", async (_name, prebuilds) => {
    const result = await bootstrapFixture({
      entries: packageEntries({ prebuilds }),
    });
    expect(result.error).toMatch(/ELF|glibc|libssl|versioned|machine/);
    expect(result.pins).toBeUndefined();
  });

  it.each([
    [
      "an extensionless file in lib",
      { name: "package/lib/util", data: "module.exports = 'shadow';" },
    ],
    [
      "a case variant in lib",
      { name: "package/lib/Util.JS", data: "module.exports = 'shadow';" },
    ],
    ["an unknown top-level file", { name: "package/install.js", data: "x" }],
    [
      "a native file outside prebuilds",
      { name: "package/lib/x.node", data: "x" },
    ],
  ])("fails on %s that is neither pinned nor reviewed as unused", async (_name, extra) => {
    const result = await bootstrapFixture({
      entries: packageEntries({ extra: [extra] }),
    });
    expect(result.error).toContain("Unreviewed better-sqlite3 package files");
    expect(result.pins).toBeUndefined();
  });

  it.each([
    ["a package outside the tarball", "require('bindings')"],
    ["a relative path that is not pinned", "require('./missing')"],
    ["a path escaping the package", "require('../../evil.js')"],
    ["an unreviewed dynamic require", "require(process.env.ADDON)"],
    ["a dynamic import", "import('./methods/pragma.js')"],
  ])("fails on %s in installed JavaScript", async (_name, code) => {
    const result = await bootstrapFixture({
      entries: packageEntries({
        extra: [{ name: "package/lib/extra.js", data: code }],
      }),
    });
    expect(result.error).toMatch(/Unreviewed require\(\)/);
    expect(result.pins).toBeUndefined();
  });

  it("accepts builtins and relative requires of pinned files", async () => {
    const result = await bootstrapFixture({
      entries: packageEntries({
        extra: [
          {
            name: "package/lib/extra.js",
            data: "require('fs'); require('node:path'); require('./methods/pragma'); require('./index.js'); require('../prebuilds/linux-x64.node');",
          },
        ],
      }),
    });
    expect(result.error).toBeUndefined();
    expect(Object.keys(result.pins?.files ?? {})).toContain("lib/extra.js");
  });

  it.each([
    [
      "registry integrity differs from the lockfile",
      { metadata: { dist: { tarball: tarballUrl, integrity: "sha512-AAAA" } } },
      "does not match the package-lock.json",
    ],
    [
      "registry tarball URL points elsewhere",
      {
        metadata: {
          dist: { tarball: "https://evil.invalid/x.tgz", integrity: "x" },
        },
      },
      "does not match the package-lock.json",
    ],
    [
      "the package declares install scripts",
      { metadata: { scripts: { install: "node-gyp rebuild" } } },
      "install scripts",
    ],
  ])("refuses before downloading bytes when %s", async (_name, options, message) => {
    const result = await bootstrapFixture(options);
    expect(result.error).toContain(message);
    expect(result.pins).toBeUndefined();
    expect(result.urls).toEqual([metadataUrl]);
  });

  it("refuses a lockfile that resolves to a different host", async () => {
    const result = await bootstrapFixture({
      resolved: "https://evil.invalid/better-sqlite3.tgz",
    });
    expect(result.error).toContain("Unexpected locked tarball URL");
    expect(result.urls).toEqual([]);
  });

  it("rejects substituted tarball bytes instead of emitting a pin", async () => {
    const result = await bootstrapFixture({
      served: tarball(packageEntries({ napi: "11" })),
    });
    expect(result.error).toContain("do not match the locked sha512");
    expect(result.pins).toBeUndefined();
  });

  it.each([
    ["a missing prebuild", packageEntries({ targets: TARGETS.slice(1) })],
    [
      "an extra prebuild",
      packageEntries({ targets: [...TARGETS, "linux-ia32"] }),
    ],
  ])("requires review for %s", async (_name, entries) => {
    const result = await bootstrapFixture({ entries });
    expect(result.error).toContain("Unexpected prebuild set");
  });

  it.each([
    { name: "package/lib/link.js", type: "2", linkname: "/etc/passwd" },
    { name: "outside/file.js", data: "x" },
    { name: "package/lib/index.js", data: "duplicate" },
  ])("rejects unexpected tarball entry $name", async (extra) => {
    const result = await bootstrapFixture({
      entries: packageEntries({ extra: [extra] }),
    });
    expect(result.error).toMatch(
      /Unexpected tarball entry|Duplicate tarball entry/,
    );
  });
});

describe("SQLite maintainer pin CLI", () => {
  const fixturePin = { path: "prebuilds/x.node", sha256: "ab", size: 42 };
  const fixture = {
    schemaVersion: 3,
    tarball: { url: "https://registry.npmjs.org/x.tgz", size: 1 },
    prebuilts: { "official/fixture.tar.gz": fixturePin },
    fixtureArray: [{ major: 42, abi: "145" }, null],
  };
  const collected = {
    ...fixture,
    tarball: { ...fixture.tarball, collectorOnly: undefined },
  };

  async function run(args: string[], collect = vi.fn(async () => collected)) {
    const { runSQLitePinMaintenance } = await import(scriptUrl);
    await runSQLitePinMaintenance(args, collect);
  }

  async function fixturePath(name: string) {
    return join(await createProjectTempDir("sqlite-pin-maintenance"), name);
  }

  afterEach(cleanupTempDirectories);

  it.each([
    { args: [] },
    { args: ["--check"] },
    { args: ["--output"] },
    { args: ["--check", "--output"] },
    { args: ["--output", "--check"] },
    { args: ["--output", "--unknown"] },
    { args: ["--check", ""] },
    { args: ["--output", " "] },
    { args: ["--unknown", "fixture.json"] },
    { args: ["--check", "fixture.json", "--output", "candidate.json"] },
  ])("rejects $args with usage before collecting", async ({ args }) => {
    const collect = vi.fn(async () => collected);
    await expect(run(args, collect)).rejects.toThrow(
      /Usage:.*--output <candidate\.json> or --check <committed-manifest\.json>/,
    );
    expect(collect).not.toHaveBeenCalled();
  });

  it("accepts reordering at every object level without modifying the manifest", async () => {
    const path = await fixturePath("manifest.json");
    const manifest = JSON.stringify({
      fixtureArray: [{ abi: "145", major: 42 }, null],
      prebuilts: {
        "official/fixture.tar.gz": {
          size: 42,
          sha256: "ab",
          path: "prebuilds/x.node",
        },
      },
      tarball: { size: 1, url: "https://registry.npmjs.org/x.tgz" },
      schemaVersion: 3,
    });
    await writeFile(path, manifest);
    const log = vi.spyOn(console, "log").mockImplementation(() => {});

    await run(["--check", path]);

    expect(log).toHaveBeenCalledWith(
      expect.stringContaining("independently downloaded bytes"),
    );
    expect(await readFile(path, "utf8")).toBe(manifest);
  });

  it.each([
    {
      change: "array order",
      pins: { fixtureArray: [null, fixture.fixtureArray[0]] },
    },
    {
      change: "array length",
      pins: { fixtureArray: [fixture.fixtureArray[0]] },
    },
    {
      change: "array type",
      pins: { fixtureArray: { ...fixture.fixtureArray } },
    },
    { change: "value type", pins: { schemaVersion: "3" } },
    {
      change: "pin content",
      pins: {
        prebuilts: { "official/fixture.tar.gz": { ...fixturePin, size: 43 } },
      },
    },
    { change: "missing pin", pins: { prebuilts: {} } },
    { change: "extra key", pins: { extra: true } },
  ])("fails closed for a change in $change", async ({ pins }) => {
    const path = await fixturePath("manifest.json");
    const manifest = JSON.stringify({ ...fixture, ...pins });
    await writeFile(path, manifest);

    await expect(run(["--check", path])).rejects.toThrow(
      "Upstream pins changed: review a --output candidate; do not silently accept it.",
    );
    expect(await readFile(path, "utf8")).toBe(manifest);
  });

  it("does not overwrite a candidate if collection fails verification", async () => {
    const path = await fixturePath("candidate.json");
    const manifest = JSON.stringify(fixture);
    await writeFile(path, manifest);
    const collect = vi.fn(async () => {
      throw new Error("API/bytes mismatch");
    });

    await expect(run(["--output", path], collect)).rejects.toThrow(
      "API/bytes mismatch",
    );
    expect(await readFile(path, "utf8")).toBe(manifest);
  });

  it("writes deterministic candidate JSON and requires separate review", async () => {
    const path = await fixturePath("candidate.json");
    const log = vi.spyOn(console, "log").mockImplementation(() => {});

    await run(["--output", path]);
    const first = await readFile(path, "utf8");
    await run(["--output", path]);

    expect(first).toBe(`${JSON.stringify(fixture, null, 2)}\n`);
    expect(await readFile(path, "utf8")).toBe(first);
    expect(log).toHaveBeenCalledWith(
      "Candidate written. A separate reviewer must verify the version, integrity and hashes before shipping.",
    );
  });
});
