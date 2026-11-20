import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  cleanupTempDirectories,
  createProjectTempDir,
} from "../runtime/tempDirectories";

const scriptUrl = new URL(
  "../../scripts/refresh-sqlite-artifact-pins.mjs",
  import.meta.url,
).href;

async function bootstrapFixture(
  options: { digest?: string; url?: string; size?: number; name?: string } = {},
) {
  const bytes = Buffer.from("fixture asset bytes");
  const digest = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  const name =
    options.name ?? "better-sqlite3-v12.10.0-electron-v145-darwin-arm64.tar.gz";
  const assetUrl = `https://github.com/WiseLibs/better-sqlite3/releases/download/v12.10.0/${name}`;
  const asset = {
    name,
    id: 123,
    digest: options.digest ?? digest,
    size: options.size ?? bytes.length,
    browser_download_url: options.url ?? assetUrl,
  };
  const program = `
    const urls = [];
    const bytes = Buffer.from(${JSON.stringify(bytes.toString("base64"))}, "base64");
    globalThis.fetch = async (url) => {
      urls.push(String(url));
      if (String(url).includes('/repos/WiseLibs/')) return new Response(JSON.stringify({id: 321008618, tag_name: 'v12.10.0', assets: [${JSON.stringify(asset)}]}));
      if (String(url).includes('api.github.com')) return new Response('', {status: 404});
      if (String(url) === ${JSON.stringify(assetUrl)}) return new Response(bytes);
      throw new Error('Unexpected bootstrap request: ' + url);
    };
    const { collectSQLitePins } = await import(${JSON.stringify(scriptUrl)});
    try { const pins = await collectSQLitePins(); console.log('RESULT:' + JSON.stringify({pins, urls})); }
    catch (error) { console.log('RESULT:' + JSON.stringify({error: error.message, urls})); }
  `;
  const { stdout } = await promisify(execFile)(process.execPath, [
    "--input-type=module",
    "-e",
    program,
  ]);
  return JSON.parse(stdout.split("RESULT:")[1]) as {
    error?: string;
    urls: string[];
    pins?: {
      prebuilts: Record<
        string,
        { integrity: string; releaseId: number; assetId: number }
      >;
    };
  };
}

describe("SQLite maintainer pin bootstrap", () => {
  it.each([
    ...[121, 123, 125, 128, 130, 132, 133, 135, 136, 139, 140, 143, 145].map(
      (abi) => `better-sqlite3-v12.10.0-electron-v${abi}-win32-ia32.tar.gz`,
    ),
    ...[127, 137, 141, 147].flatMap((abi) =>
      ["linux", "linuxmusl"].map(
        (platform) =>
          `better-sqlite3-v12.10.0-node-v${abi}-${platform}-arm.tar.gz`,
      ),
    ),
  ])("does not downgrade a published ia32/arm target during pin refresh: %s", async (name) => {
    const result = await bootstrapFixture({ name });
    expect(result.error).toBeUndefined();
    expect(Object.keys(result.pins?.prebuilts ?? {})).toEqual([
      `official/${name}`,
    ]);
    expect(
      result.urls.filter((url) => url.startsWith("https://github.com/")),
    ).toHaveLength(1);
  });
  it("records API identity only after separately downloaded bytes match its digest and size", async () => {
    const result = await bootstrapFixture();
    expect(result.error).toBeUndefined();
    const pin = Object.values(result.pins?.prebuilts ?? {})[0];
    expect(pin.assetId).toBe(123);
    expect(pin.releaseId).toBe(321008618);
    expect(pin.integrity).toMatch(/^sha256-/);
    expect(
      result.urls.filter((url) => url.startsWith("https://github.com/")),
    ).toHaveLength(1);
  });

  it.each([
    { digest: "missing" },
    { url: "https://evil.invalid/payload" },
  ])("rejects absent API digests or an unexpected publisher URL before downloading bytes", async (options) => {
    const result = await bootstrapFixture(options);
    expect(result.error).toMatch(/Missing API digest|Unexpected asset URL/);
    expect(
      result.urls.filter((url) => !url.startsWith("https://api.github.com/")),
    ).toEqual([]);
  });

  it.each([
    { digest: `sha256:${"0".repeat(64)}` },
    { size: 1 },
  ])("rejects bytes/digest or bytes/size substitution instead of emitting a pin", async (options) => {
    const result = await bootstrapFixture(options);
    expect(result.error).toContain("API/bytes mismatch");
    expect(result.pins).toBeUndefined();
  });
});

describe("SQLite maintainer pin CLI", () => {
  const fixturePin = { assetId: 123, integrity: "sha256-fixture", size: 42 };
  const fixture = {
    schemaVersion: 2,
    officialRelease: { id: 321, status: 200 },
    prebuilts: { "official/fixture.tar.gz": fixturePin },
    fixtureArray: [{ major: 42, abi: "145" }, null],
  };
  const collected = {
    ...fixture,
    officialRelease: { ...fixture.officialRelease, assets: undefined },
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
          integrity: "sha256-fixture",
          assetId: 123,
        },
      },
      officialRelease: { status: 200, id: 321 },
      schemaVersion: 2,
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
    { change: "value type", pins: { schemaVersion: "2" } },
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
      "Candidate written. A separate reviewer must verify IDs/digests/bytes before shipping.",
    );
  });
});
