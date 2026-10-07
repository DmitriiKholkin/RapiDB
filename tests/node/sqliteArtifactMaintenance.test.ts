import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

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
  // Run the real maintenance logic with a fake fetch transport. No real network
  // or manifest writes are possible in these bootstrap-boundary regressions.
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
