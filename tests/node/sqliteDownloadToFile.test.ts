import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { downloadToFile } from "../../src/extension/utils/sqliteInstaller";

let servers: Array<ReturnType<typeof createServer>> = [];
let dirs: string[] = [];

afterEach(async () => {
  for (const s of servers) {
    await new Promise<void>((resolve) => s.close(() => resolve()));
  }
  servers = [];
  for (const d of dirs) {
    rmSync(d, { recursive: true, force: true });
  }
  dirs = [];
});

function start(handler: (req: never, res: never) => void) {
  const server = createServer(handler as never);
  servers.push(server);
  return new Promise<string>((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      resolve(`http://127.0.0.1:${port}`);
    });
  });
}

function tempFile() {
  const dir = mkdtempSync(join(tmpdir(), "rapidb-dl-"));
  dirs.push(dir);
  return join(dir, "out.bin");
}

describe("downloadToFile (stage 4)", () => {
  it("writes 200 body to file", async () => {
    const base = await start((_req, res) => {
      (res as unknown as { end: (b: string) => void }).end("hello");
    });
    const dest = tempFile();
    await downloadToFile(`${base}/file`, dest);
    expect(readFileSync(dest, "utf8")).toBe("hello");
  });

  it("follows 303 and 307 redirects", async () => {
    const base = await start((req, res) => {
      const r = req as unknown as { url?: string };
      const w = res as unknown as {
        writeHead: (s: number, h: Record<string, string>) => void;
        end: (b?: string) => void;
      };
      if (r.url === "/r303") {
        w.writeHead(303, { location: `${base}/target` });
        w.end();
        return;
      }
      if (r.url === "/r307") {
        w.writeHead(307, { location: `${base}/target` });
        w.end();
        return;
      }
      w.end("target-body");
    });
    for (const path of ["/r303", "/r307"]) {
      const dest = tempFile();
      await downloadToFile(`${base}${path}`, dest);
      expect(readFileSync(dest, "utf8")).toBe("target-body");
    }
  });

  it("follows 301/302/308 including relative locations", async () => {
    const base = await start((req, res) => {
      const r = req as unknown as { url?: string };
      const w = res as unknown as {
        writeHead: (s: number, h: Record<string, string>) => void;
        end: (b?: string) => void;
      };
      if (r.url === "/r301") {
        w.writeHead(301, { location: `${base}/target` });
        w.end();
        return;
      }
      if (r.url === "/r302rel") {
        w.writeHead(302, { location: "/target" });
        w.end();
        return;
      }
      if (r.url === "/r308") {
        w.writeHead(308, { location: `${base}/target` });
        w.end();
        return;
      }
      w.end("target-body");
    });
    for (const path of ["/r301", "/r302rel", "/r308"]) {
      const dest = tempFile();
      await downloadToFile(`${base}${path}`, dest);
      expect(readFileSync(dest, "utf8")).toBe("target-body");
    }
  });

  it("refuses non-loopback http URLs", async () => {
    const dest = tempFile();
    await expect(
      downloadToFile("http://169.254.169.254/latest/meta-data/", dest),
    ).rejects.toThrow("non-loopback");
    expect(existsSync(dest)).toBe(false);
  });

  it("rejects on non-200 and leaves no file", async () => {
    const base = await start((_req, res) => {
      const w = res as unknown as {
        writeHead: (s: number) => void;
        end: (b?: string) => void;
      };
      w.writeHead(500);
      w.end("boom");
    });
    const dest = tempFile();
    await expect(downloadToFile(`${base}/x`, dest)).rejects.toThrow("HTTP 500");
    expect(existsSync(dest)).toBe(false);
  });

  it("rejects (no hang) when response stream errors mid-body", async () => {
    const base = await start((_req, res) => {
      const w = res as unknown as {
        write: (b: string) => void;
        destroy: (e: Error) => void;
      };
      w.write("partial-");
      setTimeout(() => w.destroy(new Error("socket boom")), 10);
    });
    const dest = tempFile();
    await expect(downloadToFile(`${base}/flaky`, dest)).rejects.toThrow();
    expect(existsSync(dest)).toBe(false);
  });

  it("rejects after too many redirects", async () => {
    const base = await start((_req, res) => {
      const w = res as unknown as {
        writeHead: (s: number, h: Record<string, string>) => void;
        end: () => void;
      };
      w.writeHead(302, { location: `${base}/loop` });
      w.end();
    });
    const dest = tempFile();
    await expect(downloadToFile(`${base}/loop`, dest)).rejects.toThrow(
      "Too many redirects",
    );
  });

  it("enforces an overall deadline even when the server keeps sending bytes", async () => {
    const base = await start((_req, res) => {
      const response = res as unknown as {
        write: (data: string) => void;
        on: (event: string, callback: () => void) => void;
      };
      response.write("start");
      const interval = setInterval(() => response.write("."), 10);
      response.on("close", () => clearInterval(interval));
    });
    const dest = tempFile();
    await expect(downloadToFile(`${base}/drip`, dest, 120)).rejects.toThrow(
      "Timed out downloading",
    );
    expect(existsSync(dest)).toBe(false);
  });
});
