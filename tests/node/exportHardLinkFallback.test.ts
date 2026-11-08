import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const chunks = async function* () {
  yield {
    columns: [{ name: "id", category: "integer" as const, nativeType: "int" }],
    rows: [{ id: 1 }],
  };
};

function unsupportedLinkError(code: string): NodeJS.ErrnoException {
  return Object.assign(new Error(`${code}: operation not permitted, link`), {
    code,
  });
}

describe("export publishing without hard links", () => {
  let tempDir: string;
  let outputPath: string;
  let showErrorMessage: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.resetModules();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rapidb-export-g9-"));
    outputPath = path.join(tempDir, "export.out");
    showErrorMessage = vi.fn();
    vi.doMock("vscode", () => ({
      Uri: { file: (fsPath: string) => ({ fsPath }) },
      ProgressLocation: { Notification: 15 },
      window: {
        showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
        withProgress: vi.fn(async (_options, task) =>
          task(
            {},
            { onCancellationRequested: vi.fn(() => ({ dispose: vi.fn() })) },
          ),
        ),
        showInformationMessage: vi.fn(),
        showErrorMessage,
      },
    }));
  });

  afterEach(() => {
    vi.doUnmock("node:fs");
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  async function exportWithLink(linkSync: typeof fs.linkSync): Promise<void> {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return { ...actual, linkSync };
    });
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({ fileName: "no_links", loadChunks: chunks });
  }

  it.each([
    "EPERM",
    "ENOTSUP",
    "EXDEV",
    "ENOSYS",
  ])("copies a new export exclusively when hard links fail with %s", async (code) => {
    await exportWithLink(
      vi.fn(() => {
        throw unsupportedLinkError(code);
      }) as never,
    );

    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
    expect(fs.statSync(outputPath).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
  });

  it("does not overwrite a file that appears before the copy fallback publishes", async () => {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        linkSync: vi.fn(() => {
          throw unsupportedLinkError("EPERM");
        }),
      };
    });
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "raced",
      loadChunks: async function* () {
        fs.writeFileSync(outputPath, "competing file", { flag: "wx" });
        yield* chunks();
      },
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe("competing file");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(showErrorMessage).toHaveBeenCalled();
  });

  it("removes a partially copied export when the copy fallback fails", async () => {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        linkSync: vi.fn(() => {
          throw unsupportedLinkError("EPERM");
        }),
        copyFileSync: vi.fn((_source: unknown, destination: unknown) => {
          actual.writeFileSync(String(destination), "id\n", { flag: "wx" });
          throw Object.assign(new Error("ENOSPC: no space left on device"), {
            code: "ENOSPC",
          });
        }),
      };
    });
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({ fileName: "partial", loadChunks: chunks });

    expect(fs.existsSync(outputPath)).toBe(false);
    expect(fs.readdirSync(tempDir)).toEqual([]);
    expect(showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("ENOSPC"),
    );
  });

  it("keeps failing for hard-link errors that are not unsupported-filesystem errors", async () => {
    await exportWithLink(
      vi.fn(() => {
        throw Object.assign(new Error("EIO: i/o error, link"), { code: "EIO" });
      }) as never,
    );

    expect(fs.existsSync(outputPath)).toBe(false);
    expect(fs.readdirSync(tempDir)).toEqual([]);
    expect(showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("EIO"),
    );
  });
});
