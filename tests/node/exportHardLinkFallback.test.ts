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
      workspace: { getConfiguration: () => ({ get: () => false }) },
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

  async function exportWithLink(
    linkSync: typeof fs.linkSync,
    copyFileSync = fs.copyFileSync,
  ): Promise<void> {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return { ...actual, linkSync, copyFileSync };
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
  ])("copies a new export exclusively when the hard-link probe fails with %s", async (code) => {
    const linkSync = vi.fn<typeof fs.linkSync>(() => {
      throw unsupportedLinkError(code);
    });
    const copyFileSync = vi.fn(fs.copyFileSync);
    await exportWithLink(linkSync, copyFileSync);

    expect(linkSync).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/\.tmp$/),
      expect.stringMatching(/\.probe$/),
    );
    expect(copyFileSync).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/\.tmp$/),
      outputPath,
      fs.constants.COPYFILE_EXCL,
    );

    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
    expect(fs.statSync(outputPath).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
  });

  it("copies a new export exclusively when the probe succeeds but the final link fails with ENOTSUP", async () => {
    const linkSync = vi.fn<typeof fs.linkSync>((source, destination) => {
      if (destination === outputPath) throw unsupportedLinkError("ENOTSUP");
      fs.linkSync(source, destination);
    });
    const copyFileSync = vi.fn<typeof fs.copyFileSync>((...args) => {
      const [source, destination] = args;
      expect(fs.readFileSync(source, "utf8")).toBe("id\n1\n");
      expect(fs.existsSync(destination)).toBe(false);
      fs.copyFileSync(...args);
    });
    await exportWithLink(linkSync, copyFileSync);

    expect(linkSync).toHaveBeenCalledTimes(2);
    expect(linkSync).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/\.tmp$/),
      expect.stringMatching(/\.probe$/),
    );
    expect(linkSync).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(/\.tmp$/),
      outputPath,
    );
    expect(copyFileSync).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/\.tmp$/),
      outputPath,
      fs.constants.COPYFILE_EXCL,
    );
    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
  });

  it("falls back to a copy for a new export with a 255-byte destination basename", async () => {
    const basename = `${"表".repeat(10)}${"a".repeat(221)}.csv`;
    outputPath = path.join(tempDir, basename);
    const linkSync = vi.fn<typeof fs.linkSync>(() => {
      throw unsupportedLinkError("ENOTSUP");
    });
    await exportWithLink(linkSync);

    expect(showErrorMessage).not.toHaveBeenCalled();
    expect(linkSync).toHaveBeenCalledOnce();
    expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
    expect(fs.readdirSync(tempDir)).toEqual([basename]);
  });

  it.each([
    "probe",
    "final link",
  ])("does not overwrite a file that appears as the copy fallback publishes after an unsupported %s", async (phase) => {
    const linkSync = vi.fn<typeof fs.linkSync>((source, destination) => {
      if (phase === "probe" || destination === outputPath) {
        throw unsupportedLinkError("EPERM");
      }
      fs.linkSync(source, destination);
    });
    const copyFileSync = vi.fn<typeof fs.copyFileSync>((...args) => {
      fs.writeFileSync(outputPath, "competing file", { flag: "wx" });
      fs.copyFileSync(...args);
    });
    await exportWithLink(linkSync, copyFileSync);

    expect(linkSync).toHaveBeenCalledTimes(phase === "probe" ? 1 : 2);
    expect(copyFileSync).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(/\.tmp$/),
      outputPath,
      fs.constants.COPYFILE_EXCL,
    );
    expect(fs.readFileSync(outputPath, "utf8")).toBe("competing file");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("EEXIST"),
    );
  });

  it("does not remove a newer file after the copy fallback fails", async () => {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        linkSync: vi.fn(() => {
          throw unsupportedLinkError("EPERM");
        }),
        copyFileSync: vi.fn((_source: unknown, destination: unknown) => {
          const filePath = String(destination);
          actual.writeFileSync(filePath, "id\n", { flag: "wx" });
          // Simulate runtime cleanup of a partial copy, then another save.
          actual.unlinkSync(filePath);
          actual.writeFileSync(filePath, "another export", { flag: "wx" });
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

    expect(fs.readFileSync(outputPath, "utf8")).toBe("another export");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("ENOSPC"),
    );
  });

  it("does not fall back to copying on an unexpected final hard-link EIO", async () => {
    const linkSync = vi.fn<typeof fs.linkSync>((source, destination) => {
      if (destination === outputPath) {
        throw Object.assign(new Error("EIO: i/o error, link"), { code: "EIO" });
      }
      fs.linkSync(source, destination);
    });
    const copyFileSync = vi.fn(fs.copyFileSync);
    await exportWithLink(linkSync, copyFileSync);

    expect(linkSync).toHaveBeenCalledTimes(2);
    expect(copyFileSync).not.toHaveBeenCalled();
    expect(fs.existsSync(outputPath)).toBe(false);
    expect(fs.readdirSync(tempDir)).toEqual([]);
    expect(showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("EIO"),
    );
  });
});
