import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class EventEmitter<T> {
    private readonly listeners = new Set<(value: T) => void>();

    readonly event = (listener: (value: T) => void) => {
      this.listeners.add(listener);
      return { dispose: () => this.listeners.delete(listener) };
    };

    fire(value: T): void {
      for (const listener of this.listeners) listener(value);
    }
  }

  return {
    EventEmitter,
    ThemeIcon: class ThemeIcon {
      constructor(
        readonly id: string,
        readonly color?: { id: string },
      ) {}
    },
    ThemeColor: class ThemeColor {
      constructor(readonly id: string) {}
    },
    TreeItem: class TreeItem {
      constructor(
        public label: string,
        public collapsibleState?: number,
      ) {}
    },
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    DataTransferItem: class DataTransferItem {
      constructor(readonly value: string) {}
    },
    MarkdownString: class MarkdownString {
      constructor(readonly value: string) {}
    },
    Uri: { file: (fsPath: string) => ({ scheme: "file", fsPath }) },
    commands: { executeCommand: vi.fn() },
  };
});

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

async function connectionNode(storageDir: string | undefined, color: string) {
  const manager = {
    getConnections: vi.fn(() => [
      { id: "conn", name: "Colored", type: "pg", color },
    ]),
    isConnected: vi.fn(() => false),
    isConnecting: vi.fn(() => false),
    isSchemaScopeExpanded: vi.fn(() => false),
    onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
    onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
    onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
    onDidChangeSchemaState: vi.fn(() => ({ dispose: vi.fn() })),
    onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
  };
  const { ConnectionProvider } = await import(
    "../../src/extension/providers/connectionProvider"
  );
  const provider = new ConnectionProvider(manager as never, storageDir);
  const [node] = await provider.getChildren();
  return node;
}

describe("connection icon files", () => {
  it.skipIf(process.platform === "win32")(
    "writes icons into private extension storage without following planted symlinks",
    async () => {
      const storage = fs.mkdtempSync(
        path.join(os.tmpdir(), "rapidb-icons-test-"),
      );
      directories.push(storage);
      const victim = path.join(storage, "victim.txt");
      fs.writeFileSync(victim, "original");
      const iconsDir = path.join(storage, "icons");
      fs.mkdirSync(iconsDir);
      const iconPath = path.join(iconsDir, "conn-123456-disconnected.svg");
      fs.symlinkSync(victim, iconPath);

      const node = await connectionNode(storage, "#123456");
      const icon = node.iconPath as { light: { fsPath: string } };

      expect(icon.light.fsPath).toBe(iconPath);
      expect(fs.lstatSync(iconPath).isSymbolicLink()).toBe(false);
      expect(fs.readFileSync(iconPath, "utf8")).toContain("#123456");
      expect(fs.readFileSync(victim, "utf8")).toBe("original");
      expect(fs.readdirSync(iconsDir)).toEqual([
        "conn-123456-disconnected.svg",
      ]);
    },
  );

  it.skipIf(process.platform === "win32")(
    "creates the icon directory as private to the user",
    async () => {
      const storage = fs.mkdtempSync(
        path.join(os.tmpdir(), "rapidb-icons-test-"),
      );
      directories.push(storage);
      await connectionNode(storage, "#abcdef");
      expect(fs.statSync(path.join(storage, "icons")).mode & 0o777).toBe(0o700);
    },
  );

  it("reuses an identical icon file instead of rewriting it in a new session", async () => {
    const storage = fs.mkdtempSync(
      path.join(os.tmpdir(), "rapidb-icons-test-"),
    );
    directories.push(storage);
    const first = (await connectionNode(storage, "#0a0b0c")).iconPath as {
      light: { fsPath: string };
    };
    const before = fs.statSync(first.light.fsPath);
    vi.resetModules();
    const second = (await connectionNode(storage, "#0a0b0c")).iconPath as {
      light: { fsPath: string };
    };
    expect(second.light.fsPath).toBe(first.light.fsPath);
    expect(fs.statSync(first.light.fsPath).ino).toBe(before.ino);
    expect(fs.readdirSync(path.dirname(first.light.fsPath))).toEqual([
      path.basename(first.light.fsPath),
    ]);
  });

  it("does not write to the shared temp directory without extension storage", async () => {
    const before = fs.existsSync(path.join(os.tmpdir(), "rapidb-icons"))
      ? fs.readdirSync(path.join(os.tmpdir(), "rapidb-icons"))
      : [];
    const node = await connectionNode(undefined, "#fedcba");
    expect(node.iconPath).not.toHaveProperty("light");
    const after = fs.existsSync(path.join(os.tmpdir(), "rapidb-icons"))
      ? fs.readdirSync(path.join(os.tmpdir(), "rapidb-icons"))
      : [];
    expect(after).toEqual(before);
  });
});
