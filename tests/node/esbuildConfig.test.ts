import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

function readEsbuildConfigSource(): string {
  return readFileSync(
    new URL("../../esbuild.config.mjs", import.meta.url),
    "utf8",
  );
}

describe("esbuild config", () => {
  it("keeps the extension bundle configured for the VS Code host runtime", () => {
    const source = readEsbuildConfigSource();

    expect(source).toContain('entryPoints: ["src/extension/extension.ts"]');
    expect(source).toContain('platform: "node"');
    expect(source).toContain('target: "node20"');
    expect(source).toContain('format: "cjs"');
    expect(source).toContain(
      'external: ["vscode", "oracledb", "better-sqlite3"]',
    );
  });

  it("keeps browser bundles isolated and the build script side-effect free on import", () => {
    const source = readEsbuildConfigSource();

    expect(source).toContain('entryPoints: ["src/browser/extension.ts"]');
    expect(source).toContain('entryPoints: ["src/webview/main.tsx"]');
    expect(source).toContain('target: ["chrome120"]');
    expect(source).toContain('external: ["vscode"]');
    expect(source).toContain('format: "iife"');
    expect(source).toContain("const isDirectRun =");
    expect(source).toContain("if (isDirectRun) {");
  });
});
