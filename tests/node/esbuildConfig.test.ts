import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { runInNewContext } from "node:vm";
import { build } from "esbuild";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { extensionConfig, webviewConfig } from "../../esbuild.config.mjs";

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
    expect(extensionConfig.external).toEqual([
      "vscode",
      "oracledb",
      "better-sqlite3",
      "ssh2",
    ]);
  });

  it("keeps the webview bundle isolated and the build script side-effect free on import", () => {
    const source = readEsbuildConfigSource();

    expect(source).toContain('entryPoints: ["src/webview/main.tsx"]');
    expect(source).toContain('target: ["chrome120"]');
    expect(source).toContain('format: "iife"');
    expect(source).toContain("const isDirectRun =");
    expect(source).toContain("if (isDirectRun) {");
  });

  it.each([
    false,
    true,
  ])("renders bundled TableView without global React (minify: %s)", async (minify) => {
    const result = await build({
      ...webviewConfig,
      entryPoints: ["src/webview/components/TableView.tsx"],
      format: "cjs",
      external: ["react", "react/*", "react-dom", "react-dom/*"],
      alias: { "monaco-editor": "./tests/mocks/monaco-editor.ts" },
      minify,
      define: {
        "process.env.NODE_ENV": minify ? '"production"' : '"development"',
      },
      sourcemap: false,
      write: false,
      logLevel: "silent",
    });
    const bundledModule = {
      exports: {} as typeof import("../../src/webview/components/TableView"),
    };
    runInNewContext(result.outputFiles[0].text, {
      module: bundledModule,
      exports: bundledModule.exports,
      require: createRequire(import.meta.url),
      // Messaging registers a listener on import; SSR does not run effects.
      window: new EventTarget(),
      self: {},
    });

    const markup = renderToStaticMarkup(
      createElement(bundledModule.exports.TableView, {
        connectionId: "test",
        database: "test",
        schema: "public",
        table: "users",
      }),
    );

    expect(markup).toContain('aria-label="Table data for users"');
    expect(markup).toContain("Loading data...");
  });
});
