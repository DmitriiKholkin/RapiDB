import { pathToFileURL } from "node:url";
import * as esbuild from "esbuild";

const isWatch = process.argv.includes("--watch");
const isProduction = process.argv.includes("--production");

const defineNodeEnv = {
  "process.env.NODE_ENV": isProduction ? '"production"' : '"development"',
};

const baseConfig = {
  bundle: true,
  sourcesContent: true,
  minify: isProduction,
  logLevel: "info",
};

const browserBaseConfig = {
  ...baseConfig,
  platform: "browser",
  target: ["chrome120"],
  define: defineNodeEnv,
};

export const extensionConfig = {
  ...baseConfig,
  entryPoints: ["src/extension/extension.ts"],
  outfile: "dist/extension.js",
  external: ["vscode", "oracledb", "better-sqlite3"],
  format: "cjs",
  platform: "node",
  target: "node20",
  sourcemap: isProduction ? false : "inline",
};

export const browserExtensionConfig = {
  ...browserBaseConfig,
  entryPoints: ["src/browser/extension.ts"],
  outfile: "dist/extension-web.js",
  external: ["vscode"],
  format: "cjs",
  sourcemap: isProduction ? false : "inline",
};

export const webviewConfig = {
  ...browserBaseConfig,
  entryPoints: ["src/webview/main.tsx"],
  outfile: "dist/webview.js",
  external: [],
  format: "iife",
  sourcemap: isProduction ? false : "external",
  loader: {
    ".tsx": "tsx",
    ".ts": "ts",
    ".svg": "dataurl",
    ".png": "dataurl",
    ".css": "css",
    ".ttf": "dataurl",
    ".woff": "dataurl",
    ".woff2": "dataurl",
    ".eot": "dataurl",
  },
};

export async function build() {
  if (isWatch) {
    console.log("⚡ RapiDB — watch mode (desktop + browser + webview)");
    const [extCtx, browserCtx, wvCtx] = await Promise.all([
      esbuild.context(extensionConfig),
      esbuild.context(browserExtensionConfig),
      esbuild.context(webviewConfig),
    ]);
    await Promise.all([extCtx.watch(), browserCtx.watch(), wvCtx.watch()]);
    console.log("👀 Watching for changes...");
  } else {
    console.log(
      `🔨 RapiDB — building (${isProduction ? "production" : "development"})`,
    );
    await Promise.all([
      esbuild.build(extensionConfig),
      esbuild.build(browserExtensionConfig),
      esbuild.build(webviewConfig),
    ]);
    console.log("✅ Build complete → dist/");
  }
}

const isDirectRun =
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectRun) {
  build().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
