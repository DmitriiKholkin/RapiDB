import * as esbuild from "esbuild";
const isWatch = process.argv.includes("--watch");
const isProduction = process.argv.includes("--production");
const extensionConfig = {
  entryPoints: ["src/extension/extension.ts"],
  bundle: true,
  outfile: "dist/extension.js",
  external: ["vscode", "oracledb", "node-sqlite3-wasm"],
  format: "cjs",
  platform: "node",
  target: "node20",
  sourcemap: isProduction ? false : "inline",
  sourcesContent: true,
  minify: isProduction,
  logLevel: "info",
};
const webviewConfig = {
  entryPoints: ["src/webview/main.tsx"],
  bundle: true,
  outfile: "dist/webview.js",
  external: [],
  format: "iife",
  platform: "browser",
  target: ["chrome120"],
  sourcemap: isProduction ? false : "inline",
  sourcesContent: true,
  minify: isProduction,
  logLevel: "info",
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
  define: {
    "process.env.NODE_ENV": isProduction ? '"production"' : '"development"',
  },
};
async function build() {
  if (isWatch) {
    console.log("⚡ RapiDB — watch mode (extension + webview)");
    const [extCtx, wvCtx] = await Promise.all([
      esbuild.context(extensionConfig),
      esbuild.context(webviewConfig),
    ]);
    await Promise.all([extCtx.watch(), wvCtx.watch()]);
    console.log("👀 Watching for changes...");
  } else {
    console.log(
      `🔨 RapiDB — building (${isProduction ? "production" : "development"})`,
    );
    await Promise.all([
      esbuild.build(extensionConfig),
      esbuild.build(webviewConfig),
    ]);
    console.log("✅ Build complete → dist/");
  }
}
build().catch((err) => {
  console.error(err);
  process.exit(1);
});
