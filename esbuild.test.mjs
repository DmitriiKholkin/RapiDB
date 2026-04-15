import * as esbuild from "esbuild";

/** @type {import('esbuild').BuildOptions} */
const testConfig = {
  entryPoints: ["test/db-smoke.ts"],
  bundle: true,
  outfile: "dist/test/db-smoke.cjs",
  format: "cjs",
  platform: "node",
  target: "node20",
  sourcemap: true,
  sourcesContent: true,
  minify: false,
  logLevel: "info",
  external: ["pg", "mysql2", "mssql", "oracledb", "vscode"],
};

esbuild.build(testConfig).catch((err) => {
  console.error(err);
  process.exit(1);
});
