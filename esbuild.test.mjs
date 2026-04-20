import * as esbuild from "esbuild";

/** @type {import('esbuild').BuildOptions} */
const sharedConfig = {
  bundle: true,
  format: "cjs",
  platform: "node",
  target: "node20",
  sourcemap: true,
  sourcesContent: true,
  minify: false,
  logLevel: "info",
  external: [
    "pg",
    "mysql2",
    "mssql",
    "oracledb",
    "node-sqlite3-wasm",
    "vscode",
  ],
};

const builds = [
  {
    entryPoints: ["test/db-preflight.ts"],
    outfile: "dist/test/db-preflight.cjs",
  },
  {
    entryPoints: ["test/db-smoke.ts"],
    outfile: "dist/test/db-smoke.cjs",
  },
];

for (const build of builds) {
  await esbuild.build({ ...sharedConfig, ...build });
}
