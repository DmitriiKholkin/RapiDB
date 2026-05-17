import { spawn } from "node:child_process";

const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";

const child = spawn(
  npmCommand,
  [
    "exec",
    "--",
    "vitest",
    "run",
    "--project",
    "unit-node",
    "tests/node/nosqlLive.test.ts",
  ],
  {
    stdio: "inherit",
    env: {
      ...process.env,
      RAPIDB_LIVE_NOSQL: "1",
    },
  },
);

child.on("close", (code) => {
  process.exit(code ?? 1);
});

child.on("error", (error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
