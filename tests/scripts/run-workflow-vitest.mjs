import { spawnNpm } from "./npm-command.mjs";

const engine = process.argv[2];

if (!engine || !/^[a-z]+$/.test(engine)) {
  console.error(
    "[RapiDB:test] Usage: node ./tests/scripts/run-workflow-vitest.mjs <engine> [vitest args...]",
  );
  process.exit(1);
}

const child = spawnNpm(
  [
    "exec",
    "--",
    "vitest",
    "run",
    "--project",
    "db-workflow",
    "-t",
    engine,
    ...process.argv.slice(3),
  ],
  {
    stdio: "inherit",
    env: {
      ...process.env,
      RAPIDB_WORKFLOW_ENGINES: engine,
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
