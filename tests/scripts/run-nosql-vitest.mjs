import { spawnNpm } from "./npm-command.mjs";

const child = spawnNpm(
  [
    "exec",
    "--",
    "vitest",
    "run",
    "--project",
    "unit-node",
    "--no-file-parallelism",
    "tests/node/nosqlLive.test.ts",
    "tests/node/mutationDataSafetyLive.test.ts",
    "tests/node/dynamodbNativeSafety.test.ts",
    "tests/node/queryTargetSafetyLive.test.ts",
    "tests/node/mongodbMongoshRegexSafety.test.ts",
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
