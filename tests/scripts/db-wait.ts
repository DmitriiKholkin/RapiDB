import {
  resolveRequestedComposeBackedEngines,
  waitForComposeBackedDatabases,
} from "../runtime/liveDbOrchestration.ts";

async function main() {
  const engines = resolveRequestedComposeBackedEngines(process.argv.slice(2));
  await waitForComposeBackedDatabases(engines);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
