import {
  resolveRequestedComposeBackedEngines,
  seedComposeBackedFixtures,
} from "../runtime/liveDbOrchestration.ts";

try {
  const engines = resolveRequestedComposeBackedEngines(process.argv.slice(2));
  await seedComposeBackedFixtures(engines);
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
