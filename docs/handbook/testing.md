# Testing Handbook

RapiDB uses a multi-project Vitest setup plus Docker-backed live database suites. The test shape is important because many bugs only show up at a runtime boundary.

## Test Layers

| Layer | Command | What it covers |
|---|---|---|
| Full validation | `npm run test` | Runs the entire test pipeline through `test:all`. |
| Unit and static coverage | `npm run test:unit` | Node-side logic without live DB infrastructure. |
| Webview UI | `npm run test:webview` | React components, messaging helpers, and UI interactions. |
| Extension host | `npm run test:extension` | Activation, panels, providers, and host orchestration. |
| SQLite live DB | `npm run test:db:sqlite` | Live relational behavior against SQLite. |
| PostgreSQL live DB | `npm run test:db:postgres` | Live relational behavior against PostgreSQL. |
| MySQL live DB | `npm run test:db:mysql` | Live relational behavior against MySQL. |
| MSSQL live DB | `npm run test:db:mssql` | Live relational behavior against SQL Server. |
| Oracle live DB | `npm run test:db:oracle` | Live relational behavior against Oracle. |
| NoSQL live DBs | `npm run test:db:nosql` | Redis, MongoDB, Elasticsearch, and DynamoDB live checks. |
| Workflow full journey | `npm run test:workflow` | Per-engine bridge test that runs the real extension panels + webview against a real driver. Manual trigger only. |

## Workflow Full-Journey Tests

`npm run test:workflow` runs a script that brings the docker DBs up, prepares
fixtures, runs the `db-workflow` vitest project, then tears the services down.
The suite is opt-in (no CI trigger) because it spins up every supported
database container and is sensitive to the host environment.

What the workflow tests cover, per engine, in a single test:

1. Open the connection form panel, then dispose it (the form path is
   exercised symbolically — the connection is set up via the fake store so
   the rest of the flow does not depend on the form submission).
2. Connect the saved connection via the real `ConnectionManager`.
3. Open the query editor panel and dispose it (the editor UI is fully
   covered by `tests/webview/queryView.test.tsx`; the bridge test only
   confirms the panel handshake).
4. Seed entities through the real driver (DDL, inserts).
5. Refresh the schema cache.
6. Open the table data viewer for the seeded table.
7. Resize, reorder, and hide a column.
8. Open the ERD viewer.
9. Clean up entities through the driver.
10. Delete the connection.

Per-engine scripts are also available when you only want to run a subset:

| Command | Engines |
|---|---|
| `npm run test:workflow:sqlite` | sqlite only (no docker required) |
| `npm run test:workflow:postgres` | postgres only |
| `npm run test:workflow:mysql` | mysql only |
| `npm run test:workflow:mssql` | mssql only |
| `npm run test:workflow:oracle` | oracle only |
| `npm run test:workflow:mongodb` | mongodb only |
| `npm run test:workflow:redis` | redis only |
| `npm run test:workflow:elasticsearch` | elasticsearch only |
| `npm run test:workflow:dynamodb` | dynamodb only |

`RAPIDB_KEEP_TEST_SERVICES=1` keeps the docker containers running after the
test finishes so you can poke at them.

The bridge lives in `tests/workflow/`. The setup file
`tests/workflow/setup/workflow.setup.ts` installs the `vscode`, Monaco, and
react-virtual mocks that every workflow test relies on.

## Supporting Commands

| Command | Purpose |
|---|---|
| `npm run check` | Type-check plus Biome lint. |
| `npm run typecheck` | Type-check extension, webview, and tests. |
| `npm run lint` | Biome lint only. |
| `npm run build` | Bundle the extension and webview. |
| `npm run db:up` | Start the Docker-based test services. |
| `npm run db:prepare` | Wait for, reset, and seed the live DBs. |
| `npm run db:down` | Stop the Docker-based test services. |

## Live DB Topology

| Service | Defined by | Used for |
|---|---|---|
| ssh-bastion | [compose.yaml](../../compose.yaml) | SSH tunnel and bastion scenarios. |
| postgres | [compose.yaml](../../compose.yaml) | PostgreSQL live tests. |
| mysql | [compose.yaml](../../compose.yaml) | MySQL live tests. |
| mssql | [compose.yaml](../../compose.yaml) | MSSQL live tests. |
| oracle | [compose.yaml](../../compose.yaml) | Oracle live tests. |
| redis | [compose.yaml](../../compose.yaml) | Redis live tests. |
| mongo | [compose.yaml](../../compose.yaml) | MongoDB live tests. |
| elasticsearch | [compose.yaml](../../compose.yaml) | Elasticsearch live tests. |
| dynamodb | [compose.yaml](../../compose.yaml) | DynamoDB live tests. |

## Recommended Debug Order

1. Reproduce the issue with the narrowest test project that can fail.
2. Run the relevant unit or node test first.
3. If the bug crosses a process boundary, run the webview or extension-host slice.
4. If the bug depends on a live engine, start the matching Docker service and seed data.
5. Only then widen to `npm run test` or the full DB matrix.

## Useful Test Sources

| File | Why it matters |
|---|---|
| [tests/contracts/testingContracts.ts](../../tests/contracts/testingContracts.ts) | Shared engine capability expectations. |
| [tests/setup](../../tests/setup) | Test environment bootstrap. |
| [tests/runtime](../../tests/runtime) | Live DB orchestration helpers. |
| [tests/support](../../tests/support) | Fakes, mocks, and harnesses. |
