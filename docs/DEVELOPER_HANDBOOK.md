# RapiDB Developer Handbook

This handbook is the maintainer-facing entry point for the repository. It explains how the extension is split up, how requests move through the system, and where to look when something breaks.

For the full file index, see [architecture/file-map.md](architecture/file-map.md). For message and payload shapes, see [reference/contracts.md](reference/contracts.md). For operational tasks, see [handbook/testing.md](handbook/testing.md) and [handbook/packaging.md](handbook/packaging.md).

## What RapiDB Is

RapiDB is a VS Code database client. The runtime is split across three surfaces:

| Surface | Responsibility | Key paths |
|---|---|---|
| Extension host | Owns commands, tree views, connection state, DB drivers, panel controllers, and data services | [src/extension](../src/extension) |
| Webview UI | Renders query, table, ERD, and connection-form experiences in React | [src/webview](../src/webview) |
| Shared contracts | Defines the message envelopes, initial panel state, and cross-runtime types | [src/shared](../src/shared) |

The browser build is a degraded fallback and should be documented as such, not treated as a second full runtime.

## How The Repo Is Organized

Use the following mental model when navigating the code:

| Area | What it owns | What to inspect first |
|---|---|---|
| Activation and registration | Command registration, tree views, and panel entry points | [src/extension/extension.ts](../src/extension/extension.ts) |
| Connection lifecycle | Saved connections, folder hierarchy, secrets, and validation | [src/extension/connectionManager.ts](../src/extension/connectionManager.ts), [src/extension/connectionManagerStore.ts](../src/extension/connectionManagerStore.ts), [src/extension/connectionSecrets.ts](../src/extension/connectionSecrets.ts) |
| Panels | Query, table, ERD, and connection-form window logic | [src/extension/panels](../src/extension/panels) |
| Driver layer | Engine-specific query, metadata, preview, and timeout behavior | [src/extension/dbDrivers](../src/extension/dbDrivers) |
| Table service | Read, filter, mutate, export, and preview table data | [src/extension/tableDataService.ts](../src/extension/tableDataService.ts), [src/extension/table](../src/extension/table) |
| Webview bootstrap | React entry point, state store, and message bridge | [src/webview/main.tsx](../src/webview/main.tsx), [src/webview/store](../src/webview/store), [src/webview/utils/messaging.ts](../src/webview/utils/messaging.ts) |
| Tests | Unit, UI, integration, and live DB coverage | [tests](../tests) |

## Runtime Boundaries

The host side decides what the app can do. The webview side renders whatever state the host sends it. Shared types are the contract between them, and tests in [tests/node](../tests/node) and [tests/webview](../tests/webview) are the fastest way to validate a change.

### Common Flow

1. VS Code activates the extension.
2. The host registers commands, views, and providers.
3. A panel opens or a tree command fires.
4. The host loads connection or driver data.
5. The host sends an initial state payload to the webview.
6. The webview renders and posts user actions back to the host.
7. The host resolves the action through the relevant driver or service.

## What To Check When Fixing Bugs

| Symptom | First place to look |
|---|---|
| A command is missing or inactive | [src/extension/extension.ts](../src/extension/extension.ts) and [package.json](../package.json) |
| A panel opens blank | [src/shared/webviewContracts.ts](../src/shared/webviewContracts.ts), [src/webview/main.tsx](../src/webview/main.tsx), and the matching panel controller |
| Connection state is wrong | [src/extension/connectionManager.ts](../src/extension/connectionManager.ts) and [src/extension/connectionManagerStore.ts](../src/extension/connectionManagerStore.ts) |
| Query results are truncated or filtered unexpectedly | [src/extension/tableDataService.ts](../src/extension/tableDataService.ts), [src/extension/utils/queryResultFormatting.ts](../src/extension/utils/queryResultFormatting.ts), and [src/shared/safetyContracts.ts](../src/shared/safetyContracts.ts) |
| Table edits fail or partially apply | [src/extension/table](../src/extension/table) and [src/webview/components/table](../src/webview/components/table) |
| ERD layout or edges are wrong | [src/extension/services/erdGraphService.ts](../src/extension/services/erdGraphService.ts) and [src/webview/components/ErdView.tsx](../src/webview/components/ErdView.tsx) |
| Browser mode is missing features | [src/browser/extension.ts](../src/browser/extension.ts) |

## Version And Tooling Snapshot

| Tool | Value |
|---|---|
| VS Code engine | ^1.90.0 |
| Node engine | >=20.0.0 |
| TypeScript | 6.0.3 |
| esbuild | 0.28.0 |
| Biome | 2.4.16 |
| Vitest | 4.1.7 |
| React | 19.2.6 |

Keep this file aligned with the code when a change affects architecture, support boundaries, or the maintainer workflow.
