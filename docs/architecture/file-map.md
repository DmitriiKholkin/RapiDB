# File Map

This page explains where the important pieces of the repository live and what each area owns.

## Root Files

| Path | Responsibility |
|---|---|
| [package.json](../../package.json) | Runtime metadata, contributed commands and views, scripts, dependencies, and packaging entry points. |
| [README.md](../../README.md) | Product-facing marketplace README. Keep it concise and user-oriented. |
| [biome.json](../../biome.json) | Formatting and linting policy. |
| [esbuild.config.mjs](../../esbuild.config.mjs) | Bundles the extension host and webview assets. |
| [compose.yaml](../../compose.yaml) | Docker Compose services used by live DB tests. |
| [vitest.config.ts](../../vitest.config.ts) and [vitest.workspace.ts](../../vitest.workspace.ts) | Test project wiring. |
| [tsconfig.extension.json](../../tsconfig.extension.json), [tsconfig.webview.json](../../tsconfig.webview.json), [tsconfig.tests.json](../../tsconfig.tests.json) | Type-checking boundaries per runtime. |
| [scripts](../../scripts) | Package and native runtime preparation helpers. |
| [tests](../../tests) | All unit, UI, integration, and live-database tests. |

## src/extension

| Sub-area | Key files | What it owns |
|---|---|---|
| Activation and orchestration | [extension.ts](../../src/extension/extension.ts), [connectionManager.ts](../../src/extension/connectionManager.ts), [connectionManagerModels.ts](../../src/extension/connectionManagerModels.ts), [connectionManagerStore.ts](../../src/extension/connectionManagerStore.ts), [connectionSecrets.ts](../../src/extension/connectionSecrets.ts) | Activation, command wiring, persisted connections, and secret handling. |
| Panels | [panels](../../src/extension/panels) | Query, table, ERD, and connection-form panel controllers and lifecycles. |
| Providers | [providers](../../src/extension/providers) | Explorer tree data for connections, history, bookmarks, and SQL entry nodes. |
| DB drivers | [dbDrivers](../../src/extension/dbDrivers) | Engine-specific query, metadata, DDL, and timeout behavior. |
| Table pipeline | [tableDataService.ts](../../src/extension/tableDataService.ts), [table](../../src/extension/table) | Read, filter, mutate, preview, and export table data. |
| Services | [services](../../src/extension/services) | Connection validation, SSH runtime, and ERD graph assembly. |
| Utilities | [utils](../../src/extension/utils) | Export formatting, mutation preview helpers, concurrency, safety guards, and error handling. |

### Notable files under src/extension

| File | Responsibility |
|---|---|
| [driverRuntimeConfig.ts](../../src/extension/driverRuntimeConfig.ts) | Driver runtime settings and feature toggles. |
| [connectionManagerPrompts.ts](../../src/extension/connectionManagerPrompts.ts) | UI prompts related to connection management. |
| [tableDataService.ts](../../src/extension/tableDataService.ts) | Table reads and the orchestration around them. |
| [services/erdGraphService.ts](../../src/extension/services/erdGraphService.ts) | ERD graph construction. |
| [services/connectionValidationService.ts](../../src/extension/services/connectionValidationService.ts) | Connection validation and health checks. |
| [table/tableReadService.ts](../../src/extension/table/tableReadService.ts) | Read pipeline for table data. |
| [table/tableMutationService.ts](../../src/extension/table/tableMutationService.ts) | Mutation execution and preview logic. |

## src/shared

| File | Responsibility |
|---|---|
| [webviewContracts.ts](../../src/shared/webviewContracts.ts) | Message envelopes, initial states, and cross-runtime payloads. |
| [connectionConfig.ts](../../src/shared/connectionConfig.ts) | Connection shape and normalization. |
| [connectionTypes.ts](../../src/shared/connectionTypes.ts) | Connection type enums and identifiers. |
| [connectionValidation.ts](../../src/shared/connectionValidation.ts) | Shared validation logic. |
| [dbObjectKinds.ts](../../src/shared/dbObjectKinds.ts) | Explorer node/object kinds. |
| [tableTypes.ts](../../src/shared/tableTypes.ts) | Table column metadata, filters, categories, and type inference helpers. |
| [safetyContracts.ts](../../src/shared/safetyContracts.ts) | Query safety policies, cancellation, and secret update transaction metadata. |
| [dynamodbNative.ts](../../src/shared/dynamodbNative.ts) | DynamoDB-specific shared helpers. |

## src/webview

| Sub-area | Key files | What it owns |
|---|---|---|
| Bootstrap | [main.tsx](../../src/webview/main.tsx), [types.ts](../../src/webview/types.ts), [vscode.d.ts](../../src/webview/vscode.d.ts) | Webview startup and VS Code messaging types. |
| App shell | [components/App.tsx](../../src/webview/components/App.tsx), [components/ErrorBoundary.tsx](../../src/webview/components/ErrorBoundary.tsx) | Top-level routing and error containment. |
| Query UI | [components/QueryView.tsx](../../src/webview/components/QueryView.tsx), [components/query](../../src/webview/components/query) | Query editor, toolbar, helpers, and controller hooks. |
| Table UI | [components/TableView.tsx](../../src/webview/components/TableView.tsx), [components/table](../../src/webview/components/table) | Grid, filters, edits, export, dialogs, and mutation flows. |
| ERD UI | [components/ErdView.tsx](../../src/webview/components/ErdView.tsx) | ERD rendering and interaction. |
| Connection form | [components/ConnectionFormView.tsx](../../src/webview/components/ConnectionFormView.tsx) | Connection editor UI. |
| Store and utilities | [store](../../src/webview/store), [utils](../../src/webview/utils) | Shared UI state, messaging helpers, formatting, and layout helpers. |

## tests

| Area | Representative files | Coverage focus |
|---|---|---|
| Extension host tests | [tests/extension](../../tests/extension) | Activation, controllers, panel wiring, and tree providers. |
| Node tests | [tests/node](../../tests/node) | Drivers, table services, formatting, validation, and shared utilities. |
| Webview tests | [tests/webview](../../tests/webview) | Rendering, interaction, messaging, and editor behavior. |
| Database tests | [tests/db](../../tests/db) | Live database contracts for SQLite, PostgreSQL, MySQL, MSSQL, Oracle, and shared DB harnesses. |
| Support and fixtures | [tests/support](../../tests/support), [tests/fixtures](../../tests/fixtures), [tests/runtime](../../tests/runtime), [tests/scripts](../../tests/scripts) | Harnesses, seed data, runtime helpers, and orchestration scripts. |
| Contracts | [tests/contracts/testingContracts.ts](../../tests/contracts/testingContracts.ts) | Shared expectations across live DB projects. |

## scripts

| File | Responsibility |
|---|---|
| [prepare-vscode-package.mjs](../../scripts/prepare-vscode-package.mjs) | Prepares package-time assets and metadata. |
| [sqliteInstaller.ts](../../src/extension/utils/sqliteInstaller.ts) | Downloads and caches the host-compatible SQLite native runtime on demand. |
