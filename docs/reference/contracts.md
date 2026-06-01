# Contracts Reference

This page is the canonical maintainer reference for host/webview data shapes. If a message or panel state changes, update this file alongside the code and the matching tests.

## Base Envelope

All webview messages use the same envelope shape:

| Type | Meaning |
|---|---|
| `WebviewMessageEnvelope<TType, TPayload>` | Base wrapper with a `type` discriminator and optional `payload`. |

## Initial State Types

| Type | View | Purpose | Key fields |
|---|---|---|---|
| `QueryInitialState` | `query` | Seeds the query editor | `connectionId`, `connectionType`, `queryText`, `initialSql`, `formatOnOpen`, `isBookmarked`, `editorLanguage`, `editorPresentation`, `panelRetentionMode` |
| `TableInitialState` | `table` | Seeds the table viewer | `connectionId`, `database`, `schema`, `table`, `isView`, `connectionReadOnly`, `defaultPageSize`, `panelRetentionMode` |
| `ErdInitialState` | `erd` | Seeds the ERD panel | `connectionId`, `database`, `schema`, `panelRetentionMode` |
| `ConnectionFormInitialState` | `connection` | Seeds the connection form | `existing`, `panelRetentionMode` |

## Query Panel Messages

| Message | Payload | Purpose |
|---|---|---|
| `activeConnectionChanged` | `{ connectionId: string }` | Keeps the editor aligned with the selected connection. |
| `executeQuery` | `{ queryText: string; sql?: string; connectionId?: string }` | Run the current query. |
| `getConnections` | none | Request the current connection list. |
| `getSchema` | `{ connectionId?: string }` | Request schema-aware completion data. |
| `exportResultsCSV` | none | Export the current query results. |
| `exportResultsJSON` | none | Export the current query results. |
| `readClipboard` | none | Read clipboard data for editor actions. |
| `writeClipboard` | `{ text: string }` | Write clipboard data for editor actions. |
| `addBookmark` | `{ queryText: string; sql?: string; connectionId?: string }` | Persist a query bookmark. |

## Table Panel Messages

| Message | Payload | Purpose |
|---|---|---|
| `ready` | none | Signals that the table view is ready. |
| `fetchPage` | page, pageSize, filters, sort, fetchId | Request a page of data. |
| `applyChanges` | `updates` and/or `insertValues` | Apply row edits. |
| `insertRow` | `{ values?: Record<string, unknown> }` | Insert a new row. |
| `deleteRows` | `{ primaryKeysList?: Array<Record<string, unknown>> }` | Delete selected rows. |
| `exportCSV` | sort, filters, limitToPage | Export the current slice as CSV. |
| `exportJSON` | sort, filters, limitToPage | Export the current slice as JSON. |
| `confirmMutationPreview` | `{ previewToken: string }` | Accept a mutation preview. |
| `cancelMutationPreview` | `{ previewToken: string }` | Cancel a mutation preview. |
| `readClipboard` | none | Read clipboard data. |
| `writeClipboard` | `{ text: string }` | Write clipboard data. |

## ERD Panel Messages

| Message | Payload | Purpose |
|---|---|---|
| `ready` | none | Marks the ERD webview as ready. |
| `reload` | none | Reloads graph data. |
| `openTableData` | `{ table: string; schema?: string; database?: string; isView?: boolean }` | Jump from ERD to table data. |

## Connection Form Messages

| Message | Payload | Purpose |
|---|---|---|
| `saveConnection` | `ConnectionFormSubmission` | Persist the form. |
| `testConnection` | `ConnectionFormSubmission` | Validate the form without saving. |
| `cancel` | none | Close the form without saving. |
| `browseFile` | none | Open a file picker for file-backed secrets or paths. |

## Secret And Sanitized Shapes

| Type | Meaning |
|---|---|
| `SanitizedConnectionConfig` | Connection config with password and SSH secret fields removed. |
| `ConnectionFormExistingState` | Sanitized config plus flags that indicate existing stored secrets. |
| `ConnectionFormSubmission` | Full payload that may include fresh secret values for save/test operations. |

## Safety And Transfer Rules

| Rule | Why it matters |
|---|---|
| The host is authoritative | The UI should never guess at persisted secrets or driver behavior. |
| Existing-secret flags matter | They let the form distinguish between a blank field and an intentionally preserved stored secret. |
| Payloads should stay serializable | Webview messages must survive `postMessage` and state persistence cleanly. |

## Related Source Files

| File | Why it matters |
|---|---|
| [src/shared/webviewContracts.ts](../../src/shared/webviewContracts.ts) | Canonical type source. |
| [src/webview/utils/messaging.ts](../../src/webview/utils/messaging.ts) | Host/webview bridge helpers. |
| [tests/node/webviewContractsParser.test.ts](../../tests/node/webviewContractsParser.test.ts) | Contract parsing and shape validation. |
| [tests/webview/messaging.test.ts](../../tests/webview/messaging.test.ts) | Messaging behavior coverage. |
