# Runtime Flows

This page traces the main end-to-end flows through the application. Use it when a bug spans more than one subsystem.

## 1. Connection Flow

```mermaid
sequenceDiagram
  participant U as User
  participant V as VS Code command/tree
  participant H as Extension host
  participant C as ConnectionManager
  participant S as Secret store
  participant D as DB driver
  participant W as Webview UI

  U->>V: Add or edit connection
  V->>H: command opens connection form
  H->>W: send initial connection form state
  U->>W: fill form and submit
  W->>H: saveConnection / testConnection
  H->>C: validate and normalize config
  C->>S: store or update secrets
  C->>D: test or establish the connection
  D-->>C: result / failure
  C-->>H: persist state and notify UI
  H-->>W: update explorer / connection form state
```

### Key checkpoints

| Step | What can fail | Where to inspect |
|---|---|---|
| Form opens | Wrong initial state or retention mode | [src/shared/webviewContracts.ts](../../src/shared/webviewContracts.ts) and panel controller code |
| Save/test submits | Payload mismatch or missing secret fields | [src/webview/components/ConnectionFormView.tsx](../../src/webview/components/ConnectionFormView.tsx) and [src/shared/webviewContracts.ts](../../src/shared/webviewContracts.ts) |
| Driver connection | Timeout, auth, native addon, or SSH issue | [src/extension/services/connectionValidationService.ts](../../src/extension/services/connectionValidationService.ts), [src/extension/services/sshRuntime.ts](../../src/extension/services/sshRuntime.ts), and the driver file |

## 2. Query Execution Flow

```mermaid
sequenceDiagram
  participant U as User
  participant W as Query webview
  participant P as Query panel controller
  participant S as Safety/limit logic
  participant D as DB driver
  participant R as Result formatter

  U->>W: enter SQL and run query
  W->>P: executeQuery message
  P->>S: validate query, limit, and cancellation policy
  S->>D: execute driver query
  D-->>S: rows, metadata, warnings
  S-->>R: normalize and format
  R-->>P: response payload
  P-->>W: update editor state, history, bookmarks, and results
```

### Query concerns

| Concern | Why it exists | Relevant files |
|---|---|---|
| Hard row cap | Prevents runaway result sets | [src/shared/safetyContracts.ts](../../src/shared/safetyContracts.ts), [src/extension/utils/queryResultFormatting.ts](../../src/extension/utils/queryResultFormatting.ts) |
| Cancel/timeout handling | Avoids late or duplicate settlements | [src/shared/safetyContracts.ts](../../src/shared/safetyContracts.ts), [src/extension/dbDrivers/timeout.ts](../../src/extension/dbDrivers/timeout.ts) |
| Driver-specific SQL | Different engines need different rewrites | [src/extension/dbDrivers](../../src/extension/dbDrivers) |

## 3. Table Browse And Edit Flow

```mermaid
flowchart TD
  A[Open table data] --> B[Table panel controller]
  B --> C[Load page and metadata]
  C --> D[Apply filters and sort]
  D --> E[Render virtualized grid]
  E --> F{Edit?}
  F -- no --> G[Export or paginate]
  F -- yes --> H[Build mutation preview]
  H --> I[Confirm or cancel]
  I --> J[Apply mutation]
  J --> K[Verify and refresh]
```

### Mutation behavior

| Stage | Purpose | Notes |
|---|---|---|
| Preview | Show the SQL or equivalent mutation before changes are applied | Implemented on the host side, not in the webview. |
| Confirm | Let the user accept or cancel the proposed change set | Useful when multiple rows are edited or deleted. |
| Apply | Execute inserts, updates, or deletes | Must respect driver capabilities and read-only guards. |
| Verify | Confirm the final state matches the intended mutation | If verification fails, the panel should surface the failure clearly. |

## 4. ERD Flow

```mermaid
sequenceDiagram
  participant U as User
  participant T as Explorer tree
  participant E as ERD panel controller
  participant G as ERD graph service
  participant D as DB driver
  participant W as ERD webview

  U->>T: Open ERD from database or schema node
  T->>E: openErd command
  E->>G: request graph metadata
  G->>D: load tables, columns, and relationships
  D-->>G: schema graph data
  G-->>E: graph payload
  E->>W: send initial ERD state
  W-->>E: reload/openTableData messages as needed
```

