<div align="center">

<br/>

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-6.png" alt="RapiDB" width="100%" />

<br/>
<br/>

# RapiDB - Database Client for VS Code

### PostgreSQL · MSSQL · MySQL · MariaDB · SQLite · Oracle
### Redis · MongoDB · Elasticsearch · DynamoDB
#### All in one place. Never leaving your editor.

<br/>

[![Open VSX](https://img.shields.io/open-vsx/dt/DmitriiKholkin/rapidb?style=flat-square&label=Open%20VSX&logo=visualstudiocode&color=007ACC&labelColor=555555)](https://open-vsx.org/extension/DmitriiKholkin/rapidb)
[![GitHub](https://img.shields.io/github/v/tag/DmitriiKholkin/RapiDB?style=flat-square&label=GitHub&color=007ACC&labelColor=555555)](https://github.com/DmitriiKholkin/RapiDB)
[![License](https://img.shields.io/badge/License-MIT-007ACC?style=flat-square&labelColor=555555)](https://opensource.org/licenses/MIT)

<br/>

<a href="https://marketplace.visualstudio.com/items?itemName=DmitriiKholkin.rapidb">
  <img src="https://img.shields.io/badge/Install%20from%20VS%20Marketplace-007ACC?style=for-the-badge&logo=visualstudiocode&logoColor=white" alt="Install from VS Marketplace"/>
</a>

<br/>

---

*You're deep in the code. Something's off in the data.*
*Now you have to alt-tab to DBeaver, wait for it to wake up,*
*click through five menus...*

**RapiDB kills that context switch.**
Your database lives in the sidebar - same window, same shortcuts, same theme.

---


<br/>

# ⚡ What it actually does

</div>

<br/>

### 🔌 Connect to anything

PostgreSQL, MS SQL Server, MySQL, MariaDB, SQLite, Oracle, Redis, MongoDB, Elasticsearch, DynamoDB - all supported out of the box. SSL, self-signed certs, SSH tunneling, connection folders to keep things organized.

For MongoDB, a non-empty **Database** field overrides the database in the connection URI. Otherwise the URI database is used; if neither specifies a database, RapiDB uses `admin`. `authSource` selects the authentication database, not the database queried by the editor. An explicit `db.getSiblingDB(...)` selects the database for that operation.

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-3.png" alt="Connection Form" width="100%" />

> Every connection has a **read-only mode** toggle. When enabled, table edits are blocked and query execution is limited to read operations. **SQL Server (MSSQL) and Oracle exceptions:** query editor execution is blocked in read-only mode because client-side classification cannot enforce read-only execution. Oracle autonomous transactions can commit writes even inside `SET TRANSACTION READ ONLY`, and caller rollback cannot undo them. Use a database account with enforced read-only permissions (including restrictions on executable routines) and a regular connection to run queries. Table data and schema browsing remain available. This toggle is an application safeguard, not a replacement for database permissions; database-defined views, policies, and routines may have side effects even during reads.

<br/>

### 🌲 Browse your schema without a single query

Saved connections can be grouped into folders, and each connection expands into databases → schemas → tables, views, materialized views, functions, procedures, sequences, and types. Right-click any object to copy its name, inspect columns with PK/FK badges, constraints, indexes, and triggers, open the data viewer where it applies, or pull the DDL / definition - no typing required.

Redis keys are grouped by the prefix before `:`. **All keys** and **Unprefixed keys** are separate views; prefixed groups are shown as patterns such as `default:*` and `users:*`. Edits and deletes still address the exact stored key.

Prefix groups are literal: `*`, `?`, `[`, `]`, and `\` inside a stored prefix are escaped in the Redis scan pattern, so selecting one group cannot include a different prefix through wildcard expansion.

Redis table reads and exports deduplicate `SCAN` replies and use exact-key tie-breaks when Unicode keys have equal locale collation. A repeated cursor or more than 5,000 scan requests fails explicitly rather than presenting an incomplete result. Cancellation stops waiting and prevents further reads; commands already sent may still complete. Exports are not transactional snapshots of a concurrently changing keyspace.

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-5.png" alt="Database Explorer tree" width="250" />

<br/>

### 🗂️ Query History & Bookmarks

Every query you run lands in **Query History** - click any entry to reopen it in the editor. Queries you want to keep forever go into **Bookmarks** with a single press. Query History limit is configurable.

<br/>

### 🧩 ERD with foreign key links

Open ERD from a database or schema node to visualize tables and the foreign key relationships between them. The diagram is built from live schema metadata, so it stays aligned with the current database snapshot.

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-4.png" alt="Database Explorer tree" width="100%" />

<br/>

### ✏️ A real SQL editor, not a textarea

The query editor runs on **Monaco** - the same engine as VS Code itself. You get:

- 🎨 Syntax highlighting & SQL formatting (button / `Shift+Alt+F`)
- 🧠 Schema-aware autocompletion - knows your actual tables and columns
- ⌨️ `Ctrl+Enter` / `F5` to run · Select a fragment to run just that part
- ↕️ Drag the divider to resize editor vs results

For PostgreSQL, MySQL, SQL Server, and SQLite, finish explicit transactions with `COMMIT` or `ROLLBACK` in the same executed script (or `RELEASE` for an outermost SQLite `SAVEPOINT`). Unfinished transactions are not kept for a later Execute. SQL Server keeps one session across the script's `GO` batches and cleans it before another query can borrow it; cancelling or timing out a query is not proof that earlier commits were undone.

Elasticsearch and DynamoDB use a single SDK request attempt, including for reads: automatic retries are disabled because a lost response or server error can follow a committed write. This also overrides `AWS_MAX_ATTEMPTS` for DynamoDB. After a failed mutation, refresh and verify the data before retrying; the first request may already have applied.

DynamoDB native JSON uses the official AWS `AttributeValue` format: `B` and `BS` contain canonical base64 strings, never hex or raw text. Binary values (including strings such as `"0xAB"` that also resemble hex) are decoded only as base64. AttributeValue shapes and binary encoding are checked across the script before its first request; scripts are still not transactions. Legacy updates with `AttributeUpdates` are dispatched as `UpdateItem`, including when `Expected` or `ReturnValues` is present.

Elasticsearch REST scripts validate supported endpoints and body shapes before sending the first request. DELETE requests and non-search GET requests reject JSON bodies instead of silently discarding them: `DELETE /index` deletes the entire index, not documents matching a body filter. Scripts are not transactions; server-side errors can still follow successful mutations.

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-2.png" alt="SQL Editor with results" width="100%" />

<br/>

### 📊 Results that don't freeze at 10k rows

Results land in a **virtualized table** - no jank, no browser tab hanging:

- Sort by any column · Resize columns · Alternating row stripes
- NULL values are styled differently · Values are colored by types
- Execution time shown right in the toolbar
- **Export to CSV or JSON** in one click

> Truncated results show a warning. `rapidb.queryRowLimit` controls the number of displayed rows, subject to a hard ceiling of 10,000 rows; increasing the setting above 10,000 does not lift that ceiling. This display limit does not limit rows affected by a mutation or cancel query execution.

MongoDB `find` and `aggregate` results also have a safety budget. The default sample is 100 documents; requesting more cannot bypass the 10,000-document ceiling, including with `find().limit(0)`. Negative and fractional chain limits are rejected; `aggregate().limit()` requires a positive integer. A probe document detects actual truncation without becoming part of the displayed schema. Exporting a truncated query result requires confirmation and exports only the displayed rows.

MongoDB `NumberInt(...)` and `NumberLong(...)` literals require one integer argument within the signed BSON int32/int64 range. Fractional, malformed, overflowing, and unsafe JavaScript number arguments are rejected before any operation in the script is sent. For exact large integers, use strings such as `NumberLong("9223372036854775807")` or bigint arguments rather than unquoted JavaScript numbers.

MongoDB `NumberDecimal(...)` requires exactly one string argument, such as `NumberDecimal("0.123456789012345678901")`. Numeric arguments are rejected before any operation in the script is sent because JavaScript can round them before Decimal128 conversion.

MongoDB `BinData(subtype, base64)` requires an integer subtype from 0 to 255 and a canonical base64 string (including padding when needed). Invalid literals are rejected before any operation in the script is sent, rather than silently decoding to different bytes.

MongoDB `Date` / `ISODate` literals reject invalid dates before any operation in the script is sent. They accept no arguments (current time), one date string or Date value, or one to seven safe integer arguments. Calendar-component arguments follow JavaScript's local-time `Date` constructor; use an ISO string with an explicit time zone for an unambiguous instant. Arguments are never silently discarded, and invalid dates cannot become Unix epoch during BSON serialization.

MongoDB updates and deletes require an explicit document filter; use `{}` explicitly to target all documents. Cursor modifiers on non-cursor operations, such as `deleteMany({}).limit(1)`, are rejected rather than silently ignored. Explicit database/collection names must be non-empty strings. BSON `Timestamp(t, i)` and `Timestamp({t, i})` components must be unsigned 32-bit integers, without coercion or rounding (`Timestamp(t)` defaults the increment to zero). Invalid filters, namespaces, modifiers, and timestamps are rejected before any operation in the script is sent.

MongoDB `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, and `deleteMany` helpers do not support an additional options argument. Such calls are rejected before any operation in the script is sent: options such as `collation`, `upsert`, or `ordered` must never be silently ignored. Use `db.runCommand(...)` with explicit native command options when needed.

MongoDB validates supported helper names, cursor limits, find projection/sort shapes, aggregate pipeline arrays, and no-argument helpers before submitting any operation in a script. These helper validation errors cannot follow an earlier delete or insert. Scripts are still not transactions: database/SDK errors can follow successful operations.

MongoDB `createView` requires explicit non-empty view/source names and a pipeline array; use `[]` explicitly for an unfiltered view. Invalid pipelines are never replaced with an empty one, and options cannot override the named source or pipeline. Supported cursor modifiers are `limit`, `skip`, `toArray`, `pretty`, and `sort` for find helpers only. Other modifiers, including `explain`, `close`, and `collation`, are rejected rather than silently ignored; `toArray` and `pretty` accept no arguments.

Redis query scripts are fully tokenized before the first command is sent. Invalid Unicode escapes and unpaired UTF-16 surrogates are rejected rather than addressing a replacement-character key or partially executing a script with a later parsing error. This validation does not make Redis scripts transactional; server-side errors can still follow successful commands.

Redis structured table edits (hash, list, set, sorted set) reject JSON numeric literals that would lose precision before any replacement is sent. Use quoted strings for exact large integers or decimal member values, such as `{"account":"9007199254740993"}`. Ordinary Redis string values are not interpreted as JSON.

Table exports keep a stable column schema across chunks. CSV uses the declared columns (including empty cells for missing values); a later unexpected column fails the export rather than silently dropping data. JSON applies the same unexpected-column check. Failed exports leave an existing destination file intact.

<br/>

### ✍️ Browse and edit table data

Click any table → the **Table Data Viewer** opens:

| Feature | Detail |
|---|---|
| Pagination | 25 / 100 / 500 / 1000 rows per page |
| Filtering | Draft-aware per-column filters |
| Inline editing | Click a cell → type → Enter |
| New rows | Insert bar at the top |
| Deletion | Select rows and delete |
| Safety | Preview-first apply flow with verification; transactional where applicable |

Persisted edits are prevalidated against available column metadata before any write. Known unsupported edits fail before the batch is applied; where representation compatibility depends on the stored value, verification runs in the transaction and a mismatch rolls it back. Preview skipping does not bypass validation or verification. Schemaless sampling is not a complete database schema or a guarantee that every value has the sampled type.

SQL table updates must affect exactly one row per edited row; an unexpected count aborts the transaction. This also protects against a cached primary key becoming non-unique after an external schema change. Table updates require rollback support, including temporal-only updates on MySQL; nontransactional engines such as MyISAM are blocked. Oracle catalog lookups preserve the exact identifier spelling, so quoted mixed-case objects do not borrow metadata from uppercase namesakes.

SQL table deletes also require rollback support, even when the selected rows fit into one DELETE statement. Storage-engine support is checked again after preview confirmation and before execution, so a stale selection or changed engine cannot bypass this safeguard.

Temporal-value verification is driver-specific: SQL Server and Oracle verify explicitly edited dates/times, while PostgreSQL, MySQL, and SQLite retain their existing temporal verification exceptions. Exact temporal persistence is not guaranteed by this safeguard for those three drivers.

MySQL `TINYINT(1)` is not restricted to boolean storage. Values outside `0`/`1` are preserved in query results, table reads, row keys, and exports rather than converted to booleans.

MySQL spatial table reads and exports retain nonzero reference systems as `SRID=4326;POINT(1 2)`. Editing with plain WKT preserves the existing SRID; use an explicit `SRID=...;` prefix to change it. New plain-WKT values default to SRID 0. Optimistic update checks compare both the SRID and coordinates.

<br/>

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-1.png" alt="Table Data Viewer" width="100%" />

<br/>

---

## ⚙️ Settings worth knowing

| Setting | Default | What it does |
|---|---|---|
| `rapidb.connections` | `[]` | Saved connections, including folders and other data |
| `rapidb.connectionTimeoutSeconds` | `15` | Timeout for establishing a database connection |
| `rapidb.dbOperationTimeoutSeconds` | `180` | Timeout for queries, metadata, DDL, and routine loading |
| `rapidb.queryRowLimit` | `10000` | Displayed rows per query: configured range 10–100000, effective ceiling 10000; does not limit affected rows or query execution time |
| `rapidb.queryHistoryLimit` | `100` | How many past queries to remember (0 = disable history) |
| `rapidb.defaultPageSize` | `25` | Default rows per page in the Table Data Viewer |
| `rapidb.skipTableMutationPreview` | `false` | Apply table inserts, edits, and deletes without opening the mutation preview dialog |

---

## 🚀 Get started in 4 steps

```
1. Install the extension
2. Click the RapiDB icon in the Activity Bar
3. Hit Add Connection (+) and fill in your credentials
4. Done - explore, query, edit
```

---

## 💬 Found a bug? Have an idea?

**[⭐ Leave a review in the Marketplace](https://marketplace.visualstudio.com/items?itemName=DmitriiKholkin.rapidb&ssr=false#review-details)** - even a short one helps others decide whether RapiDB fits their workflow, and tells me what's working.

**[🐛 Open an issue on GitHub](https://github.com/DmitriiKholkin/RapiDB/issues)** - I'm tracking everything there and fixing issues fast. Drop an issue with steps to reproduce and the DB type, and I'll get back to you quickly.

---

<details>

<summary>🛠️ For developers</summary>

<br/>

**Stack:**

| Layer | Technology |
|---|---|
| Extension host | TypeScript, VS Code Extension API |
| Webview UI | React 19, Monaco Editor, TanStack Table, TanStack Virtual, Zustand |
| ERD | @xyflow/react, @dagrejs/dagre |
| SQL formatting | sql-formatter |
| DB drivers | pg, mysql2, mssql, oracledb, better-sqlite3, redis, mongodb, @elastic/elasticsearch, @aws-sdk/client-dynamodb |
| VS Code icons | @vscode/codicons |
| Bundler | esbuild |

PRs and contributions are welcome at [github.com/DmitriiKholkin/RapiDB](https://github.com/DmitriiKholkin/RapiDB).

For development and verification commands, see [package.json](package.json); test projects are defined in [vitest.workspace.ts](vitest.workspace.ts). For support, use the [GitHub issue tracker](https://github.com/DmitriiKholkin/RapiDB/issues).

</details>


---

<div align="center">

**MIT License** · Made with love and the desire to never alt-tab again

</div>
