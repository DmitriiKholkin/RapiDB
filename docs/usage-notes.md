# RapiDB usage notes

Detailed behavior, safety limitations, and configuration notes. For an overview and quick start, see the [README](../README.md).

## Connections and read-only mode

For MongoDB, a non-empty **Database** field overrides the database in the connection URI. Otherwise the URI database is used; if neither specifies a database, RapiDB uses `admin`. `authSource` selects the authentication database, not the database queried by the editor. An explicit `db.getSiblingDB(...)` selects the database for that operation.

Every connection has a **read-only mode** toggle. When enabled, table edits are blocked and query execution is limited to read operations. **SQL Server (MSSQL) and Oracle exceptions:** query editor execution is blocked in read-only mode because client-side classification cannot enforce read-only execution. Oracle autonomous transactions can commit writes even inside `SET TRANSACTION READ ONLY`, and caller rollback cannot undo them. Use a database account with enforced read-only permissions (including restrictions on executable routines) and a regular connection to run queries. Table data and schema browsing remain available. This toggle is an application safeguard, not a replacement for database permissions; database-defined views, policies, and routines may have side effects even during reads.

### SSH tunneling and local trust boundary

For fixed-target SSH connections, RapiDB opens a TCP listener on `127.0.0.1` at an OS-assigned port on the **VS Code extension host** (which may be a remote machine or container). Like standard SSH local port forwarding, it relays database protocol bytes to the configured host and port through SSH. Elasticsearch Cloud ID uses in-process HTTP(S) agents instead, without a local TCP listener.

Loopback prevents direct access from other machines; it does **not** authenticate local clients. Other processes able to reach that loopback interface, potentially including other OS users, can use the forward while it is open without SSH credentials. The random port is not a secret or an access-control mechanism. Each TCP connection is forwarded separately: the tunnel does not supply RapiDB's database credentials or reuse its authenticated database session for another client.

- Require database/service authentication and least-privilege permissions, even over SSH. Do not rely solely on the bastion's source IP or passwordless/trust rules for connections from the bastion: forwarded clients share that network access. RapiDB's read-only toggle does not restrict external clients.
- Use database TLS with server identity verification where supported, especially when the database is on a different host from the SSH server; SSH encryption ends at the SSH server.
- Run the extension host in a trusted environment. If untrusted local users or processes must be isolated, use OS-enforced network isolation (for example, a separate VM), not loopback alone. RapiDB does not provide protection against a malicious process running as the same OS user. Disconnect unused connections to close their forwards.

## Redis browsing

Redis keys are grouped by the prefix before `:`. **All keys** and **Unprefixed keys** are separate views; prefixed groups are shown as patterns such as `default:*` and `users:*`. Edits and deletes still address the exact stored key.

Prefix groups are literal: `*`, `?`, `[`, `]`, and `\` inside a stored prefix are escaped in the Redis scan pattern, so selecting one group cannot include a different prefix through wildcard expansion.

Redis table reads and exports deduplicate `SCAN` replies and use exact-key tie-breaks when Unicode keys have equal locale collation. A repeated cursor or more than 5,000 scan requests fails explicitly rather than presenting an incomplete result. Cancellation stops waiting and prevents further reads; commands already sent may still complete. Exports are not transactional snapshots of a concurrently changing keyspace.

## Query text privacy and cleanup

- **Storage:** History and bookmarks persist SQL/query text (only outer whitespace is trimmed), connection IDs, and timestamps in VS Code's extension `globalState`, shared across workspaces rather than stored in workspace files. This is not credential `SecretStorage`: literals, comments, and any secrets in query text are not redacted. History recording is requested before the driver executes, so failed or cancelled queries can remain; history is not a log of successful executions.
- **History:** Set `"rapidb.queryHistoryLimit": 0` in VS Code user settings to stop new history entries (default: 100, shared across connections). Lowering the limit while the extension is active trims excess entries, including clearing history when changed to 0. To explicitly clear existing history, use **Clear History** (trash icon) in the **Query History** view header and confirm **Clear**. There is no individual history-entry deletion command.
- **Bookmarks:** The history limit does not disable or clear bookmarks. Avoid **Bookmark** if you do not want to save query text. Right-click an entry in **Bookmarks** → **Delete Bookmark** to remove one, or use **Clear Bookmarks** (trash icon) in that view's header and confirm **Clear** to remove all. Clearing history does not clear bookmarks. These cleanup commands are not shown in the Command Palette.

These controls only manage history/bookmark entries, not copies elsewhere. Editor drafts, including unexecuted text, can persist separately in VS Code webview state even with history disabled; clearing history/bookmarks does not clear those drafts. Cleanup does not guarantee secure erasure from storage or backups.

## Query execution

For PostgreSQL, MySQL, SQL Server, and SQLite, finish explicit transactions with `COMMIT` or `ROLLBACK` in the same executed script (or `RELEASE` for an outermost SQLite `SAVEPOINT`). Unfinished transactions are not kept for a later Execute. SQL Server keeps one session across the script's `GO` batches and cleans it before another query can borrow it; cancelling or timing out a query is not proof that earlier commits were undone. After a failed or cancelled write, verify the data before retrying: earlier changes may already have committed.

Elasticsearch and DynamoDB use a single SDK request attempt, including for reads: automatic retries are disabled because a lost response or server error can follow a committed write. This also overrides `AWS_MAX_ATTEMPTS` for DynamoDB. After a failed mutation, refresh and verify the data before retrying; the first request may already have applied.

### DynamoDB

DynamoDB native JSON uses the official AWS `AttributeValue` format: `B` and `BS` contain canonical base64 strings, never hex or raw text. Binary values (including strings such as `"0xAB"` that also resemble hex) are decoded only as base64. AttributeValue shapes and binary encoding are checked across the script before its first request; scripts are still not transactions. Legacy updates with `AttributeUpdates` are dispatched as `UpdateItem`, including when `Expected` or `ReturnValues` is present.

### Elasticsearch

Elasticsearch REST scripts validate supported endpoints and body shapes before sending the first request. DELETE requests and non-search GET requests reject JSON bodies instead of silently discarding them: `DELETE /index` deletes the entire index, not documents matching a body filter. Scripts are not transactions; server-side errors can still follow successful mutations.

Elasticsearch REST paths must start with `/` and address the current connection. Absolute URLs, fragments, backslashes, invalid Unicode, and literal or percent-encoded `.` / `..` path segments are rejected before any request in the script. Encode document ID delimiters such as `/`, `#`, and `?` as `%2F`, `%23`, and `%3F`; URL normalization must never turn a document operation into an index operation.

### MongoDB

MongoDB `find` and `aggregate` results also have a safety budget. The default sample is 100 documents; requesting more cannot bypass the 10,000-document ceiling, including with `find().limit(0)`. Negative and fractional chain limits are rejected; `aggregate().limit()` requires a positive integer. A probe document detects actual truncation without becoming part of the displayed schema. Exporting a truncated query result requires confirmation and exports only the displayed rows.

MongoDB `NumberInt(...)` and `NumberLong(...)` literals require one integer argument within the signed BSON int32/int64 range. Fractional, malformed, overflowing, and unsafe JavaScript number arguments are rejected before any operation in the script is sent. For exact large integers, use strings such as `NumberLong("9223372036854775807")` or bigint arguments rather than unquoted JavaScript numbers.

Bare JavaScript bigint values must also fit signed BSON int64, including nested fields and computed values. Overflow is rejected before the script starts instead of silently wrapping to a different stored value or row ID. Use `NumberDecimal("...")` or strings for larger exact values.

MongoDB `NumberDecimal(...)` requires exactly one string argument, such as `NumberDecimal("0.123456789012345678901")`. Numeric arguments are rejected before any operation in the script is sent because JavaScript can round them before Decimal128 conversion.

MongoDB `BinData(subtype, base64)` requires an integer subtype from 0 to 255 and a canonical base64 string (including padding when needed). Invalid literals are rejected before any operation in the script is sent, rather than silently decoding to different bytes.

MongoDB regular expressions preserve the `i`, `m`, `s`, and `u` flags in BSON. JavaScript-only flags `d`, `g`, `v`, and `y` are rejected before any operation in the script is sent; they must never be silently dropped or converted into different matching rules for an update or delete.

MongoDB `Date` / `ISODate` literals reject invalid dates before any operation in the script is sent. `ISODate` follows mongosh: it accepts no arguments (current time), one ISO-8601 date string (interpreted as UTC when no time zone is given; compact forms such as `20260101T000000Z` are accepted), or one Date value. `Date` keeps JavaScript's rules: it accepts no arguments, one date string or Date value, or one to seven safe integer arguments, and both strings without a time zone and calendar-component arguments follow JavaScript's `Date` constructor; use an ISO string with an explicit time zone for an unambiguous instant. Arguments are never silently discarded, and invalid dates cannot become Unix epoch during BSON serialization.

MongoDB updates and deletes require an explicit document filter; use `{}` explicitly to target all documents. Cursor modifiers on non-cursor operations, such as `deleteMany({}).limit(1)`, are rejected rather than silently ignored. Explicit database/collection names must be non-empty strings. BSON `Timestamp(t, i)` and `Timestamp({t, i})` components must be unsigned 32-bit integers, without coercion or rounding (`Timestamp(t)` defaults the increment to zero). Invalid filters, namespaces, modifiers, and timestamps are rejected before any operation in the script is sent.

MongoDB `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, and `deleteMany` helpers do not support an additional options argument. Such calls are rejected before any operation in the script is sent: options such as `collation`, `upsert`, or `ordered` must never be silently ignored. Use `db.runCommand(...)` with explicit native command options when needed.

MongoDB validates supported helper names, cursor limits, find projection/sort shapes, aggregate pipeline arrays, and no-argument helpers before submitting any operation in a script. These helper validation errors cannot follow an earlier delete or insert. Scripts are still not transactions: database/SDK errors can follow successful operations.

MongoDB `createView` requires explicit non-empty view/source names and a pipeline array; use `[]` explicitly for an unfiltered view. Invalid pipelines are never replaced with an empty one, and options cannot override the named source or pipeline. Supported cursor modifiers are `limit`, `skip`, `toArray`, `pretty`, and `sort` for find helpers only. Other modifiers, including `explain`, `close`, and `collation`, are rejected rather than silently ignored; `toArray` and `pretty` accept no arguments.

### Redis

Redis query scripts are fully tokenized before the first command is sent. Invalid Unicode escapes and unpaired UTF-16 surrogates are rejected rather than addressing a replacement-character key or partially executing a script with a later parsing error. This validation does not make Redis scripts transactional; server-side errors can still follow successful commands.

Redis structured table edits (hash, list, set, sorted set) reject JSON numeric literals that would lose precision before any replacement is sent. Use quoted strings for exact large integers or decimal member values, such as `{"account":"9007199254740993"}`. Ordinary Redis string values are not interpreted as JSON.

## Results and exports

Truncated results show a warning. `rapidb.queryRowLimit` controls the number of displayed rows, subject to a hard ceiling of 10,000 rows; increasing the setting above 10,000 does not lift that ceiling. This display limit does not limit rows affected by a mutation or cancel query execution. Query-result exports include only the displayed rows; use JSON when preserving value types matters.

Table exports keep a stable column schema across chunks. SQL NULL is written as `(NULL)` in both table and query-result CSV exports; empty strings stay empty. Missing fields are written as `MISSING`. Marker-like strings are not escaped. When a column contains both a marker and the same literal value, CSV export fails, even across chunks. CSV cannot distinguish an isolated marker from an identical literal: use JSON when retaining types or missing values matters. Spreadsheet formula-like strings are protected by an apostrophe prefix. A later unexpected column fails the export rather than silently dropping data. JSON applies the same unexpected-column check. Failed exports leave an existing destination file intact.

Exports are staged in the destination directory. New files are published with an exclusive hard link; support is probed before reading export data, and filesystems that do not support it reject the export. A later filesystem or permission change can still make publication fail. On POSIX, replacement of an existing file uses a single rename, subject to the filesystem's rename guarantees. On Windows, replacing an existing file is not supported: choose a new name. No portable compare-and-swap is available for an existing POSIX destination, so avoid changing that path concurrently. These operations do not guarantee durability after power loss.

MongoDB table exports use canonical Extended JSON for BSON values. Literal documents containing reserved EJSON type keys (for example `$oid`) fail explicitly because EJSON cannot preserve their types unambiguously; use a BSON backup for these documents.

## Table editing

Persisted edits are prevalidated against available column metadata before any write. Known unsupported edits fail before the batch is applied; where representation compatibility depends on the stored value, verification runs in the transaction and a mismatch rolls it back. Preview skipping does not bypass validation or verification. Schemaless sampling is not a complete database schema or a guarantee that every value has the sampled type.

SQL table updates must affect exactly one row per edited row; an unexpected count aborts the transaction. This also protects against a cached primary key becoming non-unique after an external schema change. Table updates require rollback support, including temporal-only updates on MySQL; nontransactional engines such as MyISAM are blocked. Oracle catalog lookups preserve the exact identifier spelling, so quoted mixed-case objects do not borrow metadata from uppercase namesakes.

SQL table deletes also require rollback support, even when the selected rows fit into one DELETE statement. Storage-engine support is checked again after preview confirmation and before execution, so a stale selection or changed engine cannot bypass this safeguard.

Temporal-value verification is driver-specific: SQL Server and Oracle verify explicitly edited dates/times, while PostgreSQL, MySQL, and SQLite retain their existing temporal verification exceptions. Exact temporal persistence is not guaranteed by this safeguard for those three drivers.

MySQL `TINYINT(1)` is not restricted to boolean storage. Values outside `0`/`1` are preserved in query results, table reads, row keys, and exports rather than converted to booleans.

MySQL spatial table reads and exports retain nonzero reference systems as `SRID=4326;POINT(1 2)`. Editing with plain WKT preserves the existing SRID; use an explicit `SRID=...;` prefix to change it. New plain-WKT values default to SRID 0. Optimistic update checks compare both the SRID and coordinates.

## Development

| Layer | Technology |
|---|---|
| Extension host | TypeScript, VS Code Extension API |
| Webview UI | React 19, Monaco Editor, TanStack Table, TanStack Virtual, Zustand |
| ERD | @xyflow/react, @dagrejs/dagre |
| SQL formatting | sql-formatter |
| DB drivers | pg, mysql2, mssql, oracledb, better-sqlite3, redis, mongodb, @elastic/elasticsearch, @aws-sdk/client-dynamodb |
| VS Code icons | @vscode/codicons |
| Bundler | esbuild |

For development and verification commands, see [package.json](../package.json); test projects are defined in [vitest.workspace.ts](../vitest.workspace.ts). PRs and contributions are welcome at [github.com/DmitriiKholkin/RapiDB](https://github.com/DmitriiKholkin/RapiDB).
