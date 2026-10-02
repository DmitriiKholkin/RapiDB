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

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-3.png" alt="Connection Form" width="100%" />

> Every connection has a **read-only mode** toggle. When enabled, table edits are blocked and query execution is limited to read operations. **SQL Server (MSSQL) exception:** query execution is blocked in read-only mode because client-side classification cannot enforce read-only execution; use a database account with read-only permissions and a regular connection to run queries. Table data remains available in read-only mode.

<br/>

### 🌲 Browse your schema without a single query

Saved connections can be grouped into folders, and each connection expands into databases → schemas → tables, views, materialized views, functions, procedures, sequences, and types. Right-click any object to copy its name, inspect columns with PK/FK badges, constraints, indexes, and triggers, open the data viewer where it applies, or pull the DDL / definition - no typing required.

**PostgreSQL table DDL is reconstructed**, not a `pg_dump` schema backup. It includes columns and named PRIMARY KEY, CHECK, UNIQUE, FOREIGN KEY and exclusion constraints from the catalog. Referenced tables, types, functions and sequences must already exist; sequences, standalone indexes, triggers, security, storage settings and partition/inheritance definitions are not included. Dependencies are schema-qualified when deparsed outside the catalog search path. Views and materialized views use the server's native view definition.

**Redis keyspaces:** `default` retains its historical **all-keys** meaning (`*`), including keys without a prefix. A real `default:` prefix has a separate navigation identity, `default:`, which reads only `default:*`. Other keyspaces use `prefix:*`. When there are no unprefixed keys, a real `default:` prefix creates only the `default:` node, not the all-keys node. These names control reads and exports; edits and deletes still address the exact stored primary-key value.

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

<img src="https://raw.githubusercontent.com/DmitriiKholkin/RapiDB/main/media/img-readme-2.png" alt="SQL Editor with results" width="100%" />

<br/>

### 📊 Results that don't freeze at 10k rows

Results land in a **virtualized table** - no jank, no browser tab hanging:

- Sort by any column · Resize columns · Alternating row stripes
- NULL values are styled differently · Values are colored by types
- Execution time shown right in the toolbar
- **Export to CSV or JSON** in one click

> Truncated results show a warning. `rapidb.queryRowLimit` controls the number of displayed rows, subject to a hard ceiling of 10,000 rows; increasing the setting above 10,000 does not lift that ceiling. This display limit does not limit rows affected by a mutation or cancel query execution.

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
