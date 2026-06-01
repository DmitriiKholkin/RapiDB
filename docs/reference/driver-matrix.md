# Driver Matrix

This matrix documents the support boundaries for each engine. It is intentionally conservative: if a capability is driver-specific or partial, the notes call that out.

## Capabilities Summary

| Driver | Connect / browse | Query | Table data | DDL / definition | ERD | Notes |
|---|---|---|---|---|---|---|
| PostgreSQL | Yes | Yes | Yes | Yes | Yes | Rich relational feature set and strong metadata support. |
| MySQL / MariaDB | Yes | Yes | Yes | Yes | Yes | Shares most relational flows, with engine-specific SQL and metadata behavior. |
| MSSQL | Yes | Yes | Yes | Yes | Yes | Includes temporal and computed-column considerations. |
| Oracle | Yes | Yes | Yes | Yes | Yes | PL/SQL and Oracle-specific DDL/metadata behavior apply. |
| SQLite | Yes | Yes | Yes | Yes | Yes | Local/native runtime support has packaging implications. |
| Redis | Partial | Partial | Partial | No | No | Specialized data model; read budgets and query flow differ from relational engines. |
| MongoDB | Partial | Partial | Partial | No | No | Query behavior is driver-specific and not SQL-first. |
| Elasticsearch | Partial | Partial | Partial | No | No | Search-oriented behavior and read budgets are engine-specific. |
| DynamoDB | Partial | Partial | Partial | No | No | Native helpers and type conventions are special-cased. |

## What "Partial" Means Here

| Capability | Meaning |
|---|---|
| Connect / browse | The engine can be connected to and explored, but the object model may be specialized. |
| Query | The editor and execution path exist, but the input language or execution semantics may differ from SQL. |
| Table data | Some browse/edit flows exist, but filters, edits, or exports may be restricted. |
| DDL / definition | No generic SQL DDL contract is promised. |
| ERD | No relational ERD is promised for non-relational engines. |

## Driver-Specific Reference Points

| Driver family | Primary code paths | Tests to consult |
|---|---|---|
| Relational | [src/extension/dbDrivers/postgres.ts](../../src/extension/dbDrivers/postgres.ts), [mysql.ts](../../src/extension/dbDrivers/mysql.ts), [mssql.ts](../../src/extension/dbDrivers/mssql.ts), [oracle.ts](../../src/extension/dbDrivers/oracle.ts), [sqlite.ts](../../src/extension/dbDrivers/sqlite.ts) | [tests/node/postgresPreviewSql.test.ts](../../tests/node/postgresPreviewSql.test.ts), [tests/node/mysqlPreviewSql.test.ts](../../tests/node/mysqlPreviewSql.test.ts), [tests/node/mssqlPreviewSql.test.ts](../../tests/node/mssqlPreviewSql.test.ts), [tests/node/oraclePreviewSql.test.ts](../../tests/node/oraclePreviewSql.test.ts), [tests/node/sqliteRuntimeAdapter.test.ts](../../tests/node/sqliteRuntimeAdapter.test.ts) |
| Redis | [src/extension/dbDrivers/redis.ts](../../src/extension/dbDrivers/redis.ts) | [tests/node/redisQuery.test.ts](../../tests/node/redisQuery.test.ts) |
| MongoDB | [src/extension/dbDrivers/mongodb.ts](../../src/extension/dbDrivers/mongodb.ts) | [tests/node/mongodbMongoshQuery.test.ts](../../tests/node/mongodbMongoshQuery.test.ts), [tests/node/mongodbFilters.test.ts](../../tests/node/mongodbFilters.test.ts) |
| Elasticsearch | [src/extension/dbDrivers/elasticsearch.ts](../../src/extension/dbDrivers/elasticsearch.ts) | [tests/node/elasticsearchLifecycle.test.ts](../../tests/node/elasticsearchLifecycle.test.ts), [tests/node/elasticsearchData.test.ts](../../tests/node/elasticsearchData.test.ts) |
| DynamoDB | [src/extension/dbDrivers/dynamodb.ts](../../src/extension/dbDrivers/dynamodb.ts) | [tests/node/dynamodbNative.test.ts](../../tests/node/dynamodbNative.test.ts), [tests/node/dynamodbMetadata.test.ts](../../tests/node/dynamodbMetadata.test.ts) |

## Operational Notes

| Issue | Implication |
|---|---|
| Native addon packaging | SQLite and some drivers require special preparation during install and publish. |
| Engine-specific SQL rewrite | Query preview and mutation logic cannot be shared blindly across engines. |
| Capability divergence | Support answers should point users to the matching driver family, not to a generic behavior promise. |
