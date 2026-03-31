import oracledb from "oracledb";
import type { ConnectionConfig } from "../connectionManager";
import type {
  ColumnMeta,
  DatabaseInfo,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  QueryResult,
  SchemaInfo,
  TableInfo,
} from "./types";

function oracleFullType(
  dataType: string,
  dataPrecision: number | null,
  dataScale: number | null,
  dataLength: number,
): string {
  if (dataType === "NUMBER") {
    if (dataPrecision !== null && dataScale !== null) {
      return `NUMBER(${dataPrecision},${dataScale})`;
    }
    if (dataPrecision !== null) {
      return `NUMBER(${dataPrecision})`;
    }
    return "NUMBER";
  }
  if (dataType === "FLOAT") {
    return dataPrecision !== null ? `FLOAT(${dataPrecision})` : "FLOAT";
  }

  if (["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"].includes(dataType)) {
    return `${dataType}(${dataLength})`;
  }
  return dataType;
}

oracledb.fetchAsString = [oracledb.CLOB];
oracledb.fetchAsBuffer = [oracledb.BLOB];

let _thickInitDone = false;

function ensureThickMode(libDir?: string): void {
  if (_thickInitDone) {
    return;
  }
  try {
    oracledb.initOracleClient(libDir ? { libDir } : undefined);
    _thickInitDone = true;
  } catch (err: any) {
    throw new Error(
      `[RapiDB] Oracle thick mode init failed: ${err.message}\n` +
        `Make sure Oracle Instant Client is installed and the path is correct.`,
    );
  }
}

function replacePositionalParams(
  sql: string,
  params: unknown[],
): {
  sql: string;
  binds: unknown[];
} {
  let resultSql = "";
  let idx = 0;
  let i = 0;
  const len = sql.length;

  while (i < len) {
    const char = sql[i];
    const nextChar = sql[i + 1];

    if (char === "'") {
      resultSql += char;
      i++;
      while (i < len) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          resultSql += "''";
          i += 2;
        } else if (sql[i] === "'") {
          resultSql += "'";
          i++;
          break;
        } else {
          resultSql += sql[i];
          i++;
        }
      }
      continue;
    }

    if (char === "/" && nextChar === "*") {
      const endIdx = sql.indexOf("*/", i + 2);
      if (endIdx === -1) {
        resultSql += sql.slice(i);
        i = len;
      } else {
        resultSql += sql.slice(i, endIdx + 2);
        i = endIdx + 2;
      }
      continue;
    }

    if (char === "-" && nextChar === "-") {
      const endIdx = sql.indexOf("\n", i + 2);
      if (endIdx === -1) {
        resultSql += sql.slice(i);
        i = len;
      } else {
        resultSql += sql.slice(i, endIdx + 1);
        i = endIdx + 1;
      }
      continue;
    }

    if (char === "?") {
      idx++;
      resultSql += `:${idx}`;
      i++;
      continue;
    }

    resultSql += char;
    i++;
  }

  if (idx !== params.length) {
    throw new Error(
      `[RapiDB] Oracle parameter mismatch: SQL has ${idx} placeholder(s) but ${params.length} value(s) were supplied.`,
    );
  }

  return { sql: resultSql, binds: params };
}

function isPLSQLBlock(stmt: string): boolean {
  const s = stmt
    .replace(/'(?:[^']|'')*'/g, "''")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ")
    .trimStart()
    .toUpperCase();

  return (
    /^CREATE\s+(OR\s+REPLACE\s+)?(EDITIONABLE\s+|NONEDITIONABLE\s+)?(FUNCTION|PROCEDURE|PACKAGE|TRIGGER|TYPE)\b/.test(
      s,
    ) ||
    /^DECLARE\b/.test(s) ||
    /^BEGIN\b/.test(s)
  );
}

function splitOracleStatements(src: string): string[] {
  const stmts: string[] = [];
  let cur = "";
  let i = 0;
  const len = src.length;

  while (i < len) {
    if (src[i] === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      if (end === -1) {
        cur += src.slice(i);
        i = len;
      } else {
        cur += src.slice(i, end + 2);
        i = end + 2;
      }
      continue;
    }

    if (src[i] === "-" && src[i + 1] === "-") {
      const end = src.indexOf("\n", i + 2);
      if (end === -1) {
        cur += src.slice(i);
        i = len;
      } else {
        cur += src.slice(i, end + 1);
        i = end + 1;
      }
      continue;
    }

    if (src[i] === "'") {
      let j = i + 1;
      while (j < len) {
        if (src[j] === "'" && src[j + 1] === "'") {
          j += 2;
        } else if (src[j] === "'") {
          j++;
          break;
        } else {
          j++;
        }
      }
      cur += src.slice(i, j);
      i = j;
      continue;
    }

    if (src[i] === ";") {
      if (isPLSQLBlock(cur)) {
        cur += src[i];
      } else {
        const stmt = cur.trim();
        if (stmt) stmts.push(stmt);
        cur = "";
      }
      i++;
      continue;
    }

    if (src[i] === "/") {
      const lineStart = src.lastIndexOf("\n", i - 1) + 1;
      const beforeSlash = src.slice(lineStart, i);
      const afterSlash = src.slice(i + 1).match(/^[ \t]*([\r\n]|$)/);

      if (beforeSlash.trim() === "" && afterSlash) {
        const stmt = cur.trim();
        if (stmt) stmts.push(stmt);
        cur = "";
        i++;
        while (i < len && src[i] !== "\n") i++;
        if (i < len) i++;
        continue;
      }
    }

    cur += src[i];
    i++;
  }

  const last = cur.trim();
  if (last) stmts.push(last);

  return stmts.filter((s) => s.length > 0);
}

export class OracleDriver implements IDBDriver {
  private pool: oracledb.Pool | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close(0);
      } catch {}
      this.pool = null;
    }

    if (this.config.thickMode) {
      ensureThickMode(this.config.clientPath || undefined);
    }

    const serviceName = this.config.serviceName || this.config.database;
    const host = this.config.host ?? "localhost";
    const port = this.config.port ?? 1521;

    const connectString = serviceName
      ? `${host}:${port}/${serviceName}`
      : `${host}:${port}`;

    this.pool = (await oracledb.createPool({
      user: this.config.username ?? "",
      password: this.config.password ?? "",
      connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 30,
      poolPingInterval: 60,
    })) as unknown as oracledb.Pool;

    const conn = await this.pool.getConnection();
    try {
      await conn.ping();
    } finally {
      await conn.close();
    }
  }

  async disconnect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close(0);
      } catch {}
      this.pool = null;
    }
  }

  isConnected(): boolean {
    if (!this.pool) {
      return false;
    }

    return this.pool.connectionsInUse > 0 || this.pool.connectionsOpen > 0;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const conn = await this.pool!.getConnection();
    try {
      const res = await conn.execute<{ ORA_DATABASE_NAME: string }>(
        `SELECT ora_database_name FROM dual`,
        {},
      );
      const name =
        res.rows?.[0]?.ORA_DATABASE_NAME ??
        this.config.serviceName ??
        this.config.database ??
        "ORACLE";
      return [{ name, schemas: [] }];
    } catch {
      return [
        {
          name: this.config.serviceName ?? this.config.database ?? "ORACLE",
          schemas: [],
        },
      ];
    } finally {
      await conn.close();
    }
  }

  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    const conn = await this.pool!.getConnection();
    try {
      const res = await conn.execute<{ OWNER: string }>(
        `SELECT username AS owner
        FROM all_users
        WHERE oracle_maintained = 'N'
            OR username = 'SYSTEM'
        ORDER BY username`,
      );
      return (res.rows ?? []).map((r) => ({ name: r.OWNER }));
    } catch {
      const res2 = await conn.execute<{ USERNAME: string }>(
        `SELECT sys_context('USERENV','SESSION_USER') AS username FROM dual`,
      );
      const owner = res2.rows?.[0]?.USERNAME ?? this.config.username ?? "";
      return owner ? [{ name: owner }] : [];
    } finally {
      await conn.close();
    }
  }

  async listObjects(_database: string, schema: string): Promise<TableInfo[]> {
    const conn = await this.pool!.getConnection();
    try {
      const objects: TableInfo[] = [];

      const tableRes = await conn.execute<{
        OBJECT_NAME: string;
        OBJECT_TYPE: string;
      }>(
        `SELECT object_name, object_type
         FROM all_objects
         WHERE owner = :1
           AND object_type IN ('TABLE','VIEW','FUNCTION','PROCEDURE','PACKAGE')
           AND status = 'VALID'
         ORDER BY object_type, object_name`,
        [schema.toUpperCase()],
      );

      for (const r of tableRes.rows ?? []) {
        const rawType = r.OBJECT_TYPE.trim();
        let type: TableInfo["type"];
        if (rawType === "TABLE") {
          type = "table";
        } else if (rawType === "VIEW") {
          type = "view";
        } else if (rawType === "FUNCTION") {
          type = "function";
        } else if (rawType === "PROCEDURE" || rawType === "PACKAGE") {
          type = "procedure";
        } else {
          continue;
        }
        objects.push({ schema, name: r.OBJECT_NAME, type });
      }

      return objects;
    } finally {
      await conn.close();
    }
  }

  async describeTable(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const conn = await this.pool!.getConnection();
    try {
      const colRes = await conn.execute<{
        COLUMN_NAME: string;
        DATA_TYPE: string;
        DATA_PRECISION: number | null;
        DATA_SCALE: number | null;
        DATA_LENGTH: number;
        NULLABLE: string;
        DATA_DEFAULT: string | null;
        COLUMN_ID: number;
      }>(
        `SELECT column_name, data_type, data_precision, data_scale,
                data_length, nullable, data_default, column_id
         FROM all_tab_columns
         WHERE owner = :1 AND table_name = :2
         ORDER BY column_id`,
        [schema.toUpperCase(), table.toUpperCase()],
      );

      const pkRes = await conn.execute<{ COLUMN_NAME: string }>(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1
           AND cons.table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
      );
      const pkCols = new Set((pkRes.rows ?? []).map((r) => r.COLUMN_NAME));

      const fkRes = await conn.execute<{ COLUMN_NAME: string }>(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'R'
           AND cons.owner = :1
           AND cons.table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
      );
      const fkCols = new Set((fkRes.rows ?? []).map((r) => r.COLUMN_NAME));

      const identityMap = new Map<
        string,
        "ALWAYS" | "BY DEFAULT" | "BY DEFAULT ON NULL"
      >();
      try {
        const idRes = await conn.execute<{
          COLUMN_NAME: string;
          GENERATION_TYPE: string;
        }>(
          `SELECT column_name, generation_type
           FROM all_tab_identity_cols
           WHERE owner = :1 AND table_name = :2`,
          [schema.toUpperCase(), table.toUpperCase()],
        );
        for (const r of idRes.rows ?? []) {
          identityMap.set(
            r.COLUMN_NAME,
            (r.GENERATION_TYPE ?? "ALWAYS") as
              | "ALWAYS"
              | "BY DEFAULT"
              | "BY DEFAULT ON NULL",
          );
        }
      } catch {}

      return (colRes.rows ?? []).map((r) => {
        const genType = identityMap.get(r.COLUMN_NAME);
        let defaultValue: string | undefined;
        if (genType !== undefined) {
          defaultValue =
            genType === "ALWAYS"
              ? "GENERATED ALWAYS AS IDENTITY"
              : genType === "BY DEFAULT ON NULL"
                ? "GENERATED BY DEFAULT ON NULL AS IDENTITY"
                : "GENERATED BY DEFAULT AS IDENTITY";
        } else {
          defaultValue = r.DATA_DEFAULT?.trim() ?? undefined;
        }

        return {
          name: r.COLUMN_NAME,
          type: oracleFullType(
            r.DATA_TYPE,
            r.DATA_PRECISION,
            r.DATA_SCALE,
            r.DATA_LENGTH,
          ),
          nullable: r.NULLABLE === "Y",
          defaultValue,
          isPrimaryKey: pkCols.has(r.COLUMN_NAME),
          isForeignKey: fkCols.has(r.COLUMN_NAME),
        };
      });
    } finally {
      await conn.close();
    }
  }

  async getIndexes(
    _database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    const conn = await this.pool!.getConnection();
    try {
      const res = await conn.execute<{
        INDEX_NAME: string;
        COLUMN_NAME: string;
        UNIQUENESS: string;
        INDEX_TYPE: string;
      }>(
        `SELECT i.index_name, c.column_name, i.uniqueness,
                CASE WHEN i.index_name = (
                  SELECT constraint_name FROM all_constraints
                  WHERE owner = i.owner AND table_name = i.table_name
                    AND constraint_type = 'P' AND ROWNUM = 1
                ) THEN 'PRIMARY' ELSE 'NORMAL' END AS index_type
         FROM all_indexes i
         JOIN all_ind_columns c
           ON c.index_name = i.index_name AND c.index_owner = i.owner
         WHERE i.owner = :1 AND i.table_name = :2
         ORDER BY i.index_name, c.column_position`,
        [schema.toUpperCase(), table.toUpperCase()],
      );

      const map = new Map<string, IndexMeta>();
      for (const r of res.rows ?? []) {
        if (!map.has(r.INDEX_NAME)) {
          map.set(r.INDEX_NAME, {
            name: r.INDEX_NAME,
            columns: [],
            unique: r.UNIQUENESS === "UNIQUE",
            primary: r.INDEX_TYPE === "PRIMARY",
          });
        }
        map.get(r.INDEX_NAME)!.columns.push(r.COLUMN_NAME);
      }
      return [...map.values()];
    } finally {
      await conn.close();
    }
  }

  async getForeignKeys(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]> {
    const conn = await this.pool!.getConnection();
    try {
      const res = await conn.execute<{
        CONSTRAINT_NAME: string;
        COLUMN_NAME: string;
        R_OWNER: string;
        R_TABLE_NAME: string;
        R_COLUMN_NAME: string;
      }>(
        `SELECT fk.constraint_name,
                fkcol.column_name,
                pk.owner       AS r_owner,
                pk.table_name  AS r_table_name,
                pkcol.column_name AS r_column_name
         FROM all_constraints fk
         JOIN all_cons_columns fkcol
           ON fkcol.constraint_name = fk.constraint_name AND fkcol.owner = fk.owner
         JOIN all_constraints pk
           ON pk.constraint_name = fk.r_constraint_name AND pk.owner = fk.r_owner
         JOIN all_cons_columns pkcol
           ON pkcol.constraint_name = pk.constraint_name AND pkcol.owner = pk.owner
              AND pkcol.position = fkcol.position
         WHERE fk.constraint_type = 'R'
           AND fk.owner = :1 AND fk.table_name = :2
         ORDER BY fk.constraint_name, fkcol.position`,
        [schema.toUpperCase(), table.toUpperCase()],
      );

      return (res.rows ?? []).map((r) => ({
        constraintName: r.CONSTRAINT_NAME,
        column: r.COLUMN_NAME,
        referencedSchema: r.R_OWNER,
        referencedTable: r.R_TABLE_NAME,
        referencedColumn: r.R_COLUMN_NAME,
      }));
    } finally {
      await conn.close();
    }
  }

  async getCreateTableDDL(
    _database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const conn = await this.pool!.getConnection();
    try {
      const objRes = await conn.execute<{ OBJECT_TYPE: string }>(
        `SELECT object_type FROM all_objects
         WHERE owner = :1 AND object_name = :2
           AND object_type IN ('TABLE','VIEW') AND ROWNUM = 1`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const objectType = objRes.rows?.[0]?.OBJECT_TYPE ?? "TABLE";

      if (objectType === "VIEW") {
        try {
          const res = await conn.execute<{ DDL: string }>(
            `SELECT DBMS_METADATA.GET_DDL('VIEW', :2, :1) AS ddl FROM dual`,
            [schema.toUpperCase(), table.toUpperCase()],
          );
          const ddl = res.rows?.[0]?.DDL;
          if (typeof ddl === "string" && ddl.trim()) {
            return ddl.trim();
          }
          if (ddl && typeof (ddl as any).getData === "function") {
            const text = ((await (ddl as any).getData()) as string).trim();
            if (text) return text;
          }
        } catch {}
        return await this._fallbackViewDDL(conn, schema, table);
      }

      return await this._fallbackDDL(conn, schema, table);
    } catch {
      try {
        return await this._fallbackDDL(conn, schema, table);
      } catch {
        return `-- DDL not available for "${schema}"."${table}"`;
      }
    } finally {
      await conn.close();
    }
  }

  private async _fallbackViewDDL(
    conn: oracledb.Connection,
    schema: string,
    view: string,
  ): Promise<string> {
    const res = await conn.execute<{ TEXT: any }>(
      `SELECT text FROM all_views WHERE owner = :1 AND view_name = :2`,
      [schema.toUpperCase(), view.toUpperCase()],
      { outFormat: oracledb.OUT_FORMAT_OBJECT },
    );

    const row = res.rows?.[0];
    if (!row || !row.TEXT) {
      return `-- View source not available for "${schema}"."${view}"`;
    }

    const text = await this._consumeLob(row.TEXT);
    const cleanText = typeof text === "string" ? text.trim() : "";

    return `CREATE OR REPLACE VIEW "${schema}"."${view}" AS\n${cleanText}`;
  }

  private async _consumeLob(lob: any): Promise<string | Buffer | null> {
    if (!lob) return null;

    try {
      if (typeof lob === "string" || Buffer.isBuffer(lob)) {
        return lob;
      }

      if (typeof lob.getData === "function") {
        const data = await lob.getData();
        return data;
      }

      return null;
    } finally {
      if (typeof lob.destroy === "function") {
        lob.destroy();
      } else if (typeof lob.close === "function") {
        await lob.close().catch(() => {});
      }
    }
  }

  private async _fallbackDDL(
    conn: oracledb.Connection,
    schema: string,
    table: string,
  ): Promise<string> {
    const OBJ = oracledb.OUT_FORMAT_OBJECT;

    const colRes = await conn.execute<{
      COLUMN_NAME: string;
      DATA_TYPE: string;
      DATA_PRECISION: number | null;
      DATA_SCALE: number | null;
      DATA_LENGTH: number;
      NULLABLE: string;
      DATA_DEFAULT: string | null;
    }>(
      `SELECT column_name, data_type, data_precision, data_scale,
              data_length, nullable, data_default
       FROM all_tab_columns
       WHERE owner = :1 AND table_name = :2
       ORDER BY column_id`,
      [schema.toUpperCase(), table.toUpperCase()],
      { outFormat: OBJ },
    );

    const identityMap = new Map<
      string,
      "ALWAYS" | "BY DEFAULT" | "BY DEFAULT ON NULL"
    >();
    try {
      const idRes = await conn.execute<{
        COLUMN_NAME: string;
        GENERATION_TYPE: string;
      }>(
        `SELECT column_name, generation_type
         FROM all_tab_identity_cols
         WHERE owner = :1 AND table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: OBJ },
      );
      for (const r of idRes.rows ?? []) {
        identityMap.set(
          r.COLUMN_NAME,
          (r.GENERATION_TYPE ?? "ALWAYS") as
            | "ALWAYS"
            | "BY DEFAULT"
            | "BY DEFAULT ON NULL",
        );
      }
    } catch {}

    const pkCols: string[] = [];
    let pkConstraintName = "";
    try {
      const pkRes = await conn.execute<{
        COLUMN_NAME: string;
        CONSTRAINT_NAME: string;
      }>(
        `SELECT cols.column_name, cons.constraint_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1 AND cons.table_name = :2
         ORDER BY cols.position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: OBJ },
      );
      for (const r of pkRes.rows ?? []) {
        pkCols.push(r.COLUMN_NAME);
        pkConstraintName = pkConstraintName || r.CONSTRAINT_NAME;
      }
    } catch {}

    const colLines = (colRes.rows ?? []).map((r) => {
      let typ = r.DATA_TYPE;
      if (r.DATA_PRECISION !== null && r.DATA_SCALE !== null) {
        typ = `${typ}(${r.DATA_PRECISION},${r.DATA_SCALE})`;
      } else if (r.DATA_PRECISION !== null) {
        typ = `${typ}(${r.DATA_PRECISION})`;
      } else if (
        ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"].includes(r.DATA_TYPE)
      ) {
        typ = `${typ}(${r.DATA_LENGTH})`;
      }

      const nullable = r.NULLABLE === "Y" ? "" : " NOT NULL";

      const genType = identityMap.get(r.COLUMN_NAME);
      if (genType !== undefined) {
        const genClause =
          genType === "ALWAYS"
            ? "GENERATED ALWAYS AS IDENTITY"
            : genType === "BY DEFAULT ON NULL"
              ? "GENERATED BY DEFAULT ON NULL AS IDENTITY"
              : "GENERATED BY DEFAULT AS IDENTITY";
        return `  "${r.COLUMN_NAME}" ${typ} ${genClause}${nullable}`;
      }

      let defClause = "";
      if (r.DATA_DEFAULT) {
        const raw = r.DATA_DEFAULT.trim();
        if (!raw.toLowerCase().includes(".nextval")) {
          defClause = ` DEFAULT ${raw}`;
        }
      }

      return `  "${r.COLUMN_NAME}" ${typ}${nullable}${defClause}`;
    });

    if (pkCols.length > 0) {
      const pkColList = pkCols.map((c) => `"${c}"`).join(", ");
      const constraintClause = pkConstraintName
        ? `CONSTRAINT "${pkConstraintName}" PRIMARY KEY (${pkColList})`
        : `PRIMARY KEY (${pkColList})`;
      colLines.push(`  ${constraintClause}`);
    }

    return `CREATE TABLE "${schema}"."${table}" (\n${colLines.join(",\n")}\n);`;
  }

  async getRoutineDefinition(
    _database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
  ): Promise<string> {
    const conn = await this.pool!.getConnection();
    try {
      const res = await conn.execute<{ TEXT: string }>(
        `SELECT text FROM all_source
         WHERE owner = :1 AND name = :2
           AND type IN ('FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY')
         ORDER BY type, line`,
        [schema.toUpperCase(), name.toUpperCase()],
      );
      const lines = (res.rows ?? []).map((r) => r.TEXT);
      if (lines.length === 0) {
        return `-- Source not available for ${schema}.${name}`;
      }
      return `CREATE OR REPLACE ${lines.join("")}`;
    } finally {
      await conn.close();
    }
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    if (params && params.length > 0) {
      return this._execOne(sql, params, start);
    }

    const statements = splitOracleStatements(sql.trim());

    let selectResult: QueryResult | null = null;
    let totalAffected = 0;

    for (const stmt of statements) {
      const result = await this._execOne(stmt, undefined, start);
      totalAffected += result.rowCount;
      if (result.columns.length > 0) {
        selectResult = result;
      }
    }

    if (selectResult) {
      return { ...selectResult, executionTimeMs: Date.now() - start };
    }

    return {
      columns: [],
      rows: [],
      rowCount: totalAffected,
      executionTimeMs: Date.now() - start,
      affectedRows: totalAffected,
    };
  }

  private _fetchTypeHandler(
    metaData: oracledb.Metadata<unknown>,
  ): oracledb.FetchTypeResponse | undefined {
    if (metaData.dbType === oracledb.DB_TYPE_NUMBER) {
      return { type: oracledb.NUMBER };
    }
    if (
      metaData.dbType === oracledb.DB_TYPE_TIMESTAMP ||
      metaData.dbType === oracledb.DB_TYPE_TIMESTAMP_TZ ||
      metaData.dbType === oracledb.DB_TYPE_TIMESTAMP_LTZ
    ) {
      return { type: oracledb.DB_TYPE_TIMESTAMP_LTZ };
    }
    return undefined;
  }

  private async _execOne(
    sql: string,
    params: unknown[] | undefined,
    start: number,
  ): Promise<QueryResult> {
    const conn = await this.pool!.getConnection();
    try {
      let finalSql = sql;
      let binds: unknown[] = [];

      if (params && params.length > 0) {
        const replaced = replacePositionalParams(sql, params);
        finalSql = replaced.sql;
        binds = replaced.binds;
      }

      const options: oracledb.ExecuteOptions = {
        outFormat: oracledb.OUT_FORMAT_ARRAY,
        fetchArraySize: 100,
        autoCommit: true,
        fetchTypeHandler: this._fetchTypeHandler.bind(this),
      };

      const res = await conn.execute(finalSql, binds, options);

      const executionTimeMs = Date.now() - start;

      if (res.metaData && res.rows && res.rows.length > 0) {
        const columns = res.metaData.map((m) => m.name);
        const rows = (res.rows as unknown[][]).map((row) =>
          Object.fromEntries(row.map((val, i) => [`__col_${i}`, val])),
        );
        return { columns, rows, rowCount: rows.length, executionTimeMs };
      }

      if (res.metaData) {
        const columns = res.metaData.map((m) => m.name);
        return {
          columns,
          rows: [],
          rowCount: res.rowsAffected ?? 0,
          executionTimeMs,
        };
      }

      const affectedRows = res.rowsAffected ?? 0;
      return {
        columns: [],
        rows: [],
        rowCount: affectedRows,
        executionTimeMs,
        affectedRows,
      };
    } finally {
      await conn.close();
    }
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const conn = await this.pool!.getConnection();
    try {
      for (const op of operations) {
        let finalSql = op.sql;
        let binds: Record<string, unknown> | unknown[] = {};
        if (op.params && op.params.length > 0) {
          const replaced = replacePositionalParams(op.sql, op.params);
          finalSql = replaced.sql;
          binds = replaced.binds;
        }

        const res = await conn.execute(
          finalSql,
          binds as oracledb.BindParameters,
          {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
            autoCommit: false,
          },
        );

        if (op.checkAffectedRows && (res.rowsAffected ?? 0) === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await conn.commit();
    } catch (e) {
      try {
        await conn.rollback();
      } catch {}
      throw e;
    } finally {
      await conn.close();
    }
  }
}
