import { describe, expect, it } from "vitest";
import {
  parseConnectionFormPanelMessage,
  parseQueryPanelMessage,
  parseSchemaPanelMessage,
  parseTablePanelMessage,
  parseWebviewInitialState,
} from "../../src/shared/webviewContracts";

describe("parseTablePanelMessage export payload", () => {
  it("parses numeric limitToPage for exportCSV", () => {
    const parsed = parseTablePanelMessage({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "contains", value: "alpha" }],
        limitToPage: { page: "2", pageSize: "50" },
      },
    });

    expect(parsed).toEqual({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "contains", value: "alpha" }],
        limitToPage: { page: 2, pageSize: 50 },
      },
    });
  });

  it("drops invalid limitToPage values for exportJSON", () => {
    const parsed = parseTablePanelMessage({
      type: "exportJSON",
      payload: {
        filters: [],
        limitToPage: { page: "nan", pageSize: 25 },
      },
    });

    expect(parsed).toEqual({
      type: "exportJSON",
      payload: {
        sort: undefined,
        filters: [],
        limitToPage: undefined,
      },
    });
  });

  it("drops non-positive and fractional limitToPage values", () => {
    const zeroPage = parseTablePanelMessage({
      type: "exportCSV",
      payload: {
        limitToPage: { page: 0, pageSize: 25 },
      },
    });

    expect(zeroPage).toEqual({
      type: "exportCSV",
      payload: {
        sort: undefined,
        filters: undefined,
        limitToPage: undefined,
      },
    });

    const fractionalPageSize = parseTablePanelMessage({
      type: "exportJSON",
      payload: {
        limitToPage: { page: 1, pageSize: 25.5 },
      },
    });

    expect(fractionalPageSize).toEqual({
      type: "exportJSON",
      payload: {
        sort: undefined,
        filters: undefined,
        limitToPage: undefined,
      },
    });
  });
});

describe("parseTablePanelMessage applyChanges payload", () => {
  it("parses updates and insertValues together", () => {
    const parsed = parseTablePanelMessage({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: { name: "New user" },
      },
    });

    expect(parsed).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: { name: "New user" },
      },
    });
  });

  it("rejects invalid insertValues", () => {
    const parsed = parseTablePanelMessage({
      type: "applyChanges",
      payload: {
        updates: [],
        insertValues: "invalid",
      },
    });

    expect(parsed).toBeNull();
  });
});

describe("parseQueryPanelMessage", () => {
  it("parses executeQuery payloads", () => {
    const parsed = parseQueryPanelMessage({
      type: "executeQuery",
      payload: {
        sql: "select 1",
        connectionId: "conn-1",
      },
    });

    expect(parsed).toEqual({
      type: "executeQuery",
      payload: {
        sql: "select 1",
        connectionId: "conn-1",
      },
    });
  });

  it("rejects malformed executeQuery payloads", () => {
    const parsed = parseQueryPanelMessage({
      type: "executeQuery",
      payload: {
        sql: 123,
      },
    });

    expect(parsed).toBeNull();
  });
});

describe("parseSchemaPanelMessage", () => {
  it("parses openRelatedSchema payloads", () => {
    const parsed = parseSchemaPanelMessage({
      type: "openRelatedSchema",
      payload: {
        table: "orders",
        schema: "public",
        database: "app_db",
      },
    });

    expect(parsed).toEqual({
      type: "openRelatedSchema",
      payload: {
        table: "orders",
        schema: "public",
        database: "app_db",
      },
    });
  });

  it("rejects malformed openRelatedSchema payloads", () => {
    const parsed = parseSchemaPanelMessage({
      type: "openRelatedSchema",
      payload: {
        schema: "public",
      },
    });

    expect(parsed).toBeNull();
  });
});

describe("parseConnectionFormPanelMessage", () => {
  it("parses saveConnection payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        password: "secret",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        port: undefined,
        database: undefined,
        username: undefined,
        filePath: undefined,
        ssl: undefined,
        rejectUnauthorized: undefined,
        folder: undefined,
        serviceName: undefined,
        thickMode: undefined,
        clientPath: undefined,
        useSecretStorage: undefined,
        password: "secret",
        hasStoredSecret: undefined,
      },
    });
  });

  it("rejects malformed saveConnection payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-1",
        name: "Primary",
        type: "postgres",
        password: 123,
      },
    });

    expect(parsed).toBeNull();
  });
});

describe("parseWebviewInitialState", () => {
  it("parses a valid query state", () => {
    const parsed = parseWebviewInitialState({
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      initialSql: "select 1",
      formatOnOpen: true,
      isBookmarked: false,
    });

    expect(parsed).toEqual({
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      initialSql: "select 1",
      formatOnOpen: true,
      isBookmarked: false,
    });
  });

  it("supports empty connectionType for query state", () => {
    const parsed = parseWebviewInitialState({
      view: "query",
      connectionId: "conn-1",
      connectionType: "",
    });

    expect(parsed).toEqual({
      view: "query",
      connectionId: "conn-1",
      connectionType: "",
      initialSql: undefined,
      formatOnOpen: undefined,
      isBookmarked: undefined,
    });
  });

  it("coerces numeric string fields in table state", () => {
    const parsed = parseWebviewInitialState({
      view: "table",
      connectionId: "conn-1",
      database: "main",
      schema: "public",
      table: "users",
      defaultPageSize: "100",
    });

    expect(parsed).toEqual({
      view: "table",
      connectionId: "conn-1",
      database: "main",
      schema: "public",
      table: "users",
      isView: undefined,
      defaultPageSize: 100,
    });
  });

  it("parses a valid schema state", () => {
    const parsed = parseWebviewInitialState({
      view: "schema",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      table: "users",
    });

    expect(parsed).toEqual({
      view: "schema",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      table: "users",
    });
  });

  it("parses a valid connection state", () => {
    const parsed = parseWebviewInitialState({
      view: "connection",
      existing: {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        hasStoredSecret: true,
      },
    });

    expect(parsed).toEqual({
      view: "connection",
      existing: {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        port: undefined,
        database: undefined,
        username: undefined,
        filePath: undefined,
        ssl: undefined,
        rejectUnauthorized: undefined,
        folder: undefined,
        serviceName: undefined,
        thickMode: undefined,
        clientPath: undefined,
        useSecretStorage: undefined,
        hasStoredSecret: true,
      },
    });
  });

  it("returns null for invalid query state", () => {
    const parsed = parseWebviewInitialState({
      view: "query",
      connectionType: "pg",
    });

    expect(parsed).toBeNull();
  });

  it("returns null for invalid connectionType", () => {
    const parsed = parseWebviewInitialState({
      view: "query",
      connectionId: "conn-1",
      connectionType: "invalid",
    });

    expect(parsed).toBeNull();
  });
});
