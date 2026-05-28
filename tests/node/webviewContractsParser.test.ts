import { describe, expect, it } from "vitest";
import {
  parseConnectionFormPanelMessage,
  parseErdPanelMessage,
  parseQueryPanelMessage,
  parseTablePanelMessage,
  parseWebviewInitialState,
} from "../../src/shared/webviewContracts";

describe("parseTablePanelMessage export payload", () => {
  it("parses numeric limitToPage for exportCSV", () => {
    const parsed = parseTablePanelMessage({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "like", value: "alpha" }],
        limitToPage: { page: "2", pageSize: "50" },
      },
    });

    expect(parsed).toEqual({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "like", value: "alpha" }],
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
  it("parses executeQuery payloads with canonical queryText and sql alias", () => {
    const parsed = parseQueryPanelMessage({
      type: "executeQuery",
      payload: {
        queryText: "select 1",
        connectionId: "conn-1",
      },
    });

    expect(parsed).toEqual({
      type: "executeQuery",
      payload: {
        queryText: "select 1",
        sql: "select 1",
        connectionId: "conn-1",
      },
    });
  });

  it("accepts sql as an alias for executeQuery payloads", () => {
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
        queryText: "select 1",
        sql: "select 1",
        connectionId: "conn-1",
      },
    });
  });

  it("rejects malformed executeQuery payloads", () => {
    const parsed = parseQueryPanelMessage({
      type: "executeQuery",
      payload: {
        queryText: 123,
      },
    });

    expect(parsed).toBeNull();
  });

  it("parses writeClipboard payload", () => {
    const parsed = parseQueryPanelMessage({
      type: "writeClipboard",
      payload: { text: "copied text" },
    });

    expect(parsed).toEqual({
      type: "writeClipboard",
      payload: { text: "copied text" },
    });
  });
});

describe("parseTablePanelMessage clipboard payload", () => {
  it("parses readClipboard message", () => {
    const parsed = parseTablePanelMessage({ type: "readClipboard" });

    expect(parsed).toEqual({ type: "readClipboard" });
  });

  it("parses writeClipboard payload", () => {
    const parsed = parseTablePanelMessage({
      type: "writeClipboard",
      payload: { text: "structured content" },
    });

    expect(parsed).toEqual({
      type: "writeClipboard",
      payload: { text: "structured content" },
    });
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
        readOnly: undefined,
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
        connectionUri: undefined,
        authDatabase: undefined,
        replicaSet: undefined,
        directConnection: undefined,
        redisUsername: undefined,
        keyPrefix: undefined,
        awsProfile: undefined,
        endpoint: undefined,
        apiKey: undefined,
        cloudId: undefined,
        uri: undefined,
        authSource: undefined,
        redisDb: undefined,
        awsRegion: undefined,
        awsAccessKeyId: undefined,
        awsSecretAccessKey: undefined,
        awsSessionToken: undefined,
        awsEndpoint: undefined,
        useSecretStorage: undefined,
        password: "secret",
        hasStoredSecret: undefined,
      },
    });
  });

  it("parses SSH fields and stored-secret presence flags in connection payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-ssh",
        name: "SSH Primary",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: "22",
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshHostVerificationMode: "manual",
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "key-passphrase",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-ssh",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "privateKey",
        sshHostVerificationMode: "manual",
        sshPrivateKey:
          "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
        sshPassphrase: "key-passphrase",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      }),
    });
  });

  it("parses trust-on-first-use SSH submissions without a manual fingerprint", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "testConnection",
      payload: {
        id: "conn-ssh-tofu",
        name: "SSH TOFU",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: "22",
        sshUsername: "tunnel",
        sshAuthMethod: "password",
        sshHostVerificationMode: "trustOnFirstUse",
        sshPassword: "ssh-secret",
      },
    });

    expect(parsed).toEqual({
      type: "testConnection",
      payload: expect.objectContaining({
        id: "conn-ssh-tofu",
        sshEnabled: true,
        sshHostVerificationMode: "trustOnFirstUse",
        sshHostFingerprintSha256: undefined,
        sshPassword: "ssh-secret",
      }),
    });
  });

  it("normalizes alias fields for new NoSQL connection payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-nosql",
        name: "Mongo Local",
        type: "mongodb",
        uri: "mongodb://localhost:27017/app",
        authSource: "admin",
        awsEndpoint: "http://localhost:8000",
        redisDb: 2,
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: {
        id: "conn-nosql",
        name: "Mongo Local",
        type: "mongodb",
        readOnly: undefined,
        host: undefined,
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
        connectionUri: "mongodb://localhost:27017/app",
        authDatabase: "admin",
        replicaSet: undefined,
        directConnection: undefined,
        redisUsername: undefined,
        keyPrefix: undefined,
        awsProfile: undefined,
        endpoint: "http://localhost:8000",
        apiKey: undefined,
        cloudId: undefined,
        uri: "mongodb://localhost:27017/app",
        authSource: "admin",
        redisDb: 2,
        awsRegion: undefined,
        awsAccessKeyId: undefined,
        awsSecretAccessKey: undefined,
        awsSessionToken: undefined,
        awsEndpoint: "http://localhost:8000",
        useSecretStorage: undefined,
        password: undefined,
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

  it("parses readonly flags in connection payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-ro",
        name: "Readonly",
        type: "pg",
        readOnly: true,
      },
    });

    expect(parsed).toMatchObject({
      type: "saveConnection",
      payload: { id: "conn-ro", readOnly: true },
    });
  });

  it("defaults sqlite WAL mode to auto in connection submissions", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-sqlite-auto",
        name: "SQLite Auto WAL",
        type: "sqlite",
        filePath: "/tmp/sqlite-auto.db",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-sqlite-auto",
        type: "sqlite",
        filePath: "/tmp/sqlite-auto.db",
        sqliteWalMode: "auto",
      }),
    });
  });

  it("preserves explicit sqlite WAL mode selections in connection submissions", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "testConnection",
      payload: {
        id: "conn-sqlite-off",
        name: "SQLite WAL Off",
        type: "sqlite",
        filePath: "/tmp/sqlite-off.db",
        sqliteWalMode: "off",
      },
    });

    expect(parsed).toEqual({
      type: "testConnection",
      payload: expect.objectContaining({
        id: "conn-sqlite-off",
        type: "sqlite",
        sqliteWalMode: "off",
      }),
    });
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
      editorLanguage: "sql",
    });

    expect(parsed).toEqual({
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      queryText: "select 1",
      initialSql: "select 1",
      formatOnOpen: true,
      isBookmarked: false,
      editorLanguage: "sql",
      editorPresentation: {
        queryMode: undefined,
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: undefined,
        allowFormatting: undefined,
      },
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
      queryText: undefined,
      initialSql: undefined,
      formatOnOpen: undefined,
      isBookmarked: undefined,
      editorLanguage: undefined,
      editorPresentation: undefined,
    });
  });

  it("parses NoSQL editor language overrides for query state", () => {
    expect(
      parseWebviewInitialState({
        view: "query",
        connectionId: "conn-js",
        connectionType: "mongodb",
        editorLanguage: "javascript",
      }),
    ).toEqual({
      view: "query",
      connectionId: "conn-js",
      connectionType: "mongodb",
      initialSql: undefined,
      formatOnOpen: undefined,
      isBookmarked: undefined,
      editorLanguage: "javascript",
      editorPresentation: {
        formatOnOpen: undefined,
        editorLanguage: "javascript",
        sqlDialect: undefined,
        allowFormatting: undefined,
      },
    });

    expect(
      parseWebviewInitialState({
        view: "query",
        connectionId: "conn-text",
        connectionType: "elasticsearch",
        editorLanguage: "plaintext",
      }),
    ).toEqual({
      view: "query",
      connectionId: "conn-text",
      connectionType: "elasticsearch",
      initialSql: undefined,
      formatOnOpen: undefined,
      isBookmarked: undefined,
      editorLanguage: "plaintext",
      editorPresentation: {
        formatOnOpen: undefined,
        editorLanguage: "plaintext",
        sqlDialect: undefined,
        allowFormatting: undefined,
      },
    });
  });

  it("prefers the shared editorPresentation contract while keeping top-level query fields populated", () => {
    const parsed = parseWebviewInitialState({
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      editorPresentation: {
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      },
      formatOnOpen: false,
      editorLanguage: "plaintext",
    });

    expect(parsed).toEqual({
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      initialSql: undefined,
      formatOnOpen: true,
      isBookmarked: undefined,
      editorLanguage: "sql",
      editorPresentation: {
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
        allowFormatting: undefined,
      },
    });
  });

  it("parses query editor formatting capability overrides", () => {
    expect(
      parseWebviewInitialState({
        view: "query",
        connectionId: "conn-ddb",
        connectionType: "dynamodb",
        editorPresentation: {
          formatOnOpen: false,
          editorLanguage: "sql",
          sqlDialect: "sql",
          allowFormatting: false,
        },
      }),
    ).toEqual({
      view: "query",
      connectionId: "conn-ddb",
      connectionType: "dynamodb",
      initialSql: undefined,
      formatOnOpen: false,
      isBookmarked: undefined,
      editorLanguage: "sql",
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "sql",
        sqlDialect: "sql",
        allowFormatting: false,
      },
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
      connectionReadOnly: undefined,
      defaultPageSize: 100,
    });
  });

  it("parses readonly flags in table state", () => {
    const parsed = parseWebviewInitialState({
      view: "table",
      connectionId: "conn-ro",
      database: "main",
      schema: "public",
      table: "users",
      connectionReadOnly: true,
    });

    expect(parsed).toEqual({
      view: "table",
      connectionId: "conn-ro",
      database: "main",
      schema: "public",
      table: "users",
      isView: undefined,
      connectionReadOnly: true,
      defaultPageSize: undefined,
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
        readOnly: undefined,
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
        connectionUri: undefined,
        authDatabase: undefined,
        replicaSet: undefined,
        directConnection: undefined,
        redisUsername: undefined,
        keyPrefix: undefined,
        awsProfile: undefined,
        endpoint: undefined,
        apiKey: undefined,
        cloudId: undefined,
        uri: undefined,
        authSource: undefined,
        redisDb: undefined,
        awsRegion: undefined,
        awsAccessKeyId: undefined,
        awsSecretAccessKey: undefined,
        awsSessionToken: undefined,
        awsEndpoint: undefined,
        useSecretStorage: undefined,
        hasStoredSecret: true,
      },
    });
  });

  it("defaults sqlite WAL mode to auto in existing connection state", () => {
    const parsed = parseWebviewInitialState({
      view: "connection",
      existing: {
        id: "conn-sqlite-existing",
        name: "SQLite Existing",
        type: "sqlite",
        filePath: "/tmp/sqlite-existing.db",
      },
    });

    expect(parsed).toEqual({
      view: "connection",
      existing: expect.objectContaining({
        id: "conn-sqlite-existing",
        type: "sqlite",
        filePath: "/tmp/sqlite-existing.db",
        sqliteWalMode: "auto",
      }),
    });
  });

  it("parses a valid erd state", () => {
    const parsed = parseWebviewInitialState({
      view: "erd",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(parsed).toEqual({
      view: "erd",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
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

describe("parseErdPanelMessage", () => {
  it("parses a reload message", () => {
    const parsed = parseErdPanelMessage({ type: "reload" });
    expect(parsed).toEqual({ type: "reload" });
  });

  it("parses openTableData payloads", () => {
    const parsed = parseErdPanelMessage({
      type: "openTableData",
      payload: {
        table: "orders",
        schema: "public",
        database: "app_db",
        isView: false,
      },
    });

    expect(parsed).toEqual({
      type: "openTableData",
      payload: {
        table: "orders",
        schema: "public",
        database: "app_db",
        isView: false,
      },
    });
  });

  it("rejects malformed openTableData payloads", () => {
    const parsed = parseErdPanelMessage({
      type: "openTableData",
      payload: {
        schema: "public",
      },
    });

    expect(parsed).toBeNull();
  });
});
