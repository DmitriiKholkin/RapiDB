import { describe, expect, it } from "vitest";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";
import {
  parseConnectionFormPanelMessage,
  parseErdPanelMessage,
  parseQueryPanelMessage,
  parseTablePanelMessage,
  parseWebviewInitialState,
} from "../../src/shared/webviewContracts";

describe("connection port contract", () => {
  const base = {
    id: "ports",
    name: "Ports",
    type: "pg",
    host: "localhost",
    database: "app",
    username: "user",
  };
  it.each([
    NaN,
    Infinity,
    -Infinity,
    "Infinity",
    "1e999",
    "abc",
    "22abc",
    "",
    "  ",
    null,
    false,
    {},
    [],
  ])("rejects malformed ports without dropping them: %j", (port) => {
    for (const fields of [{ port }, { ssh: { port } }]) {
      for (const type of ["saveConnection", "testConnection"]) {
        expect(
          parseConnectionFormPanelMessage({
            type,
            payload: { ...base, ...fields },
          }),
        ).toBeNull();
      }
      expect(
        parseWebviewInitialState({
          view: "connection",
          existing: { ...base, ...fields },
        }),
      ).toBeNull();
    }
  });
  it.each([
    0, -1, 1.5, 65536, 70000,
  ])("retains finite invalid ports for semantic validation: %s", (port) => {
    for (const type of ["saveConnection", "testConnection"] as const) {
      const parsed = parseConnectionFormPanelMessage({
        type,
        payload: { ...base, port, ssh: { port } },
      });
      expect(parsed).toMatchObject({ payload: { port, ssh: { port } } });
      if (
        parsed?.type !== "saveConnection" &&
        parsed?.type !== "testConnection"
      )
        throw new Error("Missing submission");
      if (!parsed.payload) throw new Error("Missing payload");
      expect(validateConnectionConfig(parsed.payload).issues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ code: "invalid", fields: ["port"] }),
          expect.objectContaining({ code: "invalid", fields: ["ssh"] }),
        ]),
      );
    }
  });
  it.each([
    undefined,
    1,
    65535,
    "5432",
  ])("accepts absent or numeric ports: %s", (port) => {
    for (const type of ["saveConnection", "testConnection"]) {
      expect(
        parseConnectionFormPanelMessage({
          type,
          payload: { ...base, port, ssh: { port } },
        }),
      ).toMatchObject({
        payload: {
          port: port === undefined ? undefined : Number(port),
          ssh: { port: port === undefined ? undefined : Number(port) },
        },
      });
      expect(
        parseConnectionFormPanelMessage({ type, payload: base }),
      ).not.toBeNull();
    }
  });
});

describe("connection TLS contract", () => {
  const base = { id: "tls", name: "TLS", type: "pg" };
  const parsers = [
    (payload: unknown) =>
      parseConnectionFormPanelMessage({ type: "saveConnection", payload }),
    (payload: unknown) =>
      parseConnectionFormPanelMessage({ type: "testConnection", payload }),
    (existing: unknown) =>
      parseWebviewInitialState({ view: "connection", existing }),
  ];

  it.each([
    null,
    false,
    42,
    "requireVerifyFull",
    [],
    {},
    ...[
      undefined,
      null,
      false,
      1,
      {},
      [],
      "",
      "unknown",
      "requireVerifyFull ",
      " requireVerifyFull",
      "\t",
    ].map((mode) => ({ mode, caFilePath: "/certs/ca.pem" })),
    ...[
      "caFilePath",
      "certFilePath",
      "keyFilePath",
      "keyPassphrase",
      "serverNameOverride",
    ].flatMap((field) =>
      [null, false, 42, {}, []].map((value) => ({
        mode: "requireVerifyFull",
        [field]: value,
      })),
    ),
  ])("rejects the entire save/test/edit state for malformed TLS %j", (tls) => {
    for (const parse of parsers) {
      expect(parse({ ...base, tls })).toBeNull();
    }
  });

  it.each([
    "disabled",
    "requireTrustServerCertificate",
    "requireVerifyCa",
    "requireVerifyFull",
    "mutualTls",
  ])("preserves supported mode %s and TLS strings verbatim", (mode) => {
    const tls = {
      mode,
      caFilePath: " /certs/my CA.pem ",
      certFilePath: "",
      keyFilePath: "C:\\certs\\client.key",
      keyPassphrase: " secret ",
      serverNameOverride: "db.example.com",
    };
    for (const parse of parsers) {
      const result = parse({ ...base, tls });
      expect(result).not.toBeNull();
      expect(result).toEqual(
        expect.objectContaining(
          result && "existing" in result
            ? { existing: expect.objectContaining({ tls }) }
            : { payload: expect.objectContaining({ tls }) },
        ),
      );
    }
  });

  it("accepts omitted and undefined TLS and optional TLS fields", () => {
    for (const parse of parsers) {
      for (const fields of [
        {},
        { tls: undefined },
        { tls: { mode: "requireVerifyFull", caFilePath: undefined } },
      ]) {
        expect(parse({ ...base, ...fields })).not.toBeNull();
      }
    }
    expect(
      parseWebviewInitialState({ view: "connection", existing: null }),
    ).not.toBeNull();
  });
});

describe.each([
  "exportResultsCSV",
  "exportResultsJSON",
])("%s payload", (type) => {
  it("accepts positional column IDs and an omitted payload", () => {
    const payload = {
      columnOrder: ["__col_1", "__col_0"],
      sort: [
        { id: "__col_0", desc: false },
        { id: "__col_1", desc: true },
      ],
    };
    expect(parseQueryPanelMessage({ type, payload })).toEqual({
      type,
      payload,
    });
    expect(parseQueryPanelMessage({ type })).toEqual({ type, payload: {} });
    expect(
      parseQueryPanelMessage({ type, payload: { sort: [], columnOrder: [] } }),
    ).toEqual({ type, payload: { sort: [], columnOrder: [] } });
  });

  it.each([
    "invalid",
    [],
    { columnOrder: "__col_0" },
    { columnOrder: ["name"] },
    { columnOrder: [0] },
    { columnOrder: ["__col_-1"] },
    { columnOrder: ["__col_1.5"] },
    { columnOrder: ["__col_01"] },
    { columnOrder: ["__col_9007199254740992"] },
    { columnOrder: ["__col_0", "__col_0"] },
    { sort: {} },
    { sort: [null] },
    { sort: [{ column: "name", desc: false }] },
    { sort: [{ id: "name", desc: false }] },
    { sort: [{ id: "__col_0", desc: "false" }] },
    { sort: [{ id: "__col_0" }] },
    {
      sort: [
        { id: "__col_0", desc: false },
        { id: "__col_0", desc: true },
      ],
    },
  ])("rejects malformed export options: %j", (payload) => {
    expect(parseQueryPanelMessage({ type, payload })).toBeNull();
  });
});

describe("parseTablePanelMessage export payload", () => {
  it.each([
    "exportCSV",
    "exportJSON",
  ])("preserves an explicit empty selection for %s so the host can fail closed", (type) => {
    expect(
      parseTablePanelMessage({ type, payload: { columnOrder: [] } }),
    ).toMatchObject({ type, payload: { columnOrder: [] } });
  });

  it.each([
    { columnOrder: null },
    { columnOrder: "id" },
    { columnOrder: [1] },
    { columnOrder: ["id", null] },
  ])("rejects a malformed table export selection $columnOrder rather than falling back to all columns", ({
    columnOrder,
  }) => {
    expect(
      parseTablePanelMessage({ type: "exportCSV", payload: { columnOrder } }),
    ).toBeNull();
  });

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
        operationId: "mutation-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: [{ name: "New user" }],
      },
    });

    expect(parsed).toEqual({
      type: "applyChanges",
      payload: {
        operationId: "mutation-1",
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: [{ name: "New user" }],
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

describe("parseConnectionFormPanelMessage cancellation", () => {
  it("parses cancelTestConnection without a payload", () => {
    expect(
      parseConnectionFormPanelMessage({ type: "cancelTestConnection" }),
    ).toEqual({ type: "cancelTestConnection" });
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
    const parsed = parseTablePanelMessage({
      type: "readClipboard",
      payload: { requestId: "request-1", recipient: "editor-1" },
    });

    expect(parsed).toEqual({
      type: "readClipboard",
      payload: { requestId: "request-1", recipient: "editor-1" },
    });
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
        tls: undefined,
        folder: undefined,
        serviceName: undefined,
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
        ssh: {
          host: "bastion.example.com",
          port: "22",
          username: "tunnel",
          authMethod: "privateKey",
          hostVerificationMode: "manual",
          privateKey:
            "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
          passphrase: "key-passphrase",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-ssh",
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          hostVerificationMode: "manual",
          password: undefined,
          privateKey:
            "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
          passphrase: "key-passphrase",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      }),
    });
  });

  it("parses TLS configuration payloads", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-tls",
        name: "TLS Primary",
        type: "pg",
        host: "db.internal",
        database: "app",
        username: "postgres",
        tls: {
          mode: "mutualTls",
          caFilePath: "/tmp/ca.pem",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "tls-passphrase",
          serverNameOverride: "db.example.com",
        },
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-tls",
        tls: {
          mode: "mutualTls",
          caFilePath: "/tmp/ca.pem",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "tls-passphrase",
          serverNameOverride: "db.example.com",
        },
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
        ssh: {
          host: "bastion.example.com",
          port: "22",
          username: "tunnel",
          authMethod: "password",
          hostVerificationMode: "trustOnFirstUse",
          password: "ssh-secret",
        },
      },
    });

    expect(parsed).toEqual({
      type: "testConnection",
      payload: expect.objectContaining({
        id: "conn-ssh-tofu",
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "password",
          hostVerificationMode: "trustOnFirstUse",
          hostFingerprintSha256: undefined,
          password: "ssh-secret",
          privateKey: undefined,
          passphrase: undefined,
        },
      }),
    });
  });

  it("parses targeted browse-file messages", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "browseFile",
      payload: {
        target: "tlsKeyFile",
      },
    });

    expect(parsed).toEqual({
      type: "browseFile",
      payload: {
        target: "tlsKeyFile",
      },
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
      payload: expect.objectContaining({
        id: "conn-nosql",
        name: "Mongo Local",
        type: "mongodb",
        connectionUri: "mongodb://localhost:27017/app",
        endpoint: "http://localhost:8000",
        uri: "mongodb://localhost:27017/app",
        authSource: "admin",
        awsEndpoint: "http://localhost:8000",
      }),
    });
    expect(parsed?.payload).not.toHaveProperty("authDatabase");
    expect(parsed?.payload).not.toHaveProperty("redisDb");
    expect(parsed?.payload).not.toHaveProperty("keyPrefix");
  });

  it("normalizes Oracle legacy database field to serviceName", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-oracle-legacy",
        name: "Oracle Legacy",
        type: "oracle",
        host: "localhost",
        database: "XEPDB1",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-oracle-legacy",
        type: "oracle",
        database: undefined,
        serviceName: "XEPDB1",
      }),
    });
  });

  it("prefers explicit Oracle serviceName over legacy database", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-oracle-modern",
        name: "Oracle Modern",
        type: "oracle",
        host: "localhost",
        database: "LEGACY",
        serviceName: "FREEPDB1",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-oracle-modern",
        type: "oracle",
        database: undefined,
        serviceName: "FREEPDB1",
      }),
    });
  });

  it("strips whitespace when normalizing Oracle serviceName from database field", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-oracle-ws",
        name: "Oracle Whitespace",
        type: "oracle",
        host: "localhost",
        database: "  XEPDB1  ",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "oracle",
        database: undefined,
        serviceName: "XEPDB1",
      }),
    });
  });

  it("yields undefined serviceName when both database and serviceName are absent for Oracle", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-oracle-empty",
        name: "Oracle No SN",
        type: "oracle",
        host: "localhost",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "oracle",
        database: undefined,
        serviceName: undefined,
      }),
    });
  });

  it("does not alter database field for non-Oracle connection types", () => {
    const parsed = parseConnectionFormPanelMessage({
      type: "saveConnection",
      payload: {
        id: "conn-pg-db",
        name: "PG with DB",
        type: "pg",
        host: "localhost",
        database: "mydb",
      },
    });

    expect(parsed).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "pg",
        database: "mydb",
        serviceName: undefined,
      }),
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
        tls: undefined,
        folder: undefined,
        serviceName: undefined,
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

  it("normalizes Oracle existing state database to serviceName", () => {
    const parsed = parseWebviewInitialState({
      view: "connection",
      existing: {
        id: "conn-oracle-existing",
        name: "Oracle Existing",
        type: "oracle",
        host: "localhost",
        database: "XEPDB1",
      },
    });

    expect(parsed).toEqual({
      view: "connection",
      existing: expect.objectContaining({
        id: "conn-oracle-existing",
        type: "oracle",
        database: undefined,
        serviceName: "XEPDB1",
      }),
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
