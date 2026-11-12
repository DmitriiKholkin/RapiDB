import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import { ErdGraphService } from "../../../src/extension/services/erdGraphService";

const drivers: SQLiteCoreDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

const SCHEMA = `
CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, deleted_at TEXT);
CREATE TABLE profiles (
  id INTEGER PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  owner_id INTEGER REFERENCES users(id),
  plain_id INTEGER REFERENCES users(id),
  solo_id INTEGER REFERENCES users(id),
  pair_a INTEGER,
  pair_b INTEGER,
  email TEXT,
  UNIQUE (pair_a, pair_b)
);
CREATE UNIQUE INDEX profiles_user_active ON profiles(user_id) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX profiles_owner_expr ON profiles(owner_id, (owner_id + 0));
CREATE UNIQUE INDEX "profiles lower email" ON profiles(lower(email) DESC);
CREATE INDEX profiles_mixed ON profiles(pair_a DESC, abs(pair_b) /* c */, email COLLATE NOCASE ASC);
CREATE INDEX profiles_plain ON profiles(plain_id);
CREATE UNIQUE INDEX profiles_solo ON profiles(solo_id);
`;

async function connect(mode: "native" | "worker") {
  const config = {
    id: `sqlite-index-metadata-${mode}`,
    name: "SQLite index metadata",
    type: "sqlite" as const,
    filePath: ":memory:",
  };
  const driver =
    mode === "native" ? new SQLiteCoreDriver(config) : new SQLiteDriver(config);
  drivers.push(driver);
  await driver.connect();
  await driver.query(SCHEMA);
  return { config, driver };
}

describe.each([
  "native",
  "worker",
] as const)("real %s SQLite index metadata", (mode) => {
  it("keeps keyword-column operands and the complete expression key text", async () => {
    const { driver } = await connect(mode);
    await driver.query(`
      CREATE TABLE keyword_terms(a INT, asc INT, desc INT);
      CREATE INDEX term_desc ON keyword_terms(a + desc);
      CREATE INDEX term_asc ON keyword_terms(a + asc);
      CREATE INDEX term_order ON keyword_terms(a + desc DESC);
      CREATE INDEX term_asc_order ON keyword_terms(a + asc ASC);
      CREATE INDEX term_parens ON keyword_terms((a + desc) DESC);
      CREATE INDEX term_quoted ON keyword_terms(a + "desc");
    `);
    const indexes = await driver.getIndexes("", "main", "keyword_terms");
    const terms = Object.fromEntries(
      indexes.map((index) => [index.name, index.columns]),
    );
    expect(terms).toEqual({
      term_desc: ["a + desc"],
      term_asc: ["a + asc"],
      term_order: ["a + desc DESC"],
      term_asc_order: ["a + asc ASC"],
      term_parens: ["(a + desc) DESC"],
      term_quoted: ['a + "desc"'],
    });
    expect(indexes.every((index) => index.expression)).toBe(true);
  });

  it("reports partial and expression indexes without null columns", async () => {
    const { driver } = await connect(mode);
    const indexes = await driver.getIndexes("", "main", "profiles");
    const byName = new Map(indexes.map((index) => [index.name, index]));

    expect(byName.get("profiles_user_active")).toEqual({
      name: "profiles_user_active",
      columns: ["user_id"],
      unique: true,
      primary: false,
      partial: true,
    });
    expect(byName.get("profiles lower email")).toEqual({
      name: "profiles lower email",
      columns: ["lower(email) DESC"],
      unique: true,
      primary: false,
      expression: true,
    });
    expect(byName.get("profiles_owner_expr")).toEqual({
      name: "profiles_owner_expr",
      columns: ["owner_id", "(owner_id + 0)"],
      unique: true,
      primary: false,
      expression: true,
    });
    expect(byName.get("profiles_mixed")).toEqual({
      name: "profiles_mixed",
      columns: ["pair_a", "abs(pair_b)", "email"],
      unique: false,
      primary: false,
      expression: true,
    });
    expect(byName.get("profiles_plain")).toEqual({
      name: "profiles_plain",
      columns: ["plain_id"],
      unique: false,
      primary: false,
    });
    expect(byName.get("sqlite_autoindex_profiles_1")).toEqual({
      name: "sqlite_autoindex_profiles_1",
      columns: ["pair_a", "pair_b"],
      unique: true,
      primary: false,
    });
    for (const index of indexes) {
      expect(index.columns.every((column) => typeof column === "string")).toBe(
        true,
      );
    }
  });

  it("derives UNIQUE constraints only from total column indexes", async () => {
    const { driver } = await connect(mode);
    const unique = (await driver.getConstraints("", "main", "profiles"))
      .filter((constraint) => constraint.kind === "unique")
      .map((constraint) => [constraint.name, constraint.columns]);

    expect(unique).toEqual([
      ["profiles_solo", ["solo_id"]],
      ["sqlite_autoindex_profiles_1", ["pair_a", "pair_b"]],
    ]);
  });

  it("keeps the stored DDL for partial and expression indexes", async () => {
    const { driver } = await connect(mode);

    await expect(
      driver.getIndexDDL("", "main", "profiles", "profiles_user_active"),
    ).resolves.toBe(
      "CREATE UNIQUE INDEX profiles_user_active ON profiles(user_id) WHERE user_id IS NOT NULL;",
    );
    await expect(
      driver.getIndexDDL("", "main", "profiles", "profiles lower email"),
    ).resolves.toBe(
      'CREATE UNIQUE INDEX "profiles lower email" ON profiles(lower(email) DESC);',
    );
  });

  it("does not draw one-to-one edges from partial or expression unique indexes", async () => {
    const { config, driver } = await connect(mode);
    const event = vi.fn(() => ({ dispose: () => undefined }));
    const service = new ErdGraphService({
      getConnection: vi.fn(() => config),
      getDriver: vi.fn(() => driver),
      getSchemaSnapshotAsync: vi.fn(async () => ({
        databases: [
          {
            name: "",
            schemas: [
              {
                name: "main",
                objects: ["profiles", "users"].map((name) => ({
                  name,
                  type: "table",
                  columns: [],
                })),
              },
            ],
          },
        ],
      })),
      onDidDisconnect: event,
      onDidRefreshSchemas: event,
    } as never);
    try {
      const result = await service.getGraph({
        connectionId: config.id,
        database: "",
      });
      const cardinality = Object.fromEntries(
        result.graph.edges.map((edge) => [edge.fromColumn, edge.cardinality]),
      );
      expect(cardinality).toEqual({
        user_id: "many-to-one",
        owner_id: "many-to-one",
        plain_id: "many-to-one",
        solo_id: "one-to-one",
      });
    } finally {
      service.dispose();
    }
  });
});
