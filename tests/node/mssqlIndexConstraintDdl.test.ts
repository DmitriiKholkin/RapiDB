import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";

type Inputs = Map<string, unknown>;

const indexListRows = [
  {
    idx_name: "PK_users",
    col_name: "id",
    is_unique: 1,
    is_pk: 1,
    has_filter: 0,
  },
  {
    idx_name: "ux_users_email",
    col_name: "email",
    is_unique: 1,
    is_pk: 0,
    has_filter: 1,
  },
  {
    idx_name: "uq_users_login",
    col_name: "login",
    is_unique: 1,
    is_pk: 0,
    has_filter: 0,
  },
];

const indexDdlRows: Record<string, Record<string, unknown>[]> = {
  PK_users: [
    {
      index_type: 2,
      is_unique: 1,
      is_primary_key: 1,
      is_unique_constraint: 0,
      ignore_dup_key: 0,
      filter_definition: null,
      column_name: "id",
      is_descending_key: 1,
      is_included_column: 0,
    },
  ],
  ux_users_email: [
    {
      index_type: 2,
      is_unique: 1,
      is_primary_key: 0,
      is_unique_constraint: 0,
      ignore_dup_key: 0,
      filter_definition: "([email] IS NOT NULL)",
      column_name: "email",
      is_descending_key: 0,
      is_included_column: 0,
    },
    {
      index_type: 2,
      is_unique: 1,
      is_primary_key: 0,
      is_unique_constraint: 0,
      ignore_dup_key: 0,
      filter_definition: "([email] IS NOT NULL)",
      column_name: "display_name",
      is_descending_key: 0,
      is_included_column: 1,
    },
  ],
  uq_users_login: [
    {
      index_type: 2,
      is_unique: 1,
      is_primary_key: 0,
      is_unique_constraint: 1,
      ignore_dup_key: 0,
      filter_definition: null,
      column_name: "login",
      is_descending_key: 0,
      is_included_column: 0,
    },
  ],
  ix_users_created: [
    {
      index_type: 1,
      is_unique: 0,
      is_primary_key: 0,
      is_unique_constraint: 0,
      ignore_dup_key: 0,
      filter_definition: null,
      column_name: "created_at",
      is_descending_key: 1,
      is_included_column: 0,
    },
    {
      index_type: 1,
      is_unique: 0,
      is_primary_key: 0,
      is_unique_constraint: 0,
      ignore_dup_key: 0,
      filter_definition: null,
      column_name: "id",
      is_descending_key: 0,
      is_included_column: 0,
    },
  ],
};

type ForeignKeyFixtureRow = {
  constraint_name: string;
  constraint_column_id: number;
  column_name: string;
  ref_schema: string;
  ref_table: string;
  ref_column: string;
  delete_action: string;
  update_action: string;
};

const foreignKeyRows: ForeignKeyFixtureRow[] = [
  {
    constraint_name: "FK_users_org",
    constraint_column_id: 2,
    column_name: "org_region",
    ref_schema: "dbo",
    ref_table: "orgs",
    ref_column: "region",
    delete_action: "CASCADE",
    update_action: "SET_NULL",
  },
  {
    constraint_name: "FK_users_org",
    constraint_column_id: 1,
    column_name: "org_id",
    ref_schema: "dbo",
    ref_table: "orgs",
    ref_column: "id",
    delete_action: "CASCADE",
    update_action: "SET_NULL",
  },
];

function orderForeignKeyRows(
  sql: string,
  rows: ForeignKeyFixtureRow[],
): ForeignKeyFixtureRow[] {
  if (!/ORDER BY fk\.name, fkc\.constraint_column_id\b/.test(sql)) {
    return rows;
  }
  return [...rows].sort(
    (left, right) =>
      left.constraint_name.localeCompare(right.constraint_name) ||
      left.constraint_column_id - right.constraint_column_id,
  );
}

function makeDriver(
  fixtures: {
    indexListRows?: Record<string, unknown>[];
    indexDdlRows?: Record<string, Record<string, unknown>[]>;
    foreignKeyRows?: ForeignKeyFixtureRow[];
    failCheckConstraints?: boolean;
  } = {},
): MSSQLDriver {
  const driver = new MSSQLDriver({
    id: "mssql-index-ddl",
    name: "mssql-index-ddl",
    type: "mssql",
  });
  const query = (sql: string, inputs: Inputs) => {
    if (sql.includes("sys.foreign_keys")) {
      const name = inputs.get("constraint");
      return {
        recordset: orderForeignKeyRows(
          sql,
          (fixtures.foreignKeyRows ?? foreignKeyRows).filter(
            (row) => name === undefined || row.constraint_name === name,
          ),
        ),
      };
    }
    if (sql.includes("i.name = @index")) {
      return {
        recordset:
          (fixtures.indexDdlRows ?? indexDdlRows)[
            String(inputs.get("index"))
          ] ?? [],
      };
    }
    if (sql.includes("sys.indexes")) {
      return { recordset: fixtures.indexListRows ?? indexListRows };
    }
    if (sql.includes("sys.check_constraints")) {
      if (fixtures.failCheckConstraints) {
        throw new Error("permission denied on sys.check_constraints");
      }
      return { recordset: [] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  };
  const request = vi.fn(() => {
    const inputs: Inputs = new Map();
    const requestObject = {
      input: vi.fn((key: string, _type: unknown, value: unknown) => {
        inputs.set(key, value);
        return requestObject;
      }),
      query: async (sql: string) => query(sql, inputs),
    };
    return requestObject;
  });
  (driver as unknown as { pool: unknown }).pool = {
    request,
    close: vi.fn(),
    on: vi.fn(),
  };
  vi.spyOn(driver, "describeColumns").mockResolvedValue([
    {
      name: "id",
      type: "int",
      nativeType: "int",
      category: "integer",
      nullable: false,
      isPrimaryKey: true,
      primaryKeyOrdinal: 1,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    },
  ]);
  return driver;
}

const sharedNameFixtures = {
  indexListRows: [
    ...indexListRows,
    {
      idx_name: "shared_name",
      col_name: "org_id",
      is_unique: 1,
      is_pk: 0,
      has_filter: 0,
    },
  ],
  indexDdlRows: {
    ...indexDdlRows,
    shared_name: [
      {
        index_type: 2,
        is_unique: 1,
        is_primary_key: 0,
        is_unique_constraint: 0,
        ignore_dup_key: 0,
        filter_definition: null,
        column_name: "org_id",
        is_descending_key: 0,
        is_included_column: 0,
      },
    ],
  },
  foreignKeyRows: [
    ...foreignKeyRows,
    {
      constraint_name: "shared_name",
      constraint_column_id: 1,
      column_name: "org_id",
      ref_schema: "dbo",
      ref_table: "orgs",
      ref_column: "id",
      delete_action: "NO_ACTION",
      update_action: "NO_ACTION",
    },
  ],
};

describe("MSSQL index and constraint DDL", () => {
  it("marks filtered indexes as partial so they are not unique constraints", async () => {
    const driver = makeDriver();

    const indexes = await driver.getIndexes("app", "dbo", "users");
    expect(indexes.find((index) => index.name === "ux_users_email")).toEqual(
      expect.objectContaining({ unique: true, partial: true }),
    );
    expect(
      indexes.find((index) => index.name === "uq_users_login")?.partial,
    ).toBeUndefined();

    const constraints = await driver.getConstraints("app", "dbo", "users");
    expect(constraints.map((constraint) => constraint.name)).toEqual([
      "PK_users",
      "uq_users_login",
      "FK_users_org",
    ]);
  });

  it("includes WHERE, INCLUDE and clustering in filtered index DDL", async () => {
    const driver = makeDriver();

    await expect(
      driver.getIndexDDL("app", "dbo", "users", "ux_users_email"),
    ).resolves.toBe(
      "CREATE UNIQUE NONCLUSTERED INDEX [ux_users_email] ON [app].[dbo].[users] ([email]) INCLUDE ([display_name]) WHERE ([email] IS NOT NULL);",
    );
    await expect(
      driver.getIndexDDL("app", "dbo", "users", "ix_users_created"),
    ).resolves.toBe(
      "CREATE CLUSTERED INDEX [ix_users_created] ON [app].[dbo].[users] ([created_at] DESC, [id]);",
    );
  });

  it("keeps the primary key and unique constraint names in index DDL", async () => {
    const driver = makeDriver();

    await expect(
      driver.getIndexDDL("app", "dbo", "users", "PK_users"),
    ).resolves.toBe(
      "ALTER TABLE [app].[dbo].[users] ADD CONSTRAINT [PK_users] PRIMARY KEY NONCLUSTERED ([id] DESC);",
    );
    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "uq_users_login"),
    ).resolves.toBe(
      "ALTER TABLE [app].[dbo].[users] ADD CONSTRAINT [uq_users_login] UNIQUE NONCLUSTERED ([login]);",
    );
  });

  it("includes foreign key referential actions", async () => {
    const driver = makeDriver();

    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "FK_users_org"),
    ).resolves.toBe(
      "ALTER TABLE [app].[dbo].[users] ADD CONSTRAINT [FK_users_org] FOREIGN KEY ([org_id], [org_region]) REFERENCES [app].[dbo].[orgs] ([id], [region]) ON DELETE CASCADE ON UPDATE SET NULL;",
    );
  });

  it("shows each node's own DDL when the constraint kind is passed", async () => {
    const driver = makeDriver(sharedNameFixtures);

    await expect(
      driver.getConstraintDDL(
        "app",
        "dbo",
        "users",
        "shared_name",
        "foreign_key",
      ),
    ).resolves.toBe(
      "ALTER TABLE [app].[dbo].[users] ADD CONSTRAINT [shared_name] FOREIGN KEY ([org_id]) REFERENCES [app].[dbo].[orgs] ([id]);",
    );
    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "shared_name", "unique"),
    ).resolves.toBe(
      "CREATE UNIQUE NONCLUSTERED INDEX [shared_name] ON [app].[dbo].[users] ([org_id]);",
    );
  });

  it("falls back to the foreign key query when constraint listing fails", async () => {
    const driver = makeDriver({ failCheckConstraints: true });

    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "FK_users_org"),
    ).resolves.toBe(
      "ALTER TABLE [app].[dbo].[users] ADD CONSTRAINT [FK_users_org] FOREIGN KEY ([org_id], [org_region]) REFERENCES [app].[dbo].[orgs] ([id], [region]) ON DELETE CASCADE ON UPDATE SET NULL;",
    );
    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "uq_users_login"),
    ).rejects.toThrow("permission denied on sys.check_constraints");
  });

  it("shows the unique index when a foreign key shares its name", async () => {
    const driver = makeDriver(sharedNameFixtures);

    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "shared_name"),
    ).resolves.toBe(
      "CREATE UNIQUE NONCLUSTERED INDEX [shared_name] ON [app].[dbo].[users] ([org_id]);",
    );
  });

  it("does not emit a unique constraint for a filtered unique index", async () => {
    const driver = makeDriver();

    await expect(
      driver.getConstraintDDL("app", "dbo", "users", "ux_users_email"),
    ).rejects.toThrow('Constraint "ux_users_email" not found');
  });
});
