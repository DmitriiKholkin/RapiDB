import { expect, it } from "vitest";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import { ErdGraphService } from "../../../src/extension/services/erdGraphService";
import {
  executeAtomicSqlApplyPlan,
  prepareApplyChangesPlan,
} from "../../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../../src/extension/table/tableMutationService";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import { resolveConnectionSeed } from "../../runtime/testRuntimeConfig";

it("B01: isolates two PostgreSQL databases with identical table names during parallel table operations (live)", async () => {
  const connection = (await resolveConnectionSeed(
    "postgres",
  )) as ConnectionConfig;
  const admin = new PostgresDriver(connection);
  const suffix = `${Date.now().toString(36)}_${process.pid}`;
  const databases = [`rapidb_b01_a_${suffix}`, `rapidb_b01_b_${suffix}`];
  const created: string[] = [];
  let driver: PostgresDriver | undefined;
  let erd: ErdGraphService | undefined;
  try {
    await admin.connect();
    for (const database of databases) {
      await admin.query(`CREATE DATABASE ${admin.quoteIdentifier(database)}`);
      created.push(database);
    }
    const scopedConnection = { ...connection, database: databases[0] };
    driver = new PostgresDriver(scopedConnection);
    await driver.connect();
    const activeDriver = driver;
    const manager = {
      getDriver: () => activeDriver,
      getConnection: () => scopedConnection,
      getSchemaSnapshotAsync: async () => ({
        databases: databases.map((name) => ({
          name,
          schemas: [
            {
              name: "public",
              objects: [{ name: "items", type: "table", columns: [] }],
            },
          ],
        })),
      }),
      onDidDisconnect: () => ({ dispose() {} }),
      onDidRefreshSchemas: () => ({ dispose() {} }),
    };
    const read = new TableReadService(manager as never);
    const mutation = new TableMutationService(manager as never, read);
    await Promise.all(
      databases.map(async (database, index) => {
        await activeDriver.query(
          `CREATE TABLE public.items (
        id integer PRIMARY KEY,
        amount numeric NOT NULL,
        marker_${index} text DEFAULT 'database_${index}',
        parent_id integer REFERENCES public.items(id)
      )`,
          [],
          { database },
        );
        await activeDriver.query(
          "INSERT INTO public.items(id, amount) VALUES (1, $1), (2, $2)",
          [index + 10, index + 20],
          { database },
        );
        await activeDriver.query(
          "UPDATE public.items SET parent_id = 1 WHERE id = 2",
          [],
          { database },
        );
      }),
    );
    const pages = await Promise.all(
      databases.map((database) =>
        read.getPage(connection.id, database, "public", "items", 1, 10, []),
      ),
    );
    expect(pages.map((page) => page.rows[0].amount)).toEqual(["10", "11"]);
    expect(
      pages.map((page) => page.columns.map((column) => column.name)),
    ).toEqual([
      ["id", "amount", "marker_0", "parent_id"],
      ["id", "amount", "marker_1", "parent_id"],
    ]);
    await Promise.all(
      databases.map(async (database, index) => {
        const insert = await mutation.prepareInsertRow(
          connection.id,
          database,
          "public",
          "items",
          { id: 3, amount: "30.5", [`marker_${index}`]: "preview" },
        );
        expect(insert.previewStatements[0]).toContain(`marker_${index}`);
        const columns = await read.getColumns(
          connection.id,
          database,
          "public",
          "items",
        );
        const apply = prepareApplyChangesPlan(
          manager as never,
          connection.id,
          database,
          "public",
          "items",
          [
            {
              primaryKeys: { id: 1 },
              changes: { amount: `${index + 100}.25` },
            },
          ],
          columns,
        );
        if (!apply.executable) throw new Error("Missing apply plan");
        const result = await executeAtomicSqlApplyPlan(
          manager as never,
          apply.plan,
          [insert],
        );
        expect(result).toMatchObject({
          success: true,
          insertApplied: true,
          rowOutcomes: [{ status: "applied" }],
        });
        expect(result.warning).toBeUndefined();
        await mutation.updateRow(
          connection.id,
          database,
          "public",
          "items",
          { id: 2 },
          { amount: `${index + 200}.75` },
        );
        await mutation.insertRow(connection.id, database, "public", "items", {
          id: 4,
          amount: "40.5",
        });
        await mutation.deleteRows(connection.id, database, "public", "items", [
          { id: 3 },
          { id: 4 },
        ]);
        const rows = [];
        for await (const chunk of read.exportAll(
          connection.id,
          database,
          "public",
          "items",
          1,
        ))
          rows.push(...chunk.rows);
        expect(rows.map((row) => row.amount)).toEqual([
          `${index + 100}.25`,
          `${index + 200}.75`,
        ]);
        const ddl = await activeDriver.getCreateTableDDL(
          database,
          "public",
          "items",
        );
        expect(ddl).toContain(`marker_${index}`);
        expect(ddl).not.toContain(`marker_${1 - index}`);
        expect(
          (await activeDriver.getForeignKeys(database, "public", "items"))[0]
            .referencedTable,
        ).toBe("items");
        expect(
          (await activeDriver.getConstraints(database, "public", "items"))
            .length,
        ).toBeGreaterThan(0);
      }),
    );
    erd = new ErdGraphService(manager as never);
    const activeErd = erd;
    const graphs = await Promise.all(
      databases.map((database) =>
        activeErd.getGraph({
          connectionId: connection.id,
          database,
          schema: "public",
        }),
      ),
    );
    for (const [index, result] of graphs.entries()) {
      expect(
        result.graph.nodes[0].columns.map((column) => column.name),
      ).toContain(`marker_${index}`);
      expect(result.graph.edges).toHaveLength(1);
    }
    const editor = await activeDriver.query(
      "SELECT current_database(), amount FROM public.items WHERE id = 1",
    );
    expect(editor.rows[0]).toEqual({
      __col_0: databases[0],
      __col_1: "100.25",
    });
    await expect(
      activeDriver.query("UPDATE public.items SET amount = 0", [], {
        database: databases[1],
        readOnly: true,
      }),
    ).rejects.toThrow(/read.only/i);
    const scoped = await activeDriver.query(
      "SELECT current_database(), amount FROM public.items WHERE id = 1",
      [],
      { database: databases[1] },
    );
    expect(scoped.rows[0]).toEqual({
      __col_0: databases[1],
      __col_1: "101.25",
    });
  } finally {
    erd?.dispose();
    try {
      await driver?.disconnect();
    } finally {
      try {
        await dropDatabases(admin, created);
      } finally {
        await admin.disconnect();
      }
    }
  }
}, 60_000);

async function dropDatabases(
  admin: PostgresDriver,
  databases: string[],
): Promise<void> {
  const cleanup = await Promise.allSettled(
    databases.map((database) =>
      admin.query(`DROP DATABASE ${admin.quoteIdentifier(database)}`),
    ),
  );
  const failure = cleanup.find((result) => result.status === "rejected");
  if (failure?.status === "rejected") throw failure.reason;
}
