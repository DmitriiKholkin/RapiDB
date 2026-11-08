import oracledb from "oracledb";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnMeta } from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import {
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

class InspectableOracleDriver extends OracleDriver {
  column(meta: ColumnMeta) {
    return this.enrichColumn(meta);
  }
}

const config = { id: "json-blob", name: "JSON BLOB", type: "oracle" as const };
function harness(nativeType = "BLOB IS JSON") {
  const driver = new InspectableOracleDriver(config);
  const columns = [
    driver.column({
      name: "ID",
      type: "NUMBER(10,0)",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
    }),
    driver.column({
      name: "PAYLOAD",
      type: nativeType,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    }),
  ];
  const manager = {
    getConnection: () => config,
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  return { driver, columns, payload: columns[1], manager };
}
const oldBytes = Buffer.from('{"n":9007199254740993,"tag":"old"}');
const newBytes = Buffer.from('{"n":9007199254740994,"tag":"new"}');
const hex = (bytes: Buffer) => `0x${bytes.toString("hex")}`;

afterEach(() => vi.restoreAllMocks());

describe("Oracle JSON-constrained BLOB binary mutation contract", () => {
  it.each([
    "BLOB IS JSON",
    "blob is json",
  ])("preserves bytes, NULL and JSON-aware Contains for %s", (nativeType) => {
    const { driver, payload } = harness(nativeType);
    expect(payload.category).toBe("binary");
    expect(driver.buildVerificationValueExpr(payload)).toBe('"PAYLOAD"');
    expect(driver.formatOutputValue(oldBytes, payload)).toBe(hex(oldBytes));
    expect(driver.coerceInputValue(hex(newBytes), payload)).toEqual(newBytes);
    expect(driver.coerceOriginalValue(hex(oldBytes), payload)).toEqual(
      oldBytes,
    );
    expect(
      driver.checkPersistedEdit(payload, hex(newBytes), {
        persistedValue: newBytes,
      }),
    ).toMatchObject({ ok: true, shouldVerify: true });
    expect(
      driver.checkPersistedEdit(payload, hex(newBytes), {
        persistedValue: oldBytes,
      })?.ok,
    ).toBe(false);
    expect(
      driver.checkPersistedEdit(payload, null, { persistedValue: null })?.ok,
    ).toBe(true);
    expect(driver.checkPersistedEdit(payload, '{"n":1}')?.ok).toBe(false);
    expect(
      buildWhere(
        driver,
        [{ column: "PAYLOAD", operator: "like", value: "new" }],
        [payload],
      ),
    ).toMatchObject({
      clause: expect.stringContaining(
        'JSON_SERIALIZE("PAYLOAD" RETURNING CLOB)',
      ),
      params: ["new"],
    });
  });

  it.each([
    true,
    false,
  ])("verifies raw BLOB bytes before commit (readback matches: %s)", async (matches) => {
    const { driver, columns, manager } = harness();
    const prepared = prepareApplyChangesPlan(
      manager,
      config.id,
      "",
      "QA",
      "ITEMS",
      [
        {
          primaryKeys: { ID: "1" },
          changes: { PAYLOAD: hex(newBytes) },
          originalValues: { PAYLOAD: hex(oldBytes) },
        },
      ],
      columns,
    );
    if (!prepared.executable) throw new Error(JSON.stringify(prepared.result));
    expect(prepared.plan.operations[0]).toMatchObject({
      sql: expect.stringContaining('DBMS_LOB.COMPARE("PAYLOAD", :3)'),
      params: [newBytes, "1", oldBytes],
    });
    const commit = vi.fn(async () => {});
    const rollback = vi.fn(async () => {});
    const close = vi.fn(async () => {});
    const execute = vi.fn(async (sql: string) => {
      if (sql.startsWith("UPDATE")) return { rowsAffected: 1 };
      expect(sql).toContain('SELECT "PAYLOAD"');
      let fetched = false;
      return {
        resultSet: {
          metaData: [{ name: "__col_0", dbType: oracledb.DB_TYPE_BLOB }],
          getRows: async () => {
            if (fetched) return [];
            fetched = true;
            return [[matches ? newBytes : oldBytes]];
          },
          close: async () => {},
        },
      };
    });
    const lease = driver as unknown as {
      getConnection(): Promise<oracledb.Connection>;
    };
    vi.spyOn(lease, "getConnection").mockResolvedValue({
      execute,
      commit,
      rollback,
      close,
    } as unknown as oracledb.Connection);
    expect(
      await executePreparedApplyPlan(manager, prepared.plan),
    ).toMatchObject({
      success: matches,
    });
    expect(commit).toHaveBeenCalledTimes(matches ? 1 : 0);
    expect(rollback).toHaveBeenCalledTimes(matches ? 0 : 1);
    expect(close).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("prepares binary INSERT binds with persisted verification", async () => {
    const { driver, columns, manager } = harness();
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    const plan = await service.prepareInsertRow(config.id, "", "QA", "ITEMS", {
      ID: "1",
      PAYLOAD: hex(newBytes),
    });
    expect(plan.operation.params).toEqual(["1", newBytes]);
    expect(plan.verification).toBeDefined();
    expect(driver.checkPersistedEdit(columns[1], hex(newBytes))?.ok).toBe(true);
  });

  it.each([
    "JSON",
    "CLOB IS JSON",
    "VARCHAR2(4000) IS JSON",
  ])("retains the text JSON contract for %s", (nativeType) => {
    const { driver, payload } = harness(nativeType);
    expect(payload.category).toBe("json");
    expect(driver.coerceInputValue('{"n":9007199254740993}', payload)).toBe(
      '{"n":9007199254740993}',
    );
    expect(
      driver.checkPersistedEdit(payload, '{"n":9007199254740993}', {
        persistedValue: '{ "n": 9007199254740993 }',
      })?.ok,
    ).toBe(true);
    expect(
      driver.buildOriginalValueComparisonWithParams(payload, '{"n":1}', 1)?.sql,
    ).toContain("JSON_EQUAL");
  });
});
