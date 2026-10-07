import { Binary } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type {
  ConnectionConfig,
  ConnectionManager,
} from "../../src/extension/connectionManager";
import { hexFromBuffer as baseHexFromBuffer } from "../../src/extension/dbDrivers/BaseDBDriver";
import { hexFromBuffer as extractedHexFromBuffer } from "../../src/extension/dbDrivers/hexUtils";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { canonicalizeBinaryPersistedEditValue } from "../../src/extension/dbDrivers/persistedEditCanonicalizers";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../../src/extension/dbDrivers/types";
import type { TableColumnsProvider } from "../../src/extension/table/tableDataContracts";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

const binaryColumn: ColumnTypeMeta = {
  name: "payload",
  type: "blob",
  nativeType: "blob",
  category: "binary",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq", "is_null", "is_not_null"],
  valueSemantics: "plain",
};

function config(type: ConnectionConfig["type"]): ConnectionConfig {
  return {
    id: `empty-binary-${type}`,
    name: `${type} empty binary`,
    type,
    filePath: type === "sqlite" ? ":memory:" : undefined,
  };
}

const cases = [
  {
    name: "SQLite",
    driverType: "sqlite",
    driver: new SQLiteDriver(config("sqlite")),
    column: binaryColumn,
    emptyBytes: Buffer.alloc(0),
  },
  {
    name: "PostgreSQL",
    driverType: "pg",
    driver: new PostgresDriver(config("pg")),
    column: binaryColumn,
    emptyBytes: Buffer.alloc(0),
  },
  {
    name: "MySQL",
    driverType: "mysql",
    driver: new MySQLDriver(config("mysql")),
    column: binaryColumn,
    emptyBytes: Buffer.alloc(0),
  },
  {
    name: "MongoDB",
    driverType: "mongodb",
    driver: new MongoDBDriver(config("mongodb")),
    column: { ...binaryColumn, type: "binData", nativeType: "binData" },
    emptyBytes: new Binary(Buffer.alloc(0)),
  },
] as const;

function createTableCellSaver(
  driver: IDBDriver,
  column: ColumnTypeMeta,
  driverType: ConnectionConfig["type"],
): (value: string) => Promise<unknown> {
  const connection = config(driverType);
  let persistedValue: unknown;

  if (typeof driver.updateRows === "function") {
    vi.spyOn(driver, "updateRows").mockImplementation(async (request) => {
      persistedValue = request.updates[0]?.changes[column.name];
      return { affectedRows: 1 };
    });
  } else {
    vi.spyOn(driver, "query").mockImplementation(async (_sql, params = []) => {
      persistedValue = params[0];
      return {
        columns: [],
        rows: [],
        rowCount: 1,
        affectedRows: 1,
        executionTimeMs: 0,
      };
    });
  }

  const manager = {
    getConnection: () => connection,
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const primaryKeyName = driverType === "mongodb" ? "_id" : "id";
  const idColumn: ColumnTypeMeta = {
    ...column,
    name: primaryKeyName,
    type: driverType === "mongodb" ? "objectId" : "integer",
    nativeType: driverType === "mongodb" ? "objectId" : "integer",
    category: driverType === "mongodb" ? "other" : "integer",
    nullable: false,
    isPrimaryKey: true,
  };
  const columnsProvider: TableColumnsProvider = {
    getColumns: async () => [idColumn, column],
  };
  const mutationService = new TableMutationService(manager, columnsProvider);

  return async (value) => {
    persistedValue = undefined;
    await mutationService.updateRow(
      connection.id,
      "roundtrip",
      "public",
      "empty_binary",
      driverType === "mongodb"
        ? {
            _id: {
              $rapidbMongoId: {
                type: "objectId",
                value: "0123456789abcdef01234567",
              },
            },
          }
        : { id: 1 },
      { [column.name]: value },
    );
    return persistedValue;
  };
}

describe.each(cases)("$name zero-length binary round trip", ({
  driver,
  column,
  emptyBytes,
  driverType,
}) => {
  it("formats, table-saves, and reloads empty bytes distinctly", async () => {
    const output = driver.formatOutputValue(emptyBytes, column);
    expect(output).toBe("0x");

    const editValue = String(output);
    expect(editValue).toBe("0x");
    const saveCell = createTableCellSaver(driver, column, driverType);
    const savedValue = await saveCell(editValue);
    expect(Buffer.isBuffer(savedValue) || savedValue instanceof Binary).toBe(
      true,
    );
    expect(driver.formatOutputValue(savedValue, column)).toBe("0x");

    const emptyText = await saveCell("");
    expect(emptyText).toBe("");
    expect(driver.formatOutputValue(emptyText, column)).toBe("");

    const nullValue = await saveCell(NULL_SENTINEL);
    expect(nullValue).toBeNull();
    expect(driver.formatOutputValue(nullValue, column)).toBeNull();
  });
});

describe("empty binary persisted-edit canonicalization", () => {
  it("does not treat empty text as a zero-length buffer", () => {
    expect(canonicalizeBinaryPersistedEditValue(Buffer.alloc(0))).toEqual({
      canonical: "0x",
    });
    expect(canonicalizeBinaryPersistedEditValue("0x")).toEqual({
      canonical: "0x",
    });
    expect(canonicalizeBinaryPersistedEditValue("")).toEqual({
      canonical: "",
    });
  });

  it("keeps both binary formatters in sync", () => {
    expect(baseHexFromBuffer(Buffer.alloc(0))).toBe("0x");
    expect(extractedHexFromBuffer(Buffer.alloc(0))).toBe("0x");
  });
});
