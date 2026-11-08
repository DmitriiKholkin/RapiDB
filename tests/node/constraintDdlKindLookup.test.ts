import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { TableConstraintMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const sameNameConstraints: TableConstraintMeta[] = [
  {
    name: "shared_name",
    kind: "unique",
    columns: ["code"],
    source: "catalog",
  },
  {
    name: "shared_name",
    kind: "foreign_key",
    columns: ["code"],
    referencedTable: "parents",
    referencedColumns: ["code"],
    source: "catalog",
  },
];

function makeDriver() {
  const driver = new MySQLDriver({
    id: "mysql-constraint-kind",
    name: "MySQL",
    type: "mysql",
    host: "localhost",
    database: "app",
  } as ConnectionConfig);
  vi.spyOn(driver, "getConstraints").mockResolvedValue(sameNameConstraints);
  const getIndexDDL = vi
    .spyOn(driver, "getIndexDDL")
    .mockResolvedValue(
      "CREATE UNIQUE INDEX `shared_name` ON `app`.`t` (`code`);",
    );
  return { driver, getIndexDDL };
}

describe("constraint DDL lookup by kind", () => {
  it("shows the foreign key when a unique index shares its name", async () => {
    const { driver, getIndexDDL } = makeDriver();
    const ddl = await driver.getConstraintDDL(
      "app",
      "app",
      "t",
      "shared_name",
      "foreign_key",
    );
    expect(getIndexDDL).not.toHaveBeenCalled();
    expect(ddl).toContain("FOREIGN KEY");
  });

  it("shows the unique index when asked for it", async () => {
    const { driver, getIndexDDL } = makeDriver();
    await driver.getConstraintDDL("app", "app", "t", "shared_name", "unique");
    expect(getIndexDDL).toHaveBeenCalledWith("app", "app", "t", "shared_name");
  });

  it("keeps the first-match lookup when no kind is given", async () => {
    const { driver, getIndexDDL } = makeDriver();
    await driver.getConstraintDDL("app", "app", "t", "shared_name");
    expect(getIndexDDL).toHaveBeenCalledOnce();
  });
});
