import { vi } from "vitest";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";

/** Tests that supply synthetic columns also need to supply their relation scope. */
export function mockPostgresTableScope(driver: IDBDriver): void {
  if (!driver.getTableDataReference) return;
  vi.spyOn(driver, "getTableDataReference").mockImplementation(
    (database, schema, table) =>
      `ONLY ${driver.qualifiedTableName(database, schema, table)}`,
  );
}
