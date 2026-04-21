import { Database } from "node-sqlite3-wasm";
import { afterEach, describe, expect, it } from "vitest";
import { materializeSqliteFixture } from "../../runtime/liveDbOrchestration.ts";

const openDatabases: Database[] = [];

afterEach(() => {
  for (const database of openDatabases.splice(0, openDatabases.length)) {
    database.close();
  }
});

describe("sqlite fixture materializer", () => {
  it("creates the canonical sqlite fixture dataset on a temp file", async () => {
    const { filePath } = await materializeSqliteFixture();
    const db = new Database(filePath);
    openDatabases.push(db);

    const fixtureRowCount = db.get(
      "SELECT COUNT(*) AS count FROM fixture_rows",
    ) as { count: number };
    const exportRowCount = db.get(
      "SELECT COUNT(*) AS count FROM export_rows",
    ) as { count: number };
    const compositeKeyCount = db.get(
      "SELECT COUNT(*) AS count FROM composite_links",
    ) as { count: number };

    expect(fixtureRowCount.count).toBe(2);
    expect(exportRowCount.count).toBe(128);
    expect(compositeKeyCount.count).toBe(4);
  });
});
