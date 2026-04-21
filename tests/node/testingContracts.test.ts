import { describe, expect, it } from "vitest";
import {
  CANONICAL_FIXTURE_SCHEMA,
  DB_ENGINE_IDS,
  ENGINE_CAPABILITY_PROFILES,
  ONE_COMMAND_TEST_MANIFEST,
  TEST_ADMIN_CONNECTION_SEEDS,
  TEST_CONNECTION_SEEDS,
  TEST_PROJECT_IDS,
} from "../contracts/testingContracts";

describe("testingContracts", () => {
  it("defines the one-command manifest and all project ids", () => {
    expect(ONE_COMMAND_TEST_MANIFEST.primaryCommand).toBe("npm run test:all");
    expect(ONE_COMMAND_TEST_MANIFEST.projects).toEqual(TEST_PROJECT_IDS);
    expect(ONE_COMMAND_TEST_MANIFEST.scripts.dbWait).toBe("npm run db:wait");
    expect(ONE_COMMAND_TEST_MANIFEST.scripts.dbPrepare).toBe(
      "npm run db:prepare",
    );
  });

  it("declares deterministic connection seeds and a canonical fixture schema", () => {
    expect(Object.keys(TEST_CONNECTION_SEEDS)).toEqual([...DB_ENGINE_IDS]);
    expect(CANONICAL_FIXTURE_SCHEMA.columns).toHaveLength(6);
    expect(CANONICAL_FIXTURE_SCHEMA.seedRows.map((row) => row.id)).toEqual([
      1, 2,
    ]);

    for (const engineId of DB_ENGINE_IDS) {
      expect(ENGINE_CAPABILITY_PROFILES[engineId].projectId).toMatch(/^db-/);
      expect(TEST_CONNECTION_SEEDS[engineId].projectId).toBe(
        ENGINE_CAPABILITY_PROFILES[engineId].projectId,
      );
    }

    expect(TEST_CONNECTION_SEEDS.mssql.connection.username).toBe(
      "rapidb_test_user",
    );
    expect(TEST_ADMIN_CONNECTION_SEEDS.mssql?.connection.username).toBe("sa");
  });
});
