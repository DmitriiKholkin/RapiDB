import { describe, expect, it } from "vitest";
import {
  CANONICAL_FIXTURE_DATASET,
  resolveFixtureNamespace,
} from "../fixtures/canonicalDataset.ts";
import { buildFixtureMaterializationPlan } from "../fixtures/materializers.ts";

describe("canonical fixture materializers", () => {
  it("publishes the expected logical dataset shape", () => {
    expect(CANONICAL_FIXTURE_DATASET.datasetId).toBe("baseline-v2");
    expect(CANONICAL_FIXTURE_DATASET.tables).toHaveLength(8);
    expect(CANONICAL_FIXTURE_DATASET.expected.exportRowCount).toBe(128);
    expect(CANONICAL_FIXTURE_DATASET.expected.paginationRowCount).toBe(48);
  });

  it("builds deterministic plans for every engine", () => {
    const engineIds = [
      "sqlite",
      "postgres",
      "mysql",
      "mssql",
      "oracle",
    ] as const;

    for (const engineId of engineIds) {
      const plan = buildFixtureMaterializationPlan(engineId);
      expect(plan.engineId).toBe(engineId);
      expect(plan.resetStatements.length).toBeGreaterThan(0);
      expect(plan.seedStatements.length).toBeGreaterThan(0);
      expect(resolveFixtureNamespace(engineId).logicalSchemaName).toBe(
        "rapidb_test",
      );
    }
  });

  it("keeps MSSQL bootstrap admin separate from the application fixture plan", () => {
    const plan = buildFixtureMaterializationPlan("mssql");

    expect(
      plan.bootstrapStatements.some((statement) =>
        statement.includes("CREATE DATABASE [happy_mssql_db]"),
      ),
    ).toBe(true);
    expect(
      plan.bootstrapStatements.some((statement) =>
        statement.includes("CREATE LOGIN [rapidb_test_user]"),
      ),
    ).toBe(true);
  });
});
