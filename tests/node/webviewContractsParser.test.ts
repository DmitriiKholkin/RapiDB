import { describe, expect, it } from "vitest";
import { parseTablePanelMessage } from "../../src/shared/webviewContracts";

describe("parseTablePanelMessage export payload", () => {
  it("parses numeric limitToPage for exportCSV", () => {
    const parsed = parseTablePanelMessage({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "contains", value: "alpha" }],
        limitToPage: { page: "2", pageSize: "50" },
      },
    });

    expect(parsed).toEqual({
      type: "exportCSV",
      payload: {
        sort: { column: "id", direction: "asc" },
        filters: [{ column: "name", op: "contains", value: "alpha" }],
        limitToPage: { page: 2, pageSize: 50 },
      },
    });
  });

  it("drops invalid limitToPage values for exportJSON", () => {
    const parsed = parseTablePanelMessage({
      type: "exportJSON",
      payload: {
        filters: [],
        limitToPage: { page: "nan", pageSize: 25 },
      },
    });

    expect(parsed).toEqual({
      type: "exportJSON",
      payload: {
        sort: undefined,
        filters: [],
        limitToPage: undefined,
      },
    });
  });

  it("drops non-positive and fractional limitToPage values", () => {
    const zeroPage = parseTablePanelMessage({
      type: "exportCSV",
      payload: {
        limitToPage: { page: 0, pageSize: 25 },
      },
    });

    expect(zeroPage).toEqual({
      type: "exportCSV",
      payload: {
        sort: undefined,
        filters: undefined,
        limitToPage: undefined,
      },
    });

    const fractionalPageSize = parseTablePanelMessage({
      type: "exportJSON",
      payload: {
        limitToPage: { page: 1, pageSize: 25.5 },
      },
    });

    expect(fractionalPageSize).toEqual({
      type: "exportJSON",
      payload: {
        sort: undefined,
        filters: undefined,
        limitToPage: undefined,
      },
    });
  });
});

describe("parseTablePanelMessage applyChanges payload", () => {
  it("parses updates and insertValues together", () => {
    const parsed = parseTablePanelMessage({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: { name: "New user" },
      },
    });

    expect(parsed).toEqual({
      type: "applyChanges",
      payload: {
        updates: [{ primaryKeys: { id: 1 }, changes: { name: "Alicia" } }],
        insertValues: { name: "New user" },
      },
    });
  });

  it("rejects invalid insertValues", () => {
    const parsed = parseTablePanelMessage({
      type: "applyChanges",
      payload: {
        updates: [],
        insertValues: "invalid",
      },
    });

    expect(parsed).toBeNull();
  });
});
