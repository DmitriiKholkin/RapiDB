import { describe, expect, it } from "vitest";
import { NULL_SENTINEL as extensionNullSentinel } from "../../src/extension/dbDrivers/types";
import {
  buildFilterExpression,
  buildFilterExpressionFromDraft,
  coerceFilterExpressions,
  defaultFilterOperator,
  type FilterDraftMap,
  type FilterOperator,
  isNumericCategory,
  serializeFilterDrafts,
  NULL_SENTINEL as sharedNullSentinel,
  valueFilterOperator,
} from "../../src/shared/tableTypes";
import {
  parseConnectionFormPanelMessage,
  parseQueryPanelMessage,
  parseTablePanelMessage,
  parseWebviewInitialState,
} from "../../src/shared/webviewContracts";
import { NULL_SENTINEL as webviewNullSentinel } from "../../src/webview/types";

describe("shared contract parity", () => {
  it("keeps the NULL sentinel aligned across shared, extension, and webview layers", () => {
    expect(extensionNullSentinel).toBe(sharedNullSentinel);
    expect(webviewNullSentinel).toBe(sharedNullSentinel);
  });

  it("centralizes the default value-filter operator policy in shared helpers", () => {
    expect(
      defaultFilterOperator({ category: "boolean", isBoolean: true }),
    ).toBe("eq");
    expect(
      defaultFilterOperator({ category: "integer", isBoolean: false }),
    ).toBe("eq");
    expect(defaultFilterOperator({ category: "date", isBoolean: false })).toBe(
      "eq",
    );
    expect(defaultFilterOperator({ category: "text", isBoolean: false })).toBe(
      "like",
    );
  });

  it("uses shared operator policy only when the column supports value filters", () => {
    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["like", "is_null"],
        isBoolean: false,
      }),
    ).toBe("like");

    expect(
      valueFilterOperator({
        category: "text",
        filterable: true,
        filterOperators: ["eq", "is_null"],
        isBoolean: false,
      }),
    ).toBeNull();
  });

  it("keeps numeric category detection in shared helpers", () => {
    expect(isNumericCategory("integer")).toBe(true);
    expect(isNumericCategory("float")).toBe(true);
    expect(isNumericCategory("decimal")).toBe(true);
    expect(isNumericCategory("text")).toBe(false);
    expect(isNumericCategory("datetime")).toBe(false);
  });

  it("keeps structured filter coercion in shared helpers", () => {
    expect(
      coerceFilterExpressions([
        { column: "created_on", operator: "eq", value: " 2026-04-15 " },
        { column: "name", operator: "is_null" },
      ]),
    ).toEqual([
      { column: "created_on", operator: "eq", value: "2026-04-15" },
      { column: "name", operator: "is_null" },
    ]);

    expect(
      coerceFilterExpressions([
        { column: "created_on", value: "2026-04-15" },
        { column: "name", value: sharedNullSentinel },
      ]),
    ).toEqual([]);
  });

  it("drops incomplete raw filters during coercion", () => {
    expect(
      coerceFilterExpressions([
        { column: "name", operator: "like", value: "   " },
        {
          column: "created_on",
          operator: "between",
          value: ["2026-04-01", "  "],
        },
      ]),
    ).toEqual([]);

    expect(
      coerceFilterExpressions([
        {
          column: "created_on",
          operator: "between",
          value: [" 2026-04-01 ", " 2026-04-15 "],
        },
      ]),
    ).toEqual([
      {
        column: "created_on",
        operator: "between",
        value: ["2026-04-01", "2026-04-15"],
      },
    ]);
  });

  it("parses sanitized connection initial state without exposing a password", () => {
    expect(
      parseWebviewInitialState({
        view: "connection",
        existing: {
          id: "conn-1",
          name: "Analytics",
          type: "pg",
          host: "localhost",
          username: "reader",
          useSecretStorage: true,
          hasStoredSecret: true,
          password: "should-not-pass",
        },
      }),
    ).toEqual({
      view: "connection",
      existing: {
        id: "conn-1",
        name: "Analytics",
        type: "pg",
        host: "localhost",
        username: "reader",
        useSecretStorage: true,
        hasStoredSecret: true,
      },
    });
  });

  it("allows empty database and schema values for table and schema views", () => {
    expect(
      parseWebviewInitialState({
        view: "table",
        connectionId: "conn-1",
        database: "",
        schema: "",
        table: "users",
      }),
    ).toEqual({
      view: "table",
      connectionId: "conn-1",
      database: "",
      schema: "",
      table: "users",
      isView: undefined,
      defaultPageSize: undefined,
    });

    expect(
      parseWebviewInitialState({
        view: "schema",
        connectionId: "conn-1",
        database: "",
        schema: "",
        table: "users",
      }),
    ).toEqual({
      view: "schema",
      connectionId: "conn-1",
      database: "",
      schema: "",
      table: "users",
    });
  });

  it("rejects malformed query and table panel messages", () => {
    expect(
      parseQueryPanelMessage({
        type: "executeQuery",
        payload: { connectionId: "conn-1" },
      }),
    ).toBeNull();

    expect(
      parseTablePanelMessage({
        type: "deleteRows",
        payload: { primaryKeysList: [null] },
      }),
    ).toBeNull();

    expect(
      parseTablePanelMessage({
        type: "confirmMutationPreview",
        payload: {},
      }),
    ).toBeNull();
  });

  it("parses table mutation preview confirmation messages", () => {
    expect(
      parseTablePanelMessage({
        type: "confirmMutationPreview",
        payload: { previewToken: "preview-1" },
      }),
    ).toEqual({
      type: "confirmMutationPreview",
      payload: { previewToken: "preview-1" },
    });

    expect(
      parseTablePanelMessage({
        type: "cancelMutationPreview",
        payload: { previewToken: "preview-2" },
      }),
    ).toEqual({
      type: "cancelMutationPreview",
      payload: { previewToken: "preview-2" },
    });
  });

  it("preserves hasStoredSecret on parsed connection form messages", () => {
    expect(
      parseConnectionFormPanelMessage({
        type: "testConnection",
        payload: {
          id: "conn-1",
          name: "Analytics",
          type: "pg",
          host: "localhost",
          port: 5432,
          database: "app",
          username: "reader",
          password: "",
          useSecretStorage: true,
          hasStoredSecret: true,
        },
      }),
    ).toEqual({
      type: "testConnection",
      payload: {
        id: "conn-1",
        name: "Analytics",
        type: "pg",
        host: "localhost",
        port: 5432,
        database: "app",
        username: "reader",
        password: "",
        useSecretStorage: true,
        hasStoredSecret: true,
      },
    });
  });

  describe("buildFilterExpression", () => {
    const column = {
      name: "name",
      type: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text" as const,
      nativeType: "text",
      filterable: true,
      editable: true,
      filterOperators: ["like"] as FilterOperator[],
      isBoolean: false,
    };

    it("builds a structured value filter", () => {
      expect(buildFilterExpression(column, " alice ")).toEqual({
        column: "name",
        operator: "like",
        value: "alice",
      });
    });

    it("builds a NULL filter only when supported", () => {
      expect(
        buildFilterExpression(
          { ...column, filterOperators: ["like", "is_null"] },
          sharedNullSentinel,
        ),
      ).toEqual({
        column: "name",
        operator: "is_null",
      });
      expect(buildFilterExpression(column, sharedNullSentinel)).toBeNull();
    });

    it("skips value filters when the column cannot accept the shared default operator", () => {
      expect(
        buildFilterExpression({ ...column, filterOperators: ["eq"] }, "alice"),
      ).toBeNull();

      expect(
        buildFilterExpression(
          {
            ...column,
            filterable: false,
            filterOperators: ["like", "is_null"],
          },
          "alice",
        ),
      ).toBeNull();
    });

    it("skips empty values", () => {
      expect(buildFilterExpression(column, "   ")).toBeNull();
    });
  });

  describe("buildFilterExpressionFromDraft", () => {
    const textColumn = {
      name: "name",
      filterable: true,
      filterOperators: ["like", "is_null", "is_not_null"] as FilterOperator[],
    };

    it("returns null when the draft is missing", () => {
      expect(buildFilterExpressionFromDraft(textColumn, undefined)).toBeNull();
    });

    it("builds scalar filters from explicit drafts", () => {
      expect(
        buildFilterExpressionFromDraft(textColumn, {
          operator: "like",
          value: " alice ",
        }),
      ).toEqual({
        column: "name",
        operator: "like",
        value: "alice",
      });
    });

    it("skips empty scalar drafts", () => {
      expect(
        buildFilterExpressionFromDraft(textColumn, {
          operator: "like",
          value: "   ",
        }),
      ).toBeNull();
    });

    it("allows nullability operators without requiring filterable", () => {
      expect(
        buildFilterExpressionFromDraft(
          {
            name: "archived_at",
            filterable: false,
            filterOperators: ["is_null", "is_not_null"],
          },
          { operator: "is_not_null" },
        ),
      ).toEqual({
        column: "archived_at",
        operator: "is_not_null",
      });
    });

    it("rejects unsupported nullability drafts", () => {
      expect(
        buildFilterExpressionFromDraft(
          {
            name: "archived_at",
            filterable: false,
            filterOperators: ["between"],
          },
          { operator: "is_null" },
        ),
      ).toBeNull();
    });

    it("requires both between values", () => {
      expect(
        buildFilterExpressionFromDraft(
          {
            name: "created_on",
            filterable: true,
            filterOperators: ["between"],
          },
          {
            operator: "between",
            value: ["2026-04-01", "  "],
          },
        ),
      ).toBeNull();

      expect(
        buildFilterExpressionFromDraft(
          {
            name: "created_on",
            filterable: true,
            filterOperators: ["between"],
          },
          {
            operator: "between",
            value: [" 2026-04-01 ", " 2026-04-15 "],
          },
        ),
      ).toEqual({
        column: "created_on",
        operator: "between",
        value: ["2026-04-01", "2026-04-15"],
      });
    });

    it("rejects unsupported scalar and between drafts", () => {
      expect(
        buildFilterExpressionFromDraft(
          {
            name: "name",
            filterable: false,
            filterOperators: ["like"],
          },
          {
            operator: "like",
            value: "alice",
          },
        ),
      ).toBeNull();

      expect(
        buildFilterExpressionFromDraft(
          {
            name: "created_on",
            filterable: true,
            filterOperators: ["eq"],
          },
          {
            operator: "between",
            value: ["2026-04-01", "2026-04-15"],
          },
        ),
      ).toBeNull();
    });
  });

  describe("serializeFilterDrafts", () => {
    const columns = [
      {
        name: "name",
        filterable: true,
        filterOperators: ["like", "is_null"] as FilterOperator[],
      },
      {
        name: "created_on",
        filterable: true,
        filterOperators: ["between"] as FilterOperator[],
      },
      {
        name: "archived_at",
        filterable: false,
        filterOperators: ["is_not_null"] as FilterOperator[],
      },
    ];

    it("returns an empty array when drafts are absent", () => {
      expect(serializeFilterDrafts(columns, undefined)).toEqual([]);
    });

    it("serializes in column order instead of draft insertion order", () => {
      const drafts: FilterDraftMap = {
        archived_at: { operator: "is_not_null" },
        name: { operator: "like", value: "alice" },
        created_on: {
          operator: "between",
          value: ["2026-04-01", "2026-04-15"],
        },
      };

      expect(serializeFilterDrafts(columns, drafts)).toEqual([
        { column: "name", operator: "like", value: "alice" },
        {
          column: "created_on",
          operator: "between",
          value: ["2026-04-01", "2026-04-15"],
        },
        { column: "archived_at", operator: "is_not_null" },
      ]);
    });

    it("drops incomplete or unsupported drafts", () => {
      const drafts: FilterDraftMap = {
        name: { operator: "like", value: "   " },
        created_on: {
          operator: "between",
          value: ["2026-04-01", "   "],
        },
      };

      expect(serializeFilterDrafts(columns, drafts)).toEqual([]);
    });
  });
});
