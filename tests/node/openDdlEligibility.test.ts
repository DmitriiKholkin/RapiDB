import { describe, expect, it } from "vitest";
import type { DriverEntityManifest } from "../../src/extension/dbDrivers/types";
import {
  composeOpenDdlAwareContextValue,
  isOpenDdlNodeKind,
  isOpenDdlSupportedForNode,
} from "../../src/extension/utils/openDdlEligibility";
import type { ConnectionType } from "../../src/shared/connectionTypes";

const MANIFESTS: Record<ConnectionType, DriverEntityManifest> = {
  pg: {
    dbObjectKinds: [
      "table",
      "view",
      "materializedView",
      "function",
      "procedure",
      "sequence",
      "type",
    ],
    tableSections: {
      columns: "supported",
      constraints: "supported",
      indexes: "supported",
      triggers: "supported",
    },
  },
  mysql: {
    dbObjectKinds: ["table", "view", "function", "procedure"],
    tableSections: {
      columns: "supported",
      constraints: "supported",
      indexes: "supported",
      triggers: "supported",
    },
  },
  sqlite: {
    dbObjectKinds: ["table", "view"],
    tableSections: {
      columns: "supported",
      constraints: "supported",
      indexes: "supported",
      triggers: "supported",
    },
  },
  mssql: {
    dbObjectKinds: [
      "table",
      "view",
      "function",
      "procedure",
      "sequence",
      "type",
    ],
    tableSections: {
      columns: "supported",
      constraints: "supported",
      indexes: "supported",
      triggers: "supported",
    },
  },
  oracle: {
    dbObjectKinds: [
      "table",
      "view",
      "materializedView",
      "function",
      "procedure",
      "sequence",
      "type",
    ],
    tableSections: {
      columns: "supported",
      constraints: "supported",
      indexes: "supported",
      triggers: "supported",
    },
  },
  mongodb: {
    dbObjectKinds: ["table", "view"],
    tableSections: {
      columns: "supported",
      constraints: "not_applicable",
      indexes: "supported",
      triggers: "not_applicable",
    },
  },
  dynamodb: {
    dbObjectKinds: ["table"],
    tableSections: {
      columns: "supported",
      constraints: "not_applicable",
      indexes: "supported",
      triggers: "not_applicable",
    },
  },
  elasticsearch: {
    dbObjectKinds: ["table"],
    tableSections: {
      columns: "supported",
      constraints: "not_applicable",
      indexes: "supported",
      triggers: "not_applicable",
    },
  },
  redis: {
    dbObjectKinds: ["table"],
    tableSections: {
      columns: "supported",
      constraints: "not_applicable",
      indexes: "not_applicable",
      triggers: "not_applicable",
    },
  },
};

describe("open DDL eligibility", () => {
  it("matches the canonical support matrix for NoSQL table and index nodes", () => {
    expect(
      isOpenDdlSupportedForNode("table", "mongodb", MANIFESTS.mongodb),
    ).toBe(true);
    expect(
      isOpenDdlSupportedForNode("view", "mongodb", MANIFESTS.mongodb),
    ).toBe(true);
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_index",
        "mongodb",
        MANIFESTS.mongodb,
      ),
    ).toBe(true);

    expect(
      isOpenDdlSupportedForNode("table", "dynamodb", MANIFESTS.dynamodb),
    ).toBe(true);
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_index",
        "dynamodb",
        MANIFESTS.dynamodb,
      ),
    ).toBe(true);

    expect(
      isOpenDdlSupportedForNode(
        "table",
        "elasticsearch",
        MANIFESTS.elasticsearch,
      ),
    ).toBe(true);
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_index",
        "elasticsearch",
        MANIFESTS.elasticsearch,
      ),
    ).toBe(false);

    expect(isOpenDdlSupportedForNode("table", "redis", MANIFESTS.redis)).toBe(
      false,
    );
  });

  it("keeps Open DDL enabled for supported SQL object kinds", () => {
    expect(isOpenDdlSupportedForNode("table", "pg", MANIFESTS.pg)).toBe(true);
    expect(isOpenDdlSupportedForNode("view", "mysql", MANIFESTS.mysql)).toBe(
      true,
    );
    expect(
      isOpenDdlSupportedForNode("materializedView", "oracle", MANIFESTS.oracle),
    ).toBe(true);
    expect(
      isOpenDdlSupportedForNode("table_detail_index", "mssql", MANIFESTS.mssql),
    ).toBe(true);
  });

  it("adds noDdl suffix to unsupported node context values", () => {
    expect(
      composeOpenDdlAwareContextValue("table", "mongodb", MANIFESTS.mongodb),
    ).toBe("table");
    expect(
      composeOpenDdlAwareContextValue(
        "table_detail_index",
        "elasticsearch",
        MANIFESTS.elasticsearch,
      ),
    ).toBe("table_detail_index_noDdl");
    expect(composeOpenDdlAwareContextValue("table", "pg", MANIFESTS.pg)).toBe(
      "table",
    );
  });

  it("uses per-index hints to suppress unsupported detail nodes", () => {
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_index",
        "dynamodb",
        MANIFESTS.dynamodb,
        { indexDdlSupport: "unsupported" },
      ),
    ).toBe(false);
    expect(
      composeOpenDdlAwareContextValue(
        "table_detail_index",
        "dynamodb",
        MANIFESTS.dynamodb,
        { indexDdlSupport: "unsupported" },
      ),
    ).toBe("table_detail_index_noDdl");
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_index",
        "dynamodb",
        MANIFESTS.dynamodb,
        { indexDdlSupport: "supported" },
      ),
    ).toBe(true);
  });

  it("handles unknown kinds and keeps context value unchanged", () => {
    expect(isOpenDdlNodeKind("collection")).toBe(false);
    expect(isOpenDdlSupportedForNode("collection", "pg", MANIFESTS.pg)).toBe(
      false,
    );
    expect(
      composeOpenDdlAwareContextValue("collection", "pg", MANIFESTS.pg),
    ).toBe("collection");
  });

  it("uses manifest table-section support for constraint and trigger kinds", () => {
    const unsupportedSectionsManifest: DriverEntityManifest = {
      dbObjectKinds: ["table", "view"],
      tableSections: {
        columns: "supported",
        constraints: "not_applicable",
        indexes: "supported",
        triggers: "not_applicable",
      },
    };

    expect(
      isOpenDdlSupportedForNode(
        "table_detail_constraint",
        "pg",
        unsupportedSectionsManifest,
      ),
    ).toBe(false);
    expect(
      isOpenDdlSupportedForNode(
        "table_detail_trigger",
        "pg",
        unsupportedSectionsManifest,
      ),
    ).toBe(false);
    expect(
      composeOpenDdlAwareContextValue(
        "table_detail_constraint",
        "pg",
        unsupportedSectionsManifest,
      ),
    ).toBe("table_detail_constraint_noDdl");
    expect(
      composeOpenDdlAwareContextValue(
        "table_detail_trigger",
        "pg",
        unsupportedSectionsManifest,
      ),
    ).toBe("table_detail_trigger_noDdl");
  });

  it("does not apply connection-type overrides when connection type is missing", () => {
    expect(
      isOpenDdlSupportedForNode("table", undefined, MANIFESTS.mongodb),
    ).toBe(true);
    expect(
      composeOpenDdlAwareContextValue("table", undefined, MANIFESTS.mongodb),
    ).toBe("table");
  });
});
