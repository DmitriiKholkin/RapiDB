import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ErdGraph } from "../../src/shared/webviewContracts";
import { ErdView } from "../../src/webview/components/ErdView";
import { dispatchIncomingMessage } from "./testUtils";

vi.mock("@xyflow/react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xyflow/react")>();
  function ReactFlow(): React.JSX.Element {
    return <div data-testid="react-flow" />;
  }
  return { ...actual, ReactFlow, default: ReactFlow };
});

const ORDERS = JSON.stringify(["app", "sales", "orders"]);

function graph(extra: Partial<ErdGraph> = {}): ErdGraph {
  return {
    scope: { database: "app", schema: "sales" },
    nodes: [
      {
        id: ORDERS,
        database: "app",
        schema: "sales",
        table: "orders",
        isView: false,
        position: { x: 0, y: 0 },
        columns: [
          {
            name: "user_id",
            type: "int4",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: false,
          },
        ],
      },
    ],
    edges: [],
    ...extra,
  };
}

async function sendGraph(value: ErdGraph) {
  await act(async () => {
    dispatchIncomingMessage("erdGraph", {
      graph: value,
      fromCache: false,
      partial: Boolean(value.omittedTables),
      loadedAt: new Date().toISOString(),
    });
  });
}

describe("ErdView graph notices", () => {
  it("shows omitted tables and references outside the diagram", async () => {
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(
      graph({
        omittedTables: [
          {
            database: "app",
            schema: "sales",
            table: "vec_items",
            error: "no such module: vec0",
          },
        ],
        externalReferences: [
          {
            fromTableId: ORDERS,
            fromColumns: ["user_id"],
            constraintName: "orders_user_id_fkey",
            database: "app",
            schema: "auth",
            table: "users",
            columns: ["id"],
          },
        ],
      }),
    );

    const notices = screen.getByRole("status", { name: "Diagram warnings" });
    expect(notices.textContent).toContain(
      "Could not load 1 table(s), so they are not shown: sales.vec_items (no such module: vec0)",
    );
    expect(notices.textContent).toContain(
      "1 relationship(s) point to tables outside this diagram: orders.user_id → auth.users.id",
    );
  });

  it("truncates long notice lists and clears notices for a complete graph", async () => {
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(
      graph({
        omittedTables: Array.from({ length: 7 }, (_, index) => ({
          database: "app",
          schema: "sales",
          table: `t${index}`,
          error: "boom",
        })),
      }),
    );
    expect(
      screen.getByRole("status", { name: "Diagram warnings" }).textContent,
    ).toContain("sales.t4 (boom), and 2 more");

    await sendGraph(graph());
    expect(
      screen.queryByRole("status", { name: "Diagram warnings" }),
    ).toBeNull();
  });

  it("shows a composite key once and qualifies the source when no schema is scoped", async () => {
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(
      graph({
        scope: { database: "app" },
        externalReferences: [
          {
            fromTableId: ORDERS,
            fromColumns: ["tenant_id", "user_id"],
            constraintName: "orders_tenant_user_fkey",
            database: "app",
            schema: "auth",
            table: "users",
            columns: ["tenant_id", "id"],
          },
        ],
      }),
    );

    expect(
      screen.getByRole("status", { name: "Diagram warnings" }).textContent,
    ).toContain(
      "1 relationship(s) point to tables outside this diagram: sales.orders(tenant_id, user_id) → auth.users(tenant_id, id)",
    );
  });

  it("truncates long external reference lists", async () => {
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(
      graph({
        externalReferences: Array.from({ length: 7 }, (_, index) => ({
          fromTableId: ORDERS,
          fromColumns: [`c${index}`],
          constraintName: `fk_${index}`,
          database: "app",
          schema: "auth",
          table: `t${index}`,
          columns: ["id"],
        })),
      }),
    );

    const text = screen.getByRole("status", {
      name: "Diagram warnings",
    }).textContent;
    expect(text).toContain(
      "7 relationship(s) point to tables outside this diagram: orders.c0 → auth.t0.id",
    );
    expect(text).toContain("orders.c4 → auth.t4.id, and 2 more");
    expect(text).not.toContain("auth.t5");
  });
});
