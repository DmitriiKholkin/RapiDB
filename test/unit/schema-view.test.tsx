/**
 * @vitest-environment jsdom
 */

import {
  cleanup,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ColumnTypeMeta as ColumnMeta,
  ForeignKeyMeta,
  IndexMeta,
} from "../../src/shared/tableTypes";
import { SchemaView } from "../../src/webview/components/SchemaView";
import {
  categoryColor,
  getStructuralBadgePresentation,
} from "../../src/webview/types";

afterEach(cleanup);

const postMessage = vi.fn();
const getState = vi.fn();
const setState = vi.fn();

function makeColumn(
  overrides: Partial<ColumnMeta> & { name: string; type: string },
): ColumnMeta {
  const { name, type, nativeType, ...rest } = overrides;
  return {
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    filterable: true,
    editable: true,
    filterOperators: ["like"],
    valueSemantics: "plain",
    ...rest,
    name,
    type,
    nativeType: nativeType ?? type,
  };
}

function emitSchemaData(
  columns: ColumnMeta[],
  foreignKeys: ForeignKeyMeta[] = [],
  indexes: IndexMeta[] = [],
): void {
  window.dispatchEvent(
    new MessageEvent("message", {
      data: {
        type: "schemaData",
        payload: {
          columns,
          indexes,
          foreignKeys,
        },
      },
    }),
  );
}

async function findColumnRow(name: string): Promise<HTMLElement> {
  const [columnsTable] = await screen.findAllByRole("table");
  const [cell] = within(columnsTable).getAllByText(name);
  return cell.closest("tr") as HTMLElement;
}

async function renderSchemaView(table: string): Promise<void> {
  render(
    <SchemaView
      connectionId="conn-1"
      database="appdb"
      schema="public"
      table={table}
    />,
  );

  await waitFor(() => {
    expect(postMessage).toHaveBeenCalledWith({ type: "ready" });
  });
}

describe("SchemaView", () => {
  beforeEach(() => {
    postMessage.mockReset();
    getState.mockReset();
    setState.mockReset();
    (
      window as Window & {
        __vscode?: {
          postMessage: typeof postMessage;
          getState: typeof getState;
          setState: typeof setState;
        };
      }
    ).__vscode = {
      postMessage,
      getState,
      setState,
    };
  });

  it("renders category badges from runtime enriched column metadata", async () => {
    await renderSchemaView("events");

    emitSchemaData([
      makeColumn({
        name: "payload",
        type: "jsonb",
        category: "json",
        nativeType: "jsonb",
      }),
    ]);

    const row = await screen.findByText("payload");
    expect(
      within(await findColumnRow("payload")).getByText("JSON"),
    ).toBeDefined();
    expect(row).toBeDefined();
  });

  it("renders Oracle interval columns with the interval badge", async () => {
    await renderSchemaView("events");

    emitSchemaData([
      makeColumn({
        name: "duration",
        type: "INTERVAL DAY TO SECOND",
        category: "interval",
        nativeType: "INTERVAL DAY TO SECOND",
        editable: false,
      }),
    ]);

    expect(
      within(await findColumnRow("duration")).getByText("INTV"),
    ).toBeDefined();
  });

  it("renders type badges without borders and keeps GEO readable", async () => {
    await renderSchemaView("places");

    emitSchemaData([
      makeColumn({
        name: "geom",
        type: "geometry",
        category: "spatial",
        nativeType: "geometry",
        editable: false,
      }),
    ]);

    const badge = within(await findColumnRow("geom")).getByText("GEO");
    expect(badge).toBeDefined();
    expect((badge as HTMLElement).style.borderStyle).toBe("none");
    expect((badge as HTMLElement).style.color).toBe(categoryColor("spatial"));
  });

  it("does not show an FK badge when runtime payload has no FK metadata", async () => {
    await renderSchemaView("orders");

    emitSchemaData([
      makeColumn({
        name: "customer_id",
        type: "int",
        category: "integer",
        nativeType: "int",
        isForeignKey: false,
      }),
    ]);

    const row = await screen.findByText("customer_id");
    expect(
      within(await findColumnRow("customer_id")).queryByText("FK"),
    ).toBeNull();
    expect(row).toBeDefined();
  });

  it("shows an FK badge when foreign-key metadata exists for the column", async () => {
    await renderSchemaView("orders");

    emitSchemaData(
      [
        makeColumn({
          name: "customer_id",
          type: "int",
          category: "integer",
          nativeType: "int",
          isForeignKey: false,
        }),
      ],
      [
        {
          constraintName: "orders_customer_id_fkey",
          column: "customer_id",
          referencedSchema: "public",
          referencedTable: "customers",
          referencedColumn: "id",
        },
      ],
    );

    expect(
      await within(await findColumnRow("customer_id")).findByText("FK"),
    ).toBeDefined();
  });

  it("shows an FK badge when the runtime column metadata marks it as foreign key", async () => {
    await renderSchemaView("orders");

    emitSchemaData([
      makeColumn({
        name: "account_id",
        type: "int",
        category: "integer",
        nativeType: "int",
        isForeignKey: true,
      }),
    ]);

    expect(
      await within(await findColumnRow("account_id")).findByText("FK"),
    ).toBeDefined();
  });

  it("renders foreign key and index sections from the schema payload", async () => {
    await renderSchemaView("orders");

    emitSchemaData(
      [
        makeColumn({
          name: "customer_id",
          type: "int",
          category: "integer",
          nativeType: "int",
        }),
      ],
      [
        {
          constraintName: "orders_customer_id_fkey",
          column: "customer_id",
          referencedSchema: "crm",
          referencedTable: "customers",
          referencedColumn: "id",
        },
      ],
      [
        {
          name: "orders_customer_id_idx",
          columns: ["customer_id"],
          unique: false,
          primary: false,
        },
      ],
    );

    expect(await screen.findByText("Foreign Keys")).toBeDefined();
    expect(screen.getByText("crm.customers.id")).toBeDefined();
    expect(screen.getByText("orders_customer_id_fkey")).toBeDefined();
    expect(screen.getByText("Indexes")).toBeDefined();
    expect(screen.getByText("orders_customer_id_idx")).toBeDefined();
    expect(screen.getByText("INDEX")).toBeDefined();
  });

  it("uses the shared structural badge palette for column and index badges", async () => {
    await renderSchemaView("users");

    emitSchemaData(
      [
        makeColumn({
          name: "id",
          type: "int",
          category: "integer",
          nativeType: "int",
          isPrimaryKey: true,
          isForeignKey: true,
          isAutoIncrement: true,
        }),
      ],
      [],
      [
        {
          name: "users_pkey",
          columns: ["id"],
          unique: true,
          primary: true,
        },
        {
          name: "users_email_key",
          columns: ["email"],
          unique: true,
          primary: false,
        },
        {
          name: "users_created_idx",
          columns: ["created_at"],
          unique: false,
          primary: false,
        },
      ],
    );

    const row = await findColumnRow("id");
    const pkBadge = within(row).getByText("PK") as HTMLElement;
    const fkBadge = within(row).getByText("FK") as HTMLElement;
    const aiBadge = within(row).getByText("AI") as HTMLElement;
    const primaryBadge = screen.getByText("PRIMARY") as HTMLElement;
    const uniqueBadge = screen.getByText("UNIQUE") as HTMLElement;
    const indexBadge = screen.getByText("INDEX") as HTMLElement;

    expect(pkBadge.style.color).toBe(
      getStructuralBadgePresentation("pk").foreground,
    );
    expect(fkBadge.style.color).toBe(
      getStructuralBadgePresentation("fk").foreground,
    );
    expect(aiBadge.style.color).toBe(
      getStructuralBadgePresentation("ai").foreground,
    );
    expect(primaryBadge.style.color).toBe(
      getStructuralBadgePresentation("primary").foreground,
    );
    expect(uniqueBadge.style.color).toBe(
      getStructuralBadgePresentation("unique").foreground,
    );
    expect(indexBadge.style.color).toBe(
      getStructuralBadgePresentation("index").foreground,
    );
  });
});
