import { act, fireEvent, render, screen } from "@testing-library/react";
import { type Document, Int32, Long } from "mongodb";
import { expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { TableView } from "../../src/webview/components/TableView";
import { startMongoWireFixture } from "../support/mongodbWireFixture";
import { dispatchIncomingMessage, getPostedMessages } from "./testUtils";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count }: { count: number }) => ({
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: index,
        start: index * 26,
        end: (index + 1) * 26,
      })),
    getTotalSize: () => count * 26,
  }),
}));

it.each([
  ["promoteValues=false", new Int32(26)],
  ["useBigInt64=true", Long.fromNumber(26)],
])("renders rows and paginates with MongoDB URI option %s", async (options, count) => {
  const documents = Array.from({ length: 26 }, (_, index) => ({
    _id: String(index + 1).padStart(2, "0"),
    label: `item ${index + 1}`,
  }));
  const fixture = await startMongoWireFixture((command) => {
    const batch = (collection: string, firstBatch: Document[]) => ({
      ok: 1,
      cursor: { id: Long.ZERO, ns: `db.${collection}`, firstBatch },
    });
    if (command.ping) return { ok: 1 };
    if (command.listCollections)
      return batch("$cmd.listCollections", [
        { name: "items", type: "collection" },
      ]);
    if (command.find) {
      const skip = command.skip ?? 0;
      return batch("items", documents.slice(skip, skip + command.limit));
    }
    if (command.aggregate && command.pipeline.at(-1)?.$group)
      return batch("items", [{ n: count }]);
    return { ok: 0, errmsg: "Unexpected command in MongoDB test fixture" };
  });
  const driver = new MongoDBDriver({
    id: "mongo-counts-fixture",
    name: "MongoDB counts fixture",
    type: "mongodb",
    mongoConnectionMode: "uri",
    connectionUri: `mongodb://127.0.0.1:${fixture.port}/db?directConnection=true&${options}`,
  });
  try {
    await driver.connect();
    render(
      <TableView
        connectionId="mongo-counts-fixture"
        database="db"
        schema="db"
        table="items"
        mongoRowIdentity
      />,
    );
    const columns = await driver.describeColumns("db", "db", "items");
    act(() =>
      dispatchIncomingMessage("tableInit", {
        columns,
        primaryKeyColumns: ["_id"],
      }),
    );

    async function deliverPage(expectedPage: number) {
      const fetch = getPostedMessages()
        .filter(({ type }) => type === "fetchPage")
        .at(-1)?.payload as
        | { fetchId: number; page: number; pageSize: number }
        | undefined;
      expect(fetch?.page).toBe(expectedPage);
      if (!fetch) throw new Error("Missing Table Viewer fetch");
      const page = await driver.readTablePage({
        database: "db",
        schema: "db",
        table: "items",
        page: fetch.page,
        pageSize: fetch.pageSize,
        filters: [],
        sort: null,
        skipCount: false,
      });
      const reply = { fetchId: fetch.fetchId, ...page };
      // Assert the host-side number before JSON can unwrap an Int32.
      expect(reply.totalCount).toBe(26);
      act(() =>
        dispatchIncomingMessage("tableData", JSON.parse(JSON.stringify(reply))),
      );
    }

    await deliverPage(1);
    expect(screen.getByText("item 1")).toBeTruthy();
    expect(screen.getByText(/26 rows total/)).toBeTruthy();
    expect(screen.getByText("Page 1 of 2")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Next →" }));
    await deliverPage(2);
    expect(screen.getByText("item 26")).toBeTruthy();
    expect(screen.getByText("Page 2 of 2")).toBeTruthy();
  } finally {
    try {
      await driver.disconnect();
    } finally {
      await fixture.close();
    }
  }
});
