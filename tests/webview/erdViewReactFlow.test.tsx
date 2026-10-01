import { act, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ErdGraph } from "../../src/shared/webviewContracts";
import { ErdView } from "../../src/webview/components/ErdView";
import { dispatchIncomingMessage } from "./testUtils";

// Supply the browser measurements JSDOM lacks; ReactFlow, TableNode, Handle and
// RelationshipEdge are real. Different row bounds expose incorrect anchoring.
function installFlowMeasurements() {
  vi.spyOn(HTMLElement.prototype, "offsetWidth", "get").mockImplementation(
    function (this: HTMLElement) {
      return this.classList.contains("react-flow__handle")
        ? 8
        : this.classList.contains("react-flow__node")
          ? 320
          : 1000;
    },
  );
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(
    function (this: HTMLElement) {
      return this.classList.contains("react-flow__handle")
        ? 8
        : this.classList.contains("react-flow__node")
          ? 200
          : 700;
    },
  );
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(
    function (this: HTMLElement) {
      const isHandle = this.classList.contains("react-flow__handle");
      const row = this.parentElement;
      const rowIndex = row?.parentElement
        ? Array.from(row.parentElement.children).indexOf(row)
        : 0;
      return {
        x: 0,
        y: 0,
        left: isHandle && this.classList.contains("source") ? 320 : 0,
        top: isHandle ? 60 + rowIndex * 20 : 0,
        right: this.offsetWidth,
        bottom: this.offsetHeight,
        width: this.offsetWidth,
        height: this.offsetHeight,
        toJSON: () => ({}),
      };
    },
  );
  vi.stubGlobal(
    "DOMMatrixReadOnly",
    class {
      m22 = 1;
    },
  );
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(private callback: ResizeObserverCallback) {}
      observe(target: Element) {
        queueMicrotask(() => {
          if (target.isConnected) {
            this.callback(
              [{ target } as ResizeObserverEntry],
              this as unknown as ResizeObserver,
            );
          }
        });
      }
      unobserve() {}
      disconnect() {}
    },
  );
}

describe("ErdView with real ReactFlow", () => {
  it.each([
    1, 0.2,
  ])("renders case-sensitive and encoded self edges at zoom %s", async (zoom) => {
    installFlowMeasurements();
    const names = ["Foo", "foo", "%", "%25", "列/Имя 🐘", 'a.#: "b"'];
    const vscodeApi = window.__vscode as NonNullable<Window["__vscode"]> & {
      getState: ReturnType<typeof vi.fn>;
    };
    vscodeApi.getState.mockReturnValue({ viewport: { x: 0, y: 0, zoom } });
    const nodeId = JSON.stringify(["app_db", "public", "quoted"]);
    const graph: ErdGraph = {
      scope: { database: "app_db", schema: "public" },
      nodes: [
        {
          id: nodeId,
          database: "app_db",
          schema: "public",
          table: "quoted",
          isView: false,
          position: { x: 0, y: 0 },
          columns: names.map((name) => ({
            name,
            type: "text",
            isPrimaryKey: false,
            isForeignKey: true,
            nullable: false,
          })),
        },
      ],
      edges: [
        { id: "upper-to-lower", fromColumn: "Foo", toColumn: "foo" },
        { id: "lower-to-upper", fromColumn: "foo", toColumn: "Foo" },
        { id: "percent-to-encoded-percent", fromColumn: "%", toColumn: "%25" },
        {
          id: "encoded-percent-to-unicode",
          fromColumn: "%25",
          toColumn: "列/Имя 🐘",
        },
        {
          id: "unicode-to-special",
          fromColumn: "列/Имя 🐘",
          toColumn: 'a.#: "b"',
        },
        { id: "special-to-percent", fromColumn: 'a.#: "b"', toColumn: "%" },
      ].map((edge) => ({
        ...edge,
        fromTableId: nodeId,
        toTableId: nodeId,
        constraintName: edge.id,
        cardinality: "many-to-one",
        sourceNullable: false,
      })),
    };
    const { container } = render(<ErdView connectionId="conn-1" />);
    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph,
        fromCache: false,
        loadedAt: "now",
      });
    });
    await screen.findByText("public.quoted");
    await waitFor(() => {
      const handles = Array.from(
        container.querySelectorAll(".react-flow__handle"),
      );
      expect(
        handles.map((handle) => handle.getAttribute("data-handleid")),
      ).toEqual(
        names.flatMap((name) => [
          `left-${encodeURIComponent(name)}`,
          `right-${encodeURIComponent(name)}`,
        ]),
      );
      expect(
        handles.every(
          (handle) => handle.getAttribute("data-nodeid") === nodeId,
        ),
      ).toBe(true);
      const paths = Array.from(
        container.querySelectorAll(".react-flow__edge-path"),
      );
      expect(paths).toHaveLength(graph.edges.length);
      expect(new Set(paths.map((path) => path.getAttribute("d"))).size).toBe(
        graph.edges.length,
      );
      const pathById = new Map(
        paths.map((path) => [
          path.closest(".react-flow__edge")?.getAttribute("data-id"),
          path.getAttribute("d") ?? "",
        ]),
      );
      const sourceY = (path: string) =>
        Number(path.match(/^M\s*[\d.-]+\s+([\d.-]+)/)?.[1]);
      const targetY = (path: string) =>
        Number(path.match(/[\d.-]+\s+([\d.-]+)\s*$/)?.[1]);
      const firstRowY = sourceY(pathById.get("upper-to-lower") ?? "");
      for (const edge of graph.edges) {
        const path = pathById.get(edge.id) ?? "";
        expect(sourceY(path) - firstRowY).toBe(
          names.indexOf(edge.fromColumn) * 20,
        );
        expect(targetY(path) - firstRowY).toBe(
          names.indexOf(edge.toColumn) * 20,
        );
      }
      expect(Boolean(screen.queryByText("Foo"))).toBe(zoom >= 0.3);
    });
  });
});
