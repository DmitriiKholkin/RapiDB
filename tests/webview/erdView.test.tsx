import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { NodeChange } from "@xyflow/react";
import { describe, expect, it, vi } from "vitest";
import type { ErdGraph } from "../../src/shared/webviewContracts";
import { ErdView } from "../../src/webview/components/ErdView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getPostedMessages,
} from "./testUtils";

const fitViewMock = vi.hoisted(() => vi.fn());
const setViewportMock = vi.hoisted(() => vi.fn());
const getViewportMock = vi.hoisted(() =>
  vi.fn(() => ({ x: 0, y: 0, zoom: 1 })),
);

interface MockFlowNode {
  id: string;
  position: { x: number; y: number };
  data: unknown;
  selected?: boolean;
  type?: string;
}

interface FlowSnapshot {
  nodes?: MockFlowNode[];
  edges?: Array<{
    id: string;
    source: string;
    target: string;
    sourceHandle?: string;
    targetHandle?: string;
  }>;
  onNodesChange?: (changes: NodeChange[]) => void;
  onNodeDragStop?: (event: MouseEvent, node: MockFlowNode) => void;
  onMove?: (
    event: null,
    viewport: { x: number; y: number; zoom: number },
  ) => void;
  onMoveEnd?: () => void;
}

const flowSnapshot = vi.hoisted(() => ({ current: {} as FlowSnapshot }));

vi.mock("@xyflow/react", async (importOriginal) => {
  const React = await import("react");
  const actual = await importOriginal<typeof import("@xyflow/react")>();

  interface MockReactFlowProps extends FlowSnapshot {
    nodeTypes?: Record<
      string,
      React.ComponentType<{
        id: string;
        data: unknown;
        selected?: boolean;
      }>
    >;
    onInit?: (instance: {
      fitView: typeof fitViewMock;
      setViewport: typeof setViewportMock;
      getViewport: typeof getViewportMock;
    }) => void;
    onNodeClick?: (
      event: React.MouseEvent,
      node: { id: string; data: unknown },
    ) => void;
    children?: React.ReactNode;
  }

  function ReactFlow(props: MockReactFlowProps): React.JSX.Element {
    flowSnapshot.current = props;
    React.useEffect(() => {
      props.onInit?.({
        fitView: fitViewMock,
        setViewport: setViewportMock,
        getViewport: getViewportMock,
      });
    }, []);

    const TableNode = props.nodeTypes?.tableNode;

    return (
      <div data-testid="react-flow">
        <div data-testid="react-flow-edge-count">
          {String(props.edges?.length ?? 0)}
        </div>
        {props.nodes?.map((node) => (
          <div key={node.id}>
            {TableNode ? (
              <TableNode
                id={node.id}
                data={node.data}
                selected={node.selected ?? false}
              />
            ) : null}
            <button
              type="button"
              onClick={(event) => {
                props.onNodeClick?.(event, { id: node.id, data: node.data });
              }}
            >
              SelectNode:{node.id}
            </button>
          </div>
        ))}
        {props.children}
      </div>
    );
  }

  function Background(): React.JSX.Element {
    return <div data-testid="react-flow-background" />;
  }

  function Handle(props: { id: string; type: string }): React.JSX.Element {
    return (
      <div
        data-testid="react-flow-handle"
        data-handle-id={props.id}
        data-handle-type={props.type}
      />
    );
  }

  function Controls(): React.JSX.Element {
    return <div data-testid="react-flow-controls" />;
  }

  function ControlButton(props: {
    onClick?: () => void;
    children?: React.ReactNode;
  }): React.JSX.Element {
    return (
      <button type="button" onClick={props.onClick}>
        {props.children}
      </button>
    );
  }

  function BaseEdge(): React.JSX.Element {
    return <div data-testid="react-flow-base-edge" />;
  }

  function getSmoothStepPath(): [string, number, number, number, number] {
    return ["M 0 0 L 1 1", 0, 0, 0, 0];
  }

  function useReactFlow() {
    return {
      fitView: fitViewMock,
      zoomIn: vi.fn(),
      zoomOut: vi.fn(),
      setViewport: setViewportMock,
      getViewport: getViewportMock,
    };
  }

  return {
    ...actual,
    __esModule: true,
    ReactFlow,
    default: ReactFlow,
    Background,
    BaseEdge,
    Handle,
    Controls,
    ControlButton,
    getSmoothStepPath,
    useReactFlow,
    Position: {
      Left: "left",
      Right: "right",
    },
    MarkerType: {
      ArrowClosed: "arrowclosed",
    },
  };
});

function sampleGraph(): ErdGraph {
  return {
    scope: {
      database: "app_db",
      schema: "public",
    },
    nodes: [
      {
        id: "app_db.public.users",
        database: "app_db",
        schema: "public",
        table: "users",
        isView: false,
        position: { x: 0, y: 0 },
        columns: [
          {
            name: "id",
            type: "int4",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
          {
            name: "email",
            type: "text",
            isPrimaryKey: false,
            isForeignKey: false,
            nullable: false,
          },
        ],
      },
      {
        id: "app_db.public.orders",
        database: "app_db",
        schema: "public",
        table: "orders",
        isView: false,
        position: { x: 400, y: 0 },
        columns: [
          {
            name: "id",
            type: "int4",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
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
    edges: [
      {
        id: "orders_users_fk",
        fromTableId: "app_db.public.orders",
        toTableId: "app_db.public.users",
        fromColumn: "user_id",
        toColumn: "id",
        constraintName: "orders_user_id_fkey",
        cardinality: "many-to-one",
        sourceNullable: false,
      },
    ],
  };
}

const USERS = "app_db.public.users";
const ORDERS = "app_db.public.orders";
const HIDDEN = "app_db.public.audit";
const savedPositions = {
  [USERS]: { x: 700, y: 800 },
  [ORDERS]: { x: 900, y: 1000 },
  [HIDDEN]: { x: 1200, y: 1300 },
};

function graphWithHiddenNode(): ErdGraph {
  const graph = sampleGraph();
  graph.nodes.push({
    ...graph.nodes[0],
    id: HIDDEN,
    table: "audit",
    columns: [],
  });
  return graph;
}

function vscodeStateMock() {
  return window.__vscode as NonNullable<Window["__vscode"]> & {
    getState: ReturnType<typeof vi.fn>;
    setState: ReturnType<typeof vi.fn>;
  };
}

function lastSavedPositions(): Record<string, { x: number; y: number }> {
  return vscodeStateMock().setState.mock.calls.at(-1)?.[0].nodePositions;
}

async function sendGraph(
  graph = graphWithHiddenNode(),
  loadedAt = "same-time",
) {
  await act(async () => {
    dispatchIncomingMessage("erdGraph", { graph, fromCache: false, loadedAt });
  });
}

function renderedPosition(id: string) {
  return flowSnapshot.current.nodes?.find((node) => node.id === id)?.position;
}

describe("ErdView", () => {
  it.each([
    "search",
    "isolated",
    "no-match",
  ])("retains hidden saved positions on the initial response with restored %s filter", async (filter) => {
    vscodeStateMock().getState.mockReturnValue({
      search:
        filter === "search" ? "users" : filter === "no-match" ? "absent" : "",
      hideUnmatched: filter !== "isolated",
      hideIsolated: filter === "isolated",
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    expect(screen.queryByText("public.audit")).toBeNull();
    expect(lastSavedPositions()).toEqual(savedPositions);
    if (filter === "no-match") {
      expect(flowSnapshot.current.nodes).toHaveLength(0);
    } else {
      expect(renderedPosition(USERS)).toEqual(savedPositions[USERS]);
    }

    await userEvent.setup().click(
      screen.getByRole("checkbox", {
        name: filter === "isolated" ? "Hide isolated" : "Hide non-focus",
      }),
    );
    expect(renderedPosition(HIDDEN)).toEqual(savedPositions[HIDDEN]);
  });

  it("restores hidden positions after reload and webview rehydration", async () => {
    const user = userEvent.setup();
    vscodeStateMock().getState.mockReturnValue({
      nodePositions: savedPositions,
    });
    const view = render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    await user.click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    await user.click(screen.getByRole("button", { name: "Reload" }));
    await act(async () => {
      dispatchIncomingMessage("erdLoading", { forceReload: true });
    });
    expect(lastSavedPositions()[HIDDEN]).toEqual(savedPositions[HIDDEN]);
    await sendGraph(graphWithHiddenNode(), "later-time");
    expect(lastSavedPositions()).toEqual(savedPositions);

    const persisted = vscodeStateMock().setState.mock.calls.at(-1)?.[0];
    view.unmount();
    vscodeStateMock().getState.mockReturnValue(persisted);
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    await user.click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    expect(renderedPosition(HIDDEN)).toEqual(savedPositions[HIDDEN]);
  });

  it("prunes removed nodes on every authoritative response even with repeated loadedAt", async () => {
    vscodeStateMock().getState.mockReturnValue({
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    const removedNode = flowSnapshot.current.nodes?.find(
      (node) => node.id === HIDDEN,
    );
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    await sendGraph(sampleGraph());
    expect(lastSavedPositions()).toEqual({
      [USERS]: savedPositions[USERS],
      [ORDERS]: savedPositions[ORDERS],
    });
    await act(async () => {
      if (removedNode) {
        flowSnapshot.current.onNodeDragStop?.(
          new MouseEvent("mouseup"),
          removedNode,
        );
      }
      flowSnapshot.current.onMoveEnd?.();
    });
    expect(lastSavedPositions()[HIDDEN]).toBeUndefined();
    await sendGraph({ scope: {}, nodes: [], edges: [] });
    expect(lastSavedPositions()).toEqual({});
  });

  it("prunes stale saved IDs at first response, including when all nodes are hidden", async () => {
    vscodeStateMock().getState.mockReturnValue({
      search: "absent",
      hideUnmatched: true,
      nodePositions: { ...savedPositions, deleted: { x: 1, y: 2 } },
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    expect(lastSavedPositions()).toEqual(savedPositions);
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide non-focus" }));
    expect(renderedPosition(HIDDEN)).toEqual(savedPositions[HIDDEN]);
  });

  it("commits viewport position snapshots before later filter persistence", async () => {
    vscodeStateMock().getState.mockReturnValue({
      hideIsolated: true,
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    getViewportMock.mockReturnValueOnce({ x: 10, y: 20, zoom: 0.5 });
    await act(async () => {
      flowSnapshot.current.onNodesChange?.([
        { type: "position", id: USERS, position: { x: 80, y: 90 } },
      ]);
      flowSnapshot.current.onMoveEnd?.();
    });
    await userEvent
      .setup()
      .type(
        screen.getByRole("textbox", { name: "Search tables and columns" }),
        "users",
      );
    expect(lastSavedPositions()).toEqual({
      ...savedPositions,
      [USERS]: { x: 80, y: 90 },
    });
    expect(vscodeStateMock().setState.mock.calls.at(-1)?.[0].viewport).toEqual({
      x: 10,
      y: 20,
      zoom: 0.5,
    });
  });

  it("saves manual drag and viewport changes without losing hidden positions", async () => {
    vscodeStateMock().getState.mockReturnValue({
      hideIsolated: true,
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    const dragged = {
      ...flowSnapshot.current.nodes?.find((n) => n.id === USERS),
    } as MockFlowNode;
    dragged.position = { x: 50, y: 75 };
    await act(async () => {
      flowSnapshot.current.onNodesChange?.([
        {
          type: "position",
          id: USERS,
          position: dragged.position,
          dragging: false,
        },
      ]);
      flowSnapshot.current.onNodeDragStop?.(new MouseEvent("mouseup"), dragged);
      flowSnapshot.current.onMoveEnd?.();
    });
    expect(lastSavedPositions()).toEqual({
      ...savedPositions,
      [USERS]: dragged.position,
    });
    await sendGraph(graphWithHiddenNode(), "after-drag");
    expect(renderedPosition(USERS)).toEqual(dragged.position);
    expect(lastSavedPositions()[HIDDEN]).toEqual(savedPositions[HIDDEN]);
  });

  it("merges live visible changes on refresh and keeps them when filters persist state", async () => {
    vscodeStateMock().getState.mockReturnValue({
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    await act(async () => {
      flowSnapshot.current.onNodesChange?.([
        {
          type: "position",
          id: USERS,
          position: { x: 40, y: 60 },
          dragging: true,
        },
      ]);
      dispatchIncomingMessage("erdGraph", {
        graph: graphWithHiddenNode(),
        fromCache: false,
        loadedAt: "same-time",
      });
    });
    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    expect(lastSavedPositions()).toEqual({
      ...savedPositions,
      [USERS]: { x: 40, y: 60 },
    });
  });

  it("lays out new hidden nodes from the full graph and preserves existing positions", async () => {
    vscodeStateMock().getState.mockReturnValue({
      hideIsolated: true,
      nodePositions: {
        [USERS]: savedPositions[USERS],
        [ORDERS]: savedPositions[ORDERS],
      },
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(sampleGraph());
    await sendGraph();
    const addedPosition = lastSavedPositions()[HIDDEN];
    expect(Number.isFinite(addedPosition?.x)).toBe(true);
    expect(Number.isFinite(addedPosition?.y)).toBe(true);
    expect(addedPosition).not.toEqual({ x: 0, y: 0 });
    expect(lastSavedPositions()[USERS]).toEqual(savedPositions[USERS]);
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    expect(renderedPosition(HIDDEN)).toEqual(addedPosition);
    await sendGraph(graphWithHiddenNode(), "next-response");
    expect(lastSavedPositions()[HIDDEN]).toEqual(addedPosition);
  });

  it("keeps a newer live position after graph response commits and viewport saves", async () => {
    vscodeStateMock().getState.mockReturnValue({
      search: "users",
      hideUnmatched: true,
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    expect(renderedPosition(USERS)).toEqual(savedPositions[USERS]);

    const refreshed = graphWithHiddenNode();
    const addedId = "app_db.public.payments";
    refreshed.nodes = refreshed.nodes.filter((node) => node.id !== ORDERS);
    refreshed.nodes.push({
      ...sampleGraph().nodes[1],
      id: addedId,
      table: "payments",
    });
    refreshed.nodes[0].columns.push({
      name: "refreshed",
      type: "text",
      nullable: false,
      isPrimaryKey: false,
      isForeignKey: false,
    });
    refreshed.edges[0].fromTableId = addedId;

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: refreshed,
        fromCache: false,
        loadedAt: "same-time",
      });
      flowSnapshot.current.onNodesChange?.([
        { type: "position", id: USERS, position: { x: 40, y: 60 } },
      ]);
    });

    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });
    expect(screen.getByText("refreshed")).toBeTruthy();
    expect(renderedPosition(ORDERS)).toBeUndefined();
    expect(screen.queryByText("public.audit")).toBeNull();
    const addedPosition = renderedPosition(addedId);
    expect(Number.isFinite(addedPosition?.x)).toBe(true);
    expect(Number.isFinite(addedPosition?.y)).toBe(true);
    expect(addedPosition).toEqual(lastSavedPositions()[addedId]);

    await act(async () => flowSnapshot.current.onMoveEnd?.());
    expect(lastSavedPositions()).toEqual({
      [USERS]: { x: 40, y: 60 },
      [HIDDEN]: savedPositions[HIDDEN],
      [addedId]: addedPosition,
    });
    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });
  });

  it("keeps unsaved live coordinates while changing LOD presentation", async () => {
    vscodeStateMock().getState.mockReturnValue({
      hideIsolated: true,
      nodePositions: savedPositions,
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    await act(async () => {
      flowSnapshot.current.onNodesChange?.([
        { type: "position", id: USERS, position: { x: 40, y: 60 } },
      ]);
    });
    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });

    await act(async () =>
      flowSnapshot.current.onMove?.(null, { x: 0, y: 0, zoom: 0.2 }),
    );
    expect(screen.queryByText("email")).toBeNull();
    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });
    await act(async () => flowSnapshot.current.onMoveEnd?.());
    expect(lastSavedPositions()).toEqual({
      ...savedPositions,
      [USERS]: { x: 40, y: 60 },
    });

    await act(async () =>
      flowSnapshot.current.onMove?.(null, { x: 0, y: 0, zoom: 1 }),
    );
    expect(screen.getByText("email")).toBeTruthy();
    expect(renderedPosition(USERS)).toEqual({ x: 40, y: 60 });
  });

  it("retains auto-layout positions across filtering, LOD changes and graph refresh", async () => {
    render(<ErdView connectionId="conn-1" />);
    await sendGraph();
    const positions = lastSavedPositions();
    expect(Object.keys(positions).sort()).toEqual(
      [USERS, ORDERS, HIDDEN].sort(),
    );
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    await act(async () =>
      flowSnapshot.current.onMove?.(null, { x: 0, y: 0, zoom: 0.2 }),
    );
    await sendGraph(graphWithHiddenNode(), "zoomed-out");
    expect(lastSavedPositions()).toEqual(positions);
    await act(async () =>
      flowSnapshot.current.onMove?.(null, { x: 0, y: 0, zoom: 1 }),
    );
    await userEvent
      .setup()
      .click(screen.getByRole("checkbox", { name: "Hide isolated" }));
    expect(renderedPosition(HIDDEN)).toEqual(positions[HIDDEN]);
  });

  it.each([
    1, 0.2,
  ])("renders exact-case and encoded handles matching edges at zoom %s", async (zoom) => {
    const names = ["Foo", "foo", "FOO", "%", "%25", "列/Имя 🐘", 'a.#: "b"'];
    const graph = sampleGraph();
    graph.nodes = graph.nodes.map((node) => ({
      ...node,
      columns: names.map((name) => ({
        name,
        type: "text",
        nullable: false,
        isPrimaryKey: false,
        isForeignKey: true,
      })),
    }));
    graph.edges = names.map((name, index) => ({
      ...graph.edges[0],
      id: `edge-${index}`,
      fromColumn: name,
      toColumn: names[(index + 1) % names.length],
    }));
    vscodeStateMock().getState.mockReturnValue({
      viewport: { x: 0, y: 0, zoom },
    });
    render(<ErdView connectionId="conn-1" />);
    await sendGraph(graph);
    for (const node of graph.nodes) {
      const handles = within(
        screen.getByRole("region", { name: `public.${node.table}` }),
      )
        .getAllByTestId("react-flow-handle")
        .map((handle) => handle.getAttribute("data-handle-id"));
      expect(new Set(handles).size).toBe(names.length * 2);
      expect(handles).toEqual(
        names.flatMap((name) => [
          `left-${encodeURIComponent(name)}`,
          `right-${encodeURIComponent(name)}`,
        ]),
      );
    }
    graph.edges.forEach((edge, index) => {
      expect(flowSnapshot.current.edges?.[index]).toMatchObject({
        source: edge.fromTableId,
        target: edge.toTableId,
        sourceHandle: `right-${encodeURIComponent(edge.fromColumn)}`,
        targetHandle: `left-${encodeURIComponent(edge.toColumn)}`,
      });
    });
  });

  it("restores persisted search and filter state", async () => {
    const vscodeApi = window.__vscode as
      | (NonNullable<Window["__vscode"]> & {
          getState: ReturnType<typeof vi.fn>;
        })
      | undefined;

    vscodeApi?.getState.mockReturnValue({
      search: "users",
      hideUnmatched: true,
      hideIsolated: true,
      viewport: { x: 10, y: 20, zoom: 0.75 },
    });

    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: sampleGraph(),
        fromCache: false,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(
        (
          screen.getByRole("textbox", {
            name: "Search tables and columns",
          }) as HTMLInputElement
        ).value,
      ).toBe("users");
      expect(
        (
          screen.getByRole("checkbox", {
            name: "Hide non-focus",
          }) as HTMLInputElement
        ).checked,
      ).toBe(true);
      expect(
        (
          screen.getByRole("checkbox", {
            name: "Hide isolated",
          }) as HTMLInputElement
        ).checked,
      ).toBe(true);
    });
  });

  it("posts ready, renders loading then graph", async () => {
    const { container } = render(
      <ErdView connectionId="conn-1" database="app_db" schema="public" />,
    );

    expect(getPostedMessages()).toEqual([{ type: "ready" }]);
    expect(screen.getByText("Loading data...")).toBeTruthy();

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: sampleGraph(),
        fromCache: false,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(screen.getByText("public.users")).toBeTruthy();
      expect(screen.getByText("public.orders")).toBeTruthy();
      expect(screen.getByTestId("react-flow-edge-count").textContent).toBe("1");
    });

    await expectNoAxeViolations(container);
  });

  it("shows error message from erdError", async () => {
    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdError", {
        error: "Could not load schema metadata",
      });
    });

    expect(screen.getByText("Error:")).toBeTruthy();
    expect(screen.getByText("Could not load schema metadata")).toBeTruthy();
  });

  it("shows the empty state when the selected scope has no tables", async () => {
    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: {
          scope: {
            database: "app_db",
            schema: "public",
          },
          nodes: [],
          edges: [],
        },
        fromCache: false,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(
        screen.getByText("No tables found for the selected scope."),
      ).toBeTruthy();
      expect(screen.queryByTestId("react-flow")).toBeNull();
    });
  });

  it("supports search filter and control actions", async () => {
    const user = userEvent.setup();
    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: sampleGraph(),
        fromCache: true,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(screen.getByText("cached")).toBeTruthy();
    });

    await user.type(
      screen.getByRole("textbox", { name: "Search tables and columns" }),
      "users",
    );

    await waitFor(() => {
      expect(screen.getByText("public.users")).toBeTruthy();
      expect(screen.getByText("public.orders")).toBeTruthy();
      expect(screen.getByText("Showing 2 of 2 tables")).toBeTruthy();
    });

    await user.click(screen.getByRole("checkbox", { name: "Hide non-focus" }));

    await waitFor(() => {
      expect(screen.getByText("public.users")).toBeTruthy();
      expect(screen.getByText("public.orders")).toBeTruthy();
      expect(screen.getByText("Showing 2 of 2 tables")).toBeTruthy();
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Reload" }));
    expect(
      (
        screen.getByRole("textbox", {
          name: "Search tables and columns",
        }) as HTMLInputElement
      ).value,
    ).toBe("users");
    expect(getPostedMessages()).toContainEqual({ type: "reload" });
  });

  it("opens table data from node actions", async () => {
    const user = userEvent.setup();
    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: sampleGraph(),
        fromCache: false,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(screen.getByText("public.users")).toBeTruthy();
    });

    clearPostedMessages();

    await user.click(
      screen.getByRole("button", { name: "Open data public.users" }),
    );

    expect(getPostedMessages()).toContainEqual({
      type: "openTableData",
      payload: {
        database: "app_db",
        schema: "public",
        table: "users",
        isView: false,
      },
    });
  });

  it("keeps current graph visible while showing overlay loader on reload", async () => {
    render(<ErdView connectionId="conn-1" database="app_db" schema="public" />);

    await act(async () => {
      dispatchIncomingMessage("erdGraph", {
        graph: sampleGraph(),
        fromCache: false,
        loadedAt: "2026-05-02T12:00:00.000Z",
      });
    });

    await waitFor(() => {
      expect(screen.getByText("public.users")).toBeTruthy();
      expect(screen.queryByText("Loading ERD...")).toBeNull();
    });

    await act(async () => {
      dispatchIncomingMessage("erdLoading", {
        forceReload: true,
      });
    });

    expect(screen.getByText("public.users")).toBeTruthy();
    expect(screen.getByText("Loading data...")).toBeTruthy();
  });
});
