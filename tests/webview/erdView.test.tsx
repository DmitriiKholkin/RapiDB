import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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

vi.mock("@xyflow/react", async () => {
  const React = await import("react");

  interface MockReactFlowProps {
    nodes?: Array<{
      id: string;
      data: unknown;
      selected?: boolean;
      type?: string;
    }>;
    edges?: Array<{ id: string }>;
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

  function Handle(): React.JSX.Element {
    return <div data-testid="react-flow-handle" />;
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

describe("ErdView", () => {
  it("restores persisted search and filter state", async () => {
    window.__vscode?.getState.mockReturnValue({
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

  it("opens schema and table data from node actions", async () => {
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
      screen.getByRole("button", { name: "Open schema public.users" }),
    );
    await user.click(
      screen.getByRole("button", { name: "Open data public.users" }),
    );

    expect(getPostedMessages()).toContainEqual({
      type: "openSchema",
      payload: {
        database: "app_db",
        schema: "public",
        table: "users",
      },
    });
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
