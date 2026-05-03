import dagre from "@dagrejs/dagre";
import {
  applyNodeChanges,
  Background,
  BaseEdge,
  ControlButton,
  Controls,
  type Edge,
  type EdgeProps,
  getSmoothStepPath,
  Handle,
  type Node,
  type NodeChange,
  type NodeProps,
  Position,
  ReactFlow,
  type ReactFlowInstance,
  useReactFlow,
} from "@xyflow/react";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import "@xyflow/react/dist/style.css";
import type { ErdGraph } from "../../shared/webviewContracts";
import { onMessage, postMessage } from "../utils/messaging";
import { GridLoadingOverlay } from "./GridOverlay";

interface ErdGraphMessage {
  graph: ErdGraph;
  fromCache: boolean;
  loadedAt: string;
}

interface Props {
  connectionId: string;
  database?: string;
  schema?: string;
}

interface ErdViewState {
  search?: string;
  hideUnmatched?: boolean;
  hideIsolated?: boolean;
  viewport?: {
    x: number;
    y: number;
    zoom: number;
  };
  nodePositions?: Record<string, { x: number; y: number }>;
}

interface TableNodeData extends Record<string, unknown> {
  node: ErdGraph["nodes"][number];
  isDimmed: boolean;
  lodLevel: LODLevel;
  onOpenSchema: (node: ErdGraph["nodes"][number]) => void;
  onOpenData: (node: ErdGraph["nodes"][number]) => void;
}

type ErdFlowNode = Node<TableNodeData, "tableNode">;
type ErdFlowEdge = Edge<{
  fromColumn: string;
  toColumn: string;
  constraintName: string;
  cardinality: ErdGraph["edges"][number]["cardinality"];
  sourceNullable: boolean;
  renderCardinality: boolean;
}>;

const CARD_WIDTH = 320;
const HEADER_HEIGHT = 60;
const COLUMN_ROW_HEIGHT = 20;
const LOD_ZOOM_THRESHOLD_FULL = 0.3;
const reactFlowProOptions = { hideAttribution: true };
const reactFlowFitViewOptions = { padding: 0.2 };

type LODLevel = "full" | "placeholder";

const persistedState = (): ErdViewState => {
  return window.__vscode?.getState<ErdViewState>() ?? {};
};

const persistState = (state: ErdViewState): void => {
  window.__vscode?.setState<ErdViewState>(state);
};

function matchesSearch(
  node: ErdGraph["nodes"][number],
  search: string,
): boolean {
  if (!search) {
    return true;
  }

  const text = [
    node.database,
    node.schema,
    node.table,
    ...node.columns.map((column) => `${column.name} ${column.type}`),
  ]
    .join(" ")
    .toLowerCase();

  return text.includes(search);
}

function computeNodeHeight(node: ErdGraph["nodes"][number]): number {
  return HEADER_HEIGHT + 16 + node.columns.length * COLUMN_ROW_HEIGHT + 14;
}

function getLODLevel(zoom: number): LODLevel {
  if (zoom >= LOD_ZOOM_THRESHOLD_FULL) {
    return "full";
  }
  return "placeholder";
}

function getTextPlaceholderRatio(
  text: string,
  slotWidthPx: number,
  minRatio: number,
): number {
  // Approximate text width in this font and clamp to visible slot width.
  const approxCharWidthPx = 6.1;
  const visibleChars = Math.max(1, Math.floor(slotWidthPx / approxCharWidthPx));
  const renderedChars = Math.min(text.length, visibleChars);
  return Math.max(minRatio, Math.min(1, renderedChars / visibleChars));
}

function normalizeHandlePart(columnName: string): string {
  return encodeURIComponent(columnName.toLowerCase());
}

function handleId(side: "left" | "right", columnName: string): string {
  return `${side}-${normalizeHandlePart(columnName)}`;
}

function layoutGraph(
  nodes: ErdGraph["nodes"],
  edges: ErdGraph["edges"],
): {
  positions: Map<string, { x: number; y: number }>;
  heights: Map<string, number>;
} {
  const heights = new Map<string, number>();
  for (const node of nodes) {
    const height = computeNodeHeight(node);
    heights.set(node.id, height);
  }

  const byId = new Map(nodes.map((node) => [node.id, node]));
  const adjacency = new Map<string, Set<string>>();
  for (const node of nodes) {
    adjacency.set(node.id, new Set());
  }

  for (const edge of edges) {
    adjacency.get(edge.fromTableId)?.add(edge.toTableId);
    adjacency.get(edge.toTableId)?.add(edge.fromTableId);
  }

  const visited = new Set<string>();
  const components: string[][] = [];
  for (const node of nodes) {
    if (visited.has(node.id)) {
      continue;
    }

    const stack = [node.id];
    const component: string[] = [];
    visited.add(node.id);

    while (stack.length > 0) {
      const current = stack.pop();
      if (!current) {
        continue;
      }

      component.push(current);
      for (const neighbor of adjacency.get(current) ?? []) {
        if (visited.has(neighbor)) {
          continue;
        }
        visited.add(neighbor);
        stack.push(neighbor);
      }
    }

    components.push(component);
  }

  interface PackedComponent {
    positions: Map<string, { x: number; y: number }>;
    width: number;
    height: number;
    area: number;
  }

  const packedComponents: PackedComponent[] = components.map((componentIds) => {
    const graph = new dagre.graphlib.Graph();
    graph.setDefaultEdgeLabel(() => ({}));
    graph.setGraph({
      rankdir: "TB",
      nodesep: 36,
      ranksep: 52,
      marginx: 24,
      marginy: 24,
    });

    const componentIdSet = new Set(componentIds);
    for (const id of componentIds) {
      graph.setNode(id, {
        width: CARD_WIDTH,
        height: heights.get(id) ?? 220,
      });
    }

    for (const edge of edges) {
      if (
        componentIdSet.has(edge.fromTableId) &&
        componentIdSet.has(edge.toTableId)
      ) {
        graph.setEdge(edge.fromTableId, edge.toTableId);
      }
    }

    dagre.layout(graph);

    const positions = new Map<string, { x: number; y: number }>();
    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;

    for (const id of componentIds) {
      const node = graph.node(id);
      const width = CARD_WIDTH;
      const height = heights.get(id) ?? 220;
      if (!node) {
        const fallback = byId.get(id)?.position ?? { x: 0, y: 0 };
        positions.set(id, fallback);
        minX = Math.min(minX, fallback.x);
        minY = Math.min(minY, fallback.y);
        maxX = Math.max(maxX, fallback.x + width);
        maxY = Math.max(maxY, fallback.y + height);
        continue;
      }

      const x = node.x - width / 2;
      const y = node.y - height / 2;
      positions.set(id, { x, y });
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + width);
      maxY = Math.max(maxY, y + height);
    }

    const width = Math.max(1, maxX - minX);
    const height = Math.max(1, maxY - minY);
    const normalized = new Map<string, { x: number; y: number }>();
    for (const [id, pos] of positions) {
      normalized.set(id, {
        x: pos.x - minX,
        y: pos.y - minY,
      });
    }

    return {
      positions: normalized,
      width,
      height,
      area: width * height,
    };
  });

  packedComponents.sort((a, b) => b.area - a.area);

  const totalArea = packedComponents.reduce(
    (sum, component) => sum + component.area,
    0,
  );
  const targetRowWidth = Math.max(900, Math.sqrt(totalArea) * 1.2);
  const componentGapX = 56;
  const componentGapY = 56;

  const positions = new Map<string, { x: number; y: number }>();
  let cursorX = 36;
  let cursorY = 36;
  let rowHeight = 0;

  for (const component of packedComponents) {
    if (cursorX > 36 && cursorX + component.width > targetRowWidth) {
      cursorX = 36;
      cursorY += rowHeight + componentGapY;
      rowHeight = 0;
    }

    for (const [id, pos] of component.positions) {
      positions.set(id, {
        x: cursorX + pos.x,
        y: cursorY + pos.y,
      });
    }

    cursorX += component.width + componentGapX;
    rowHeight = Math.max(rowHeight, component.height);
  }

  return {
    positions,
    heights,
  };
}

const TableNode = React.memo(function TableNode({
  data,
  dragging,
}: NodeProps<ErdFlowNode>): React.JSX.Element {
  const node = data.node;
  const isFullLOD = data.lodLevel === "full";

  return (
    <section
      aria-label={`${node.schema}.${node.table}`}
      style={{
        width: CARD_WIDTH,
        border: "1px solid var(--vscode-panel-border)",
        borderRadius: 7,
        background: "var(--vscode-sideBar-background)",
        boxShadow: dragging
          ? "0 6px 16px color-mix(in srgb, var(--vscode-editor-foreground) 18%, transparent)"
          : "none",
        transition: "box-shadow 120ms ease",
        zIndex: dragging ? 12 : 1,
        cursor: dragging ? "grabbing" : "grab",
        opacity: data.isDimmed ? 0.4 : 1,
        overflow: "visible",
      }}
    >
      <div
        style={{
          padding: "8px 10px",
          borderBottom: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
          borderTopLeftRadius: 7,
          borderTopRightRadius: 7,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 8,
          }}
        >
          <div style={nodeHeaderLabelStyle}>
            <span
              title={`${node.schema}.${node.table}`}
              style={tableTitleStyle}
            >
              {node.schema}.{node.table}
            </span>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              flexShrink: 0,
            }}
          >
            <button
              aria-label={`Open schema ${node.schema}.${node.table}`}
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                data.onOpenSchema(node);
              }}
              style={miniButtonStyle}
            >
              Schema
            </button>
            <button
              aria-label={`Open data ${node.schema}.${node.table}`}
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                data.onOpenData(node);
              }}
              style={miniButtonStyle}
            >
              Data
            </button>
          </div>
        </div>
      </div>

      <div style={{ padding: "8px 10px", fontSize: 11 }}>
        {node.columns.map((column) => {
          const columnTypeLabel =
            column.nullable && !column.type.endsWith("?")
              ? `${column.type}?`
              : column.type;

          if (!isFullLOD) {
            const contentWidthPx = CARD_WIDTH - 20;
            const rowGapPx = 8;
            const nameSlotWidthPx = Math.max(
              1,
              contentWidthPx * 0.56 - rowGapPx / 2,
            );
            const typeSlotWidthPx = Math.max(
              1,
              contentWidthPx * 0.44 - rowGapPx / 2,
            );
            const nameRatio = getTextPlaceholderRatio(
              column.name,
              nameSlotWidthPx,
              0.18,
            );
            const typeRatio = getTextPlaceholderRatio(
              columnTypeLabel,
              typeSlotWidthPx,
              0.22,
            );
            return (
              <div
                key={`${node.id}:${column.name}`}
                style={{
                  position: "relative",
                  display: "flex",
                  gap: 8,
                  height: COLUMN_ROW_HEIGHT,
                  alignItems: "center",
                  minWidth: 0,
                }}
              >
                <span
                  aria-hidden
                  style={{
                    minWidth: 0,
                    flex: "1 1 56%",
                    display: "flex",
                    justifyContent: "flex-start",
                    alignItems: "center",
                  }}
                >
                  <div
                    style={{
                      height: 7,
                      width: `${nameRatio * 100}%`,
                      borderRadius: 3,
                      background:
                        "color-mix(in srgb, var(--vscode-descriptionForeground) 35%, transparent)",
                      flexShrink: 0,
                    }}
                  />
                </span>
                <span
                  aria-hidden
                  style={{
                    minWidth: 0,
                    flex: "0 1 44%",
                    display: "flex",
                    justifyContent: "flex-end",
                    alignItems: "center",
                  }}
                >
                  <div
                    style={{
                      height: 6,
                      width: `${typeRatio * 100}%`,
                      borderRadius: 3,
                      background:
                        "color-mix(in srgb, var(--vscode-descriptionForeground) 20%, transparent)",
                      flexShrink: 0,
                    }}
                  />
                </span>
              </div>
            );
          }

          return (
            <div
              key={`${node.id}:${column.name}`}
              style={{
                position: "relative",
                display: "flex",
                justifyContent: "flex-start",
                gap: 8,
                height: COLUMN_ROW_HEIGHT,
                alignItems: "center",
                minWidth: 0,
              }}
            >
              {isFullLOD ? (
                <>
                  <Handle
                    id={handleId("left", column.name)}
                    type="target"
                    position={Position.Left}
                    style={rowLeftHandleStyle}
                  />
                  <Handle
                    id={handleId("right", column.name)}
                    type="source"
                    position={Position.Right}
                    style={rowRightHandleStyle}
                  />
                </>
              ) : null}
              <span style={columnNameAndBadgesStyle}>
                <span
                  title={column.name}
                  style={{
                    ...columnNameTextStyle,
                    fontWeight: column.isPrimaryKey ? 700 : 400,
                  }}
                >
                  {column.name}
                </span>
                {isFullLOD && column.isPrimaryKey ? (
                  <span style={columnBadgeStyle}>PK</span>
                ) : null}
                {isFullLOD && column.isForeignKey ? (
                  <span style={columnBadgeStyle}>FK</span>
                ) : null}
              </span>
              {isFullLOD ? (
                <span title={columnTypeLabel} style={columnTypeTextStyle}>
                  {columnTypeLabel}
                </span>
              ) : null}
            </div>
          );
        })}
      </div>
    </section>
  );
});

TableNode.displayName = "TableNode";

const nodeTypes = {
  tableNode: TableNode,
};

type CardinalityEnd =
  | "one"
  | "many"
  | "oneOnly"
  | "zeroOrOne"
  | "oneOrMany"
  | "zeroOrMany";

function mapCardinalityEnds(
  cardinality: ErdGraph["edges"][number]["cardinality"] | undefined,
  sourceNullable: boolean | undefined,
): { start: CardinalityEnd; end: CardinalityEnd } {
  if (cardinality === "one-to-one") {
    return {
      start: sourceNullable ? "zeroOrOne" : "oneOnly",
      end: "zeroOrOne",
    };
  }
  if (cardinality === "many-to-one") {
    return {
      start: sourceNullable ? "zeroOrMany" : "oneOrMany",
      end: sourceNullable ? "zeroOrOne" : "oneOnly",
    };
  }
  return { start: "many", end: "one" };
}

interface CardinalityGlyphProps {
  x: number;
  y: number;
  angle: number;
  kind: CardinalityEnd;
  color: string;
  strokeWidth: number;
}

function CardinalityGlyph({
  x,
  y,
  angle,
  kind,
  color,
  strokeWidth,
}: CardinalityGlyphProps): React.JSX.Element {
  return (
    <g
      transform={`translate(${x} ${y}) rotate(${angle})`}
      style={{ pointerEvents: "none" }}
    >
      {kind === "one" ? (
        <path
          d="M -1 -7 L -1 7"
          stroke={color}
          strokeWidth={strokeWidth}
          fill="none"
        />
      ) : null}
      {kind === "many" ? (
        <>
          <path
            d="M 0 0 L -8 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M 0 0 L -8 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M 0 0 L -8 5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "oneOnly" ? (
        <>
          <path
            d="M -1 -7 L -1 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -6 -7 L -6 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "zeroOrOne" ? (
        <circle
          cx={-5}
          cy={0}
          r={3}
          stroke={color}
          strokeWidth={strokeWidth}
          fill="var(--vscode-editor-background)"
        />
      ) : null}

      {kind === "zeroOrOne" ? (
        <path
          d="M -1 -7 L -1 7"
          stroke={color}
          strokeWidth={strokeWidth}
          fill="none"
        />
      ) : null}

      {kind === "oneOrMany" ? (
        <>
          <path
            d="M -1 -7 L -1 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -5 0 L -13 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -5 0 L -13 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -5 0 L -13 5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "zeroOrMany" ? (
        <>
          <circle
            cx={-5}
            cy={0}
            r={3}
            stroke={color}
            strokeWidth={strokeWidth}
            fill="var(--vscode-editor-background)"
          />
          <path
            d="M -9 0 L -17 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -9 0 L -17 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -9 0 L -17 5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}
    </g>
  );
}

function outwardVector(position: Position): {
  x: number;
  y: number;
  angle: number;
} {
  if (position === Position.Right) {
    return { x: 1, y: 0, angle: 180 };
  }
  if (position === Position.Top) {
    return { x: 0, y: -1, angle: 90 };
  }
  if (position === Position.Bottom) {
    return { x: 0, y: 1, angle: -90 };
  }
  return { x: -1, y: 0, angle: 0 };
}

const RelationshipEdge = React.memo(function RelationshipEdge(
  props: EdgeProps<ErdFlowEdge>,
): React.JSX.Element {
  const {
    id,
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    style,
    selected,
    data,
  } = props;

  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 10,
  });

  const relationshipColor = selected
    ? "var(--vscode-focusBorder)"
    : "var(--vscode-descriptionForeground)";
  const strokeWidth = Number(style?.strokeWidth ?? (selected ? 2.4 : 1.6));
  const { start, end } = mapCardinalityEnds(
    data?.cardinality,
    data?.sourceNullable,
  );

  const sourceOut = outwardVector(sourcePosition);
  const targetOut = outwardVector(targetPosition);
  const markerOffset = 9;

  // Keep glyphs outside node borders so they don't get hidden under card backgrounds.
  const startGlyphX = sourceX + sourceOut.x * markerOffset;
  const startGlyphY = sourceY + sourceOut.y * markerOffset;
  const endGlyphX = targetX + targetOut.x * markerOffset;
  const endGlyphY = targetY + targetOut.y * markerOffset;

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={style} />
      {data?.renderCardinality ? (
        <>
          <CardinalityGlyph
            x={startGlyphX}
            y={startGlyphY}
            angle={sourceOut.angle}
            kind={start}
            color={relationshipColor}
            strokeWidth={strokeWidth + 0.2}
          />
          <CardinalityGlyph
            x={endGlyphX}
            y={endGlyphY}
            angle={targetOut.angle}
            kind={end}
            color={relationshipColor}
            strokeWidth={strokeWidth + 0.2}
          />
        </>
      ) : null}
    </>
  );
});

RelationshipEdge.displayName = "RelationshipEdge";

const edgeTypes = {
  erdRelationship: RelationshipEdge,
};

const ErdControls = React.memo(function ErdControls(): React.JSX.Element {
  const { fitView, zoomIn, zoomOut } = useReactFlow();

  const handleZoomIn = useCallback(() => {
    zoomIn({ duration: 220 });
  }, [zoomIn]);

  const handleZoomOut = useCallback(() => {
    zoomOut({ duration: 220 });
  }, [zoomOut]);

  const handleFitView = useCallback(() => {
    fitView({ ...reactFlowFitViewOptions, duration: 220 });
  }, [fitView]);

  return (
    <Controls
      position="top-right"
      showInteractive={false}
      showZoom={false}
      showFitView={false}
    >
      <ControlButton
        title="Zoom in"
        aria-label="Zoom in"
        onClick={handleZoomIn}
      >
        +
      </ControlButton>
      <ControlButton
        title="Zoom out"
        aria-label="Zoom out"
        onClick={handleZoomOut}
      >
        -
      </ControlButton>
      <ControlButton
        title="Fit view"
        aria-label="Fit view"
        onClick={handleFitView}
      >
        [ ]
      </ControlButton>
    </Controls>
  );
});

ErdControls.displayName = "ErdControls";

export function ErdView({
  connectionId: _connectionId,
}: Props): React.JSX.Element {
  const initialState = useMemo(() => persistedState(), []);
  const [graph, setGraph] = useState<ErdGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [hasCommittedGraph, setHasCommittedGraph] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fromCache, setFromCache] = useState(false);
  const [loadedAt, setLoadedAt] = useState<string | null>(null);
  const [search, setSearch] = useState(initialState.search ?? "");
  const [hideUnmatched, setHideUnmatched] = useState(
    initialState.hideUnmatched ?? false,
  );
  const [hideIsolated, setHideIsolated] = useState(
    initialState.hideIsolated ?? false,
  );
  const [manualPositions, setManualPositions] = useState<
    Record<string, { x: number; y: number }>
  >(initialState.nodePositions ?? {});
  const [reactFlowApi, setReactFlowApi] = useState<ReactFlowInstance<
    ErdFlowNode,
    ErdFlowEdge
  > | null>(null);
  const [flowNodes, setFlowNodes] = useState<ErdFlowNode[]>([]);
  const [lodLevel, setLodLevel] = useState<LODLevel>(() =>
    getLODLevel(initialState.viewport?.zoom ?? 1),
  );

  const hasRestoredViewportRef = useRef(false);
  const lastSyncedLoadedAtRef = useRef<string | null>(null);
  const viewportRef = useRef(initialState.viewport);
  const reactFlowApiRef = useRef<ReactFlowInstance<
    ErdFlowNode,
    ErdFlowEdge
  > | null>(null);
  const flowNodesRef = useRef<ErdFlowNode[]>([]);
  const manualPositionsRef = useRef(manualPositions);
  const searchRef = useRef(search);
  const hideUnmatchedRef = useRef(hideUnmatched);
  const hideIsolatedRef = useRef(hideIsolated);
  const lodLevelRef = useRef(lodLevel);

  const normalizedSearch = search.trim().toLowerCase();
  const shouldHideUnmatched = hideUnmatched && Boolean(normalizedSearch);

  useEffect(() => {
    flowNodesRef.current = flowNodes;
  }, [flowNodes]);

  useEffect(() => {
    manualPositionsRef.current = manualPositions;
  }, [manualPositions]);

  useEffect(() => {
    searchRef.current = search;
  }, [search]);

  useEffect(() => {
    hideUnmatchedRef.current = hideUnmatched;
  }, [hideUnmatched]);

  useEffect(() => {
    hideIsolatedRef.current = hideIsolated;
  }, [hideIsolated]);

  useEffect(() => {
    lodLevelRef.current = lodLevel;
  }, [lodLevel]);

  useEffect(() => {
    const unGraph = onMessage<ErdGraphMessage>("erdGraph", (payload) => {
      setGraph(payload.graph);
      setFromCache(payload.fromCache);
      setLoadedAt(payload.loadedAt);
      setLoading(false);
      setHasCommittedGraph(true);
      setError(null);
    });

    const unError = onMessage<{ error: string }>("erdError", (payload) => {
      setError(payload.error);
      setLoading(false);
    });

    const unLoading = onMessage<{ forceReload?: boolean }>("erdLoading", () => {
      setError(null);
      setLoading(true);
    });

    postMessage("ready");

    return () => {
      unGraph();
      unError();
      unLoading();
    };
  }, []);

  useEffect(() => {
    const nextState: ErdViewState = {
      search,
      hideUnmatched,
      hideIsolated,
      viewport: viewportRef.current,
      nodePositions: manualPositions,
    };
    persistState(nextState);
  }, [hideIsolated, hideUnmatched, manualPositions, search]);

  const connectedNodeIds = useMemo(() => {
    if (!graph) {
      return new Set<string>();
    }

    const nextConnectedNodeIds = new Set<string>();
    for (const edge of graph.edges) {
      nextConnectedNodeIds.add(edge.fromTableId);
      nextConnectedNodeIds.add(edge.toTableId);
    }

    return nextConnectedNodeIds;
  }, [graph]);

  const focusedNodeIds = useMemo(() => {
    if (!graph) {
      return new Set<string>();
    }

    const allNodeIds = new Set(graph.nodes.map((node) => node.id));
    const matchedNodeIds = new Set(
      graph.nodes
        .filter((node) => matchesSearch(node, normalizedSearch))
        .map((node) => node.id),
    );

    const nextFocusedNodeIds = normalizedSearch
      ? new Set(matchedNodeIds)
      : new Set(allNodeIds);

    if (normalizedSearch) {
      for (const edge of graph.edges) {
        if (
          matchedNodeIds.has(edge.fromTableId) ||
          matchedNodeIds.has(edge.toTableId)
        ) {
          nextFocusedNodeIds.add(edge.fromTableId);
          nextFocusedNodeIds.add(edge.toTableId);
        }
      }
    }

    return nextFocusedNodeIds;
  }, [graph, normalizedSearch]);

  const visibilityFilterNodeIds = shouldHideUnmatched ? focusedNodeIds : null;

  const visibleGraph = useMemo(() => {
    if (!graph) {
      return {
        nodes: [] as ErdGraph["nodes"],
        edges: [] as ErdGraph["edges"],
      };
    }

    const visibleNodes = graph.nodes.filter((node) => {
      if (hideIsolated && !connectedNodeIds.has(node.id)) {
        return false;
      }
      if (visibilityFilterNodeIds && !visibilityFilterNodeIds.has(node.id)) {
        return false;
      }
      return true;
    });

    const visibleIds = new Set(visibleNodes.map((node) => node.id));
    const visibleEdges = graph.edges.filter((edge) => {
      return visibleIds.has(edge.fromTableId) && visibleIds.has(edge.toTableId);
    });

    return {
      nodes: visibleNodes,
      edges: visibleEdges,
    };
  }, [connectedNodeIds, graph, hideIsolated, visibilityFilterNodeIds]);

  const layout = useMemo(() => {
    return layoutGraph(visibleGraph.nodes, visibleGraph.edges);
  }, [visibleGraph.edges, visibleGraph.nodes]);

  const openSchema = useCallback((node: ErdGraph["nodes"][number]) => {
    postMessage("openSchema", {
      database: node.database,
      schema: node.schema,
      table: node.table,
    });
  }, []);

  const openData = useCallback((node: ErdGraph["nodes"][number]) => {
    postMessage("openTableData", {
      database: node.database,
      schema: node.schema,
      table: node.table,
      isView: node.isView,
    });
  }, []);

  const preparedFlowNodes = useMemo<ErdFlowNode[]>(() => {
    return visibleGraph.nodes.map((node) => {
      const isDimmed =
        Boolean(normalizedSearch) && !focusedNodeIds.has(node.id);
      const manualPosition = manualPositions[node.id];

      return {
        id: node.id,
        type: "tableNode",
        position:
          manualPosition ?? layout.positions.get(node.id) ?? node.position,
        data: {
          node,
          isDimmed,
          lodLevel,
          onOpenSchema: openSchema,
          onOpenData: openData,
        },
      };
    });
  }, [
    focusedNodeIds,
    layout.positions,
    manualPositions,
    normalizedSearch,
    openData,
    lodLevel,
    openSchema,
    visibleGraph.nodes,
  ]);

  useEffect(() => {
    setFlowNodes(preparedFlowNodes);
  }, [preparedFlowNodes]);

  const flowEdges = useMemo<ErdFlowEdge[]>(() => {
    return visibleGraph.edges.map((edge) => {
      const isFocused =
        !normalizedSearch ||
        (focusedNodeIds.has(edge.fromTableId) &&
          focusedNodeIds.has(edge.toTableId));

      return {
        id: edge.id,
        source: edge.fromTableId,
        target: edge.toTableId,
        sourceHandle: handleId("right", edge.fromColumn),
        targetHandle: handleId("left", edge.toColumn),
        type: "erdRelationship",
        style: {
          stroke: "var(--vscode-descriptionForeground)",
          strokeWidth: 1.6,
          opacity: isFocused ? 1 : 0.25,
        },
        zIndex: 5,
        data: {
          fromColumn: edge.fromColumn,
          toColumn: edge.toColumn,
          constraintName: edge.constraintName,
          cardinality: edge.cardinality,
          sourceNullable: edge.sourceNullable,
          renderCardinality: lodLevel !== "placeholder",
        },
      };
    });
  }, [focusedNodeIds, lodLevel, normalizedSearch, visibleGraph.edges]);

  useEffect(() => {
    if (!loadedAt || !reactFlowApi || loading || flowNodes.length === 0) {
      return;
    }

    if (!hasRestoredViewportRef.current && viewportRef.current) {
      reactFlowApi.setViewport(viewportRef.current, { duration: 0 });
      hasRestoredViewportRef.current = true;
      return;
    }

    reactFlowApi.fitView({ padding: 0.2, duration: 220 });
    hasRestoredViewportRef.current = true;
  }, [flowNodes.length, loadedAt, loading, reactFlowApi]);

  useEffect(() => {
    if (!loadedAt || lastSyncedLoadedAtRef.current === loadedAt) {
      return;
    }

    lastSyncedLoadedAtRef.current = loadedAt;
    setManualPositions((previousPositions) => {
      const nextPositions: Record<string, { x: number; y: number }> = {};
      for (const node of visibleGraph.nodes) {
        const existing = previousPositions[node.id];
        nextPositions[node.id] =
          existing ?? layout.positions.get(node.id) ?? node.position;
      }
      return nextPositions;
    });
  }, [layout.positions, loadedAt, visibleGraph.nodes]);

  const handleReload = useCallback(() => {
    postMessage("reload");
  }, []);

  const handleNodesChange = useCallback(
    (changes: NodeChange<ErdFlowNode>[]) => {
      setFlowNodes((nodes) => applyNodeChanges(changes, nodes));
    },
    [],
  );

  const handleInit = useCallback(
    (instance: ReactFlowInstance<ErdFlowNode, ErdFlowEdge>) => {
      reactFlowApiRef.current = instance;
      setReactFlowApi(instance);
    },
    [],
  );

  const handleNodeDragStop = useCallback(
    (_event: React.MouseEvent | MouseEvent, node: ErdFlowNode) => {
      setManualPositions((prev) => ({
        ...prev,
        [node.id]: {
          x: node.position.x,
          y: node.position.y,
        },
      }));
    },
    [],
  );

  const handleMoveEnd = useCallback(() => {
    const instance = reactFlowApiRef.current;
    if (!instance) {
      return;
    }

    const viewport = instance.getViewport();
    viewportRef.current = viewport;
    const nodePositions = flowNodesRef.current.reduce<
      Record<string, { x: number; y: number }>
    >(
      (acc, item) => {
        acc[item.id] = {
          x: item.position.x,
          y: item.position.y,
        };
        return acc;
      },
      {
        ...manualPositionsRef.current,
      },
    );
    persistState({
      search: searchRef.current,
      hideUnmatched: hideUnmatchedRef.current,
      hideIsolated: hideIsolatedRef.current,
      nodePositions,
      viewport,
    });
  }, []);

  const handleMove = useCallback(
    (
      _event: MouseEvent | TouchEvent | null,
      viewport: { x: number; y: number; zoom: number },
    ) => {
      const nextLodLevel = getLODLevel(viewport.zoom);
      if (nextLodLevel !== lodLevelRef.current) {
        lodLevelRef.current = nextLodLevel;
        setLodLevel(nextLodLevel);
      }
    },
    [],
  );

  if (error && !hasCommittedGraph) {
    return (
      <div
        style={{
          margin: 12,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
        }}
      >
        <strong>Error:</strong> {error}
      </div>
    );
  }

  const showRefetchOverlay = loading && hasCommittedGraph;

  if (!hasCommittedGraph) {
    return (
      <main
        aria-label="Entity relationship graph"
        aria-busy="true"
        style={{
          display: "flex",
          flexDirection: "column",
          height: "100vh",
          overflow: "hidden",
          position: "relative",
        }}
      >
        <GridLoadingOverlay mode="fullscreen" message="Loading data..." />
      </main>
    );
  }

  return (
    <main
      aria-label="Entity relationship graph"
      aria-busy={showRefetchOverlay}
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        position: "relative",
      }}
    >
      <header
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "8px 12px",
          borderBottom: "1px solid var(--vscode-panel-border)",
          gap: 12,
        }}
      >
        <div style={{ flexShrink: 0 }}>
          <div style={{ fontSize: 12, fontWeight: 600 }}>
            Showing {visibleGraph.nodes.length} of {graph?.nodes.length ?? 0}{" "}
            tables
          </div>
          <div style={{ fontSize: 11, opacity: 0.65, marginTop: 2 }}>
            Relationships: {visibleGraph.edges.length}
            {graph
              ? `, Isolated: ${graph.nodes.length - connectedNodeIds.size}`
              : ""}
          </div>
        </div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            flexWrap: "wrap",
            justifyContent: "flex-end",
          }}
        >
          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ fontSize: 11, opacity: 0.75 }}>Search</span>
            <input
              aria-label="Search tables and columns"
              value={search}
              onChange={(event) => {
                setSearch(event.target.value);
              }}
              placeholder="table, schema, column"
              style={{
                minWidth: 220,
                padding: "4px 8px",
                borderRadius: 4,
                border:
                  "1px solid var(--vscode-input-border, var(--vscode-panel-border))",
                background: "var(--vscode-input-background)",
                color: "var(--vscode-input-foreground)",
              }}
            />
          </label>

          <label style={toggleLabelStyle}>
            <input
              type="checkbox"
              checked={hideUnmatched}
              onChange={(event) => {
                setHideUnmatched(event.target.checked);
              }}
              disabled={!normalizedSearch}
            />
            <span>Hide non-focus</span>
          </label>

          <label style={toggleLabelStyle}>
            <input
              type="checkbox"
              checked={hideIsolated}
              onChange={(event) => {
                setHideIsolated(event.target.checked);
              }}
            />
            <span>Hide isolated</span>
          </label>

          {fromCache ? (
            <span style={{ fontSize: 11, opacity: 0.7 }}>cached</span>
          ) : null}
          <button
            type="button"
            onClick={handleReload}
            style={{
              cursor: "pointer",
              fontSize: 12,
              padding: "4px 10px",
              border: "1px solid var(--vscode-button-border, transparent)",
              borderRadius: 4,
              background: "var(--vscode-button-background)",
              color: "var(--vscode-button-foreground)",
            }}
          >
            Reload
          </button>
        </div>
      </header>

      {showRefetchOverlay ? (
        <GridLoadingOverlay
          mode="overlay"
          message="Loading data..."
          trapFocus
        />
      ) : null}

      {error ? (
        <div
          style={{
            margin: 12,
            padding: "10px 12px",
            background: "var(--vscode-inputValidation-errorBackground)",
            border: "1px solid var(--vscode-inputValidation-errorBorder)",
            color: "var(--vscode-errorForeground)",
            borderRadius: 4,
          }}
        >
          {error}
        </div>
      ) : null}

      <div style={{ flex: 1, overflow: "auto", position: "relative" }}>
        {!graph ? (
          <div style={{ padding: 14, fontSize: 12, opacity: 0.75 }}>
            No graph data.
          </div>
        ) : null}

        {graph && graph.nodes.length === 0 ? (
          <div style={{ padding: 14, fontSize: 12, opacity: 0.75 }}>
            No tables found for the selected scope.
          </div>
        ) : null}

        {graph && graph.nodes.length > 0 ? (
          <div style={{ width: "100%", height: "100%" }}>
            <ReactFlow
              aria-label="Entity relationship graph"
              nodes={flowNodes}
              edges={flowEdges}
              nodeTypes={nodeTypes}
              edgeTypes={edgeTypes}
              proOptions={reactFlowProOptions}
              nodesDraggable
              nodesConnectable={false}
              onlyRenderVisibleElements
              panOnScroll
              zoomOnScroll={false}
              zoomOnPinch
              elementsSelectable={false}
              onNodesChange={handleNodesChange}
              onInit={handleInit}
              onNodeDragStop={handleNodeDragStop}
              onMove={handleMove}
              onMoveEnd={handleMoveEnd}
              fitView
              fitViewOptions={reactFlowFitViewOptions}
              minZoom={0.1}
              maxZoom={2}
            >
              <style>{reactFlowControlsThemeCss}</style>
              <ErdControls />
              {lodLevel !== "placeholder" ? (
                <Background
                  gap={lodLevel === "full" ? 18 : 34}
                  size={lodLevel === "full" ? 1 : 0.7}
                  color="var(--vscode-panel-border)"
                />
              ) : null}
            </ReactFlow>
          </div>
        ) : null}
      </div>
    </main>
  );
}

const miniButtonStyle: React.CSSProperties = {
  cursor: "pointer",
  fontSize: 10,
  padding: "3px 8px",
  border:
    "1px solid var(--vscode-button-secondaryBorder, var(--vscode-panel-border))",
  borderRadius: 4,
  color: "var(--vscode-button-secondaryForeground)",
  background: "var(--vscode-button-secondaryBackground)",
};

const nodeHeaderLabelStyle: React.CSSProperties = {
  flex: 1,
  display: "flex",
  alignItems: "center",
  minWidth: 0,
  overflow: "hidden",
};

const tableTitleStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 600,
  textAlign: "left",
  minWidth: 0,
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

const toggleLabelStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  fontSize: 11,
  opacity: 0.85,
};

const columnBadgeStyle: React.CSSProperties = {
  fontSize: 9,
  padding: "1px 4px",
  borderRadius: 3,
  border: "1px solid var(--vscode-panel-border)",
  background: "var(--vscode-editor-background)",
  opacity: 0.9,
};

const columnNameAndBadgesStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  minWidth: 0,
  flex: "1 1 56%",
};

const columnNameTextStyle: React.CSSProperties = {
  minWidth: 0,
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

const columnTypeTextStyle: React.CSSProperties = {
  opacity: 0.7,
  minWidth: 0,
  flex: "0 1 44%",
  textAlign: "right",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
};

const rowHandleStyle: React.CSSProperties = {
  width: 8,
  height: 8,
  opacity: 0,
  border: 0,
  background: "transparent",
  top: "50%",
  transform: "translateY(-50%)",
};

const rowLeftHandleStyle: React.CSSProperties = {
  ...rowHandleStyle,
  left: 0,
};

const rowRightHandleStyle: React.CSSProperties = {
  ...rowHandleStyle,
  right: 0,
};

const reactFlowControlsThemeCss = `
.react-flow__controls {
  border: 1px solid var(--vscode-panel-border);
  border-radius: 4px;
  overflow: hidden;
  box-shadow: none;
}

.react-flow__controls-button {
  background: var(--vscode-button-secondaryBackground);
  color: var(--vscode-button-secondaryForeground);
  border-bottom: 1px solid var(--vscode-panel-border);
}

.react-flow__controls-button:last-child {
  border-bottom: 0;
}

.react-flow__controls-button:hover {
  background: var(--vscode-list-hoverBackground);
}

.react-flow__controls-button svg,
.react-flow__controls-button path {
  fill: currentColor;
  stroke: currentColor;
}
`;
