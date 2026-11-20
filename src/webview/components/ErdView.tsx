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
  useReactFlow,
  type Viewport,
} from "@xyflow/react";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import "../xyflow.css";
import { formatPrimaryKeyBadgeLabel } from "../../shared/tableTypes";
import { type ErdGraph, isRecord } from "../../shared/webviewContracts";
import { formatErdDiscoveryNotice } from "../../shared/webviewContracts/erd";
import { onMessage, postMessage } from "../utils/messaging";
import { isArrayOf, isString } from "../utils/replyShape";
import { readWebviewState, updateWebviewState } from "../utils/vscodeState";
import { GridLoadingOverlay } from "./GridOverlay";

interface ErdGraphMessage {
  graph: ErdGraph;
  fromCache: boolean;
  loadedAt: string;
}

const INVALID_ERD_REPLY =
  "The host returned an invalid ERD reply. Reload to try again.";

function isErdGraphMessage(
  payload: Record<string, unknown>,
): payload is Record<string, unknown> & ErdGraphMessage {
  const { graph } = payload;
  return (
    typeof payload.fromCache === "boolean" &&
    typeof payload.loadedAt === "string" &&
    isRecord(graph) &&
    isRecord(graph.scope) &&
    isArrayOf(graph.nodes, isRecord) &&
    graph.nodes.every(
      (node) =>
        typeof node.id === "string" &&
        typeof node.database === "string" &&
        typeof node.schema === "string" &&
        typeof node.table === "string" &&
        isRecord(node.position) &&
        Number.isFinite(node.position.x) &&
        Number.isFinite(node.position.y) &&
        isArrayOf(node.columns, isRecord) &&
        node.columns.every(
          (column) =>
            typeof column.name === "string" && typeof column.type === "string",
        ),
    ) &&
    isArrayOf(graph.edges, isRecord) &&
    graph.edges.every(
      (edge) =>
        typeof edge.id === "string" &&
        typeof edge.fromTableId === "string" &&
        typeof edge.toTableId === "string" &&
        typeof edge.fromColumn === "string" &&
        typeof edge.toColumn === "string",
    ) &&
    (graph.omittedTables === undefined ||
      isArrayOf(graph.omittedTables, isRecord)) &&
    (graph.discoveryTruncated === undefined ||
      graph.discoveryTruncated === "redis-prefixes" ||
      graph.discoveryTruncated === "objects") &&
    (graph.externalReferences === undefined ||
      (isArrayOf(graph.externalReferences, isRecord) &&
        graph.externalReferences.every(
          (reference) =>
            isArrayOf(reference.fromColumns, isString) &&
            isArrayOf(reference.columns, isString),
        )))
  );
}

interface Props {
  connectionId: string;
  database?: string;
  schema?: string;
}

type NodePosition = ErdGraph["nodes"][number]["position"];
type NodePositions = Record<string, NodePosition>;

interface LayoutOverrides {
  pinnedPositions: NodePositions;
  legacyPositions: NodePositions;
}

interface ErdViewState {
  search?: string;
  hideUnmatched?: boolean;
  hideIsolated?: boolean;
  viewport?: Viewport;
}

interface ErdWebviewState extends ErdViewState {
  initialState?: Props & { view: string };
  nodePositions?: NodePositions;
  erdView?: ErdViewState & LayoutOverrides & { scopeKey: string };
}

interface TableNodeData extends Record<string, unknown> {
  node: ErdGraph["nodes"][number];
  isDimmed: boolean;
  lodLevel: LODLevel;
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
const PIN_CLEARANCE = 16;
const LOD_ZOOM_THRESHOLD_FULL = 0.3;
const reactFlowProOptions = { hideAttribution: true };
const reactFlowFitViewOptions = { padding: 0.2 };

type LODLevel = "full" | "placeholder";

function erdScopeKey({ connectionId, database, schema }: Props): string {
  return JSON.stringify([
    connectionId.trim(),
    database || null,
    schema || null,
  ]);
}

function persistedState(scopeKey: string): ErdViewState & LayoutOverrides {
  const state = readWebviewState<ErdWebviewState>({});
  const empty = { pinnedPositions: {}, legacyPositions: {} };
  if (state.erdView) {
    return state.erdView.scopeKey === scopeKey
      ? {
          ...state.erdView,
          pinnedPositions: state.erdView.pinnedPositions ?? {},
          legacyPositions: state.erdView.legacyPositions ?? {},
        }
      : empty;
  }
  if (
    state.initialState &&
    (state.initialState.view !== "erd" ||
      typeof state.initialState.connectionId !== "string" ||
      erdScopeKey(state.initialState) !== scopeKey)
  ) {
    return empty;
  }

  // Older panel snapshots stored automatic and user positions together.
  // Keep them as separate compatibility anchors: even a point matching Dagre
  // could be intentional, and a different point might be from an older graph.
  const { search, hideUnmatched, hideIsolated, viewport, nodePositions } =
    state;
  return {
    search,
    hideUnmatched,
    hideIsolated,
    viewport,
    pinnedPositions: {},
    legacyPositions: nodePositions ?? {},
  };
}

function isNodePosition(position: unknown): position is NodePosition {
  return (
    isRecord(position) &&
    Number.isFinite(position.x) &&
    Number.isFinite(position.y)
  );
}

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
  const approxCharWidthPx = 6.1;
  const visibleChars = Math.max(1, Math.floor(slotWidthPx / approxCharWidthPx));
  const renderedChars = Math.min(text.length, visibleChars);
  return Math.max(minRatio, Math.min(1, renderedChars / visibleChars));
}

function normalizeHandlePart(columnName: string): string {
  return encodeURIComponent(columnName);
}

function handleId(side: "left" | "right", columnName: string): string {
  return `${side}-${normalizeHandlePart(columnName)}`;
}

const MAX_NOTICE_ITEMS = 5;

function formatNoticeList<T>(
  items: readonly T[],
  format: (item: T) => string,
): string {
  const shown = items.slice(0, MAX_NOTICE_ITEMS).map(format).join(", ");
  return items.length > MAX_NOTICE_ITEMS
    ? `${shown}, and ${items.length - MAX_NOTICE_ITEMS} more`
    : shown;
}

function formatQualifiedName(...parts: string[]): string {
  return parts.filter(Boolean).join(".");
}

function formatColumnReference(table: string, columns: string[]): string {
  return columns.length === 1
    ? formatQualifiedName(table, columns[0])
    : `${table}(${columns.join(", ")})`;
}

function buildGraphNotices(graph: ErdGraph | null): string[] {
  if (!graph) {
    return [];
  }

  const notices: string[] = [];
  if (graph.discoveryTruncated) {
    notices.push(formatErdDiscoveryNotice(graph.discoveryTruncated));
  }
  const omittedTables = graph.omittedTables ?? [];
  if (omittedTables.length > 0) {
    notices.push(
      `Could not load ${omittedTables.length} table(s), so they are not shown: ${formatNoticeList(
        omittedTables,
        (table) =>
          `${formatQualifiedName(table.schema, table.table)} (${table.error})`,
      )}`,
    );
  }

  const externalReferences = graph.externalReferences ?? [];
  if (externalReferences.length > 0) {
    const tablesById = new Map(graph.nodes.map((node) => [node.id, node]));
    const qualifySource = !graph.scope.schema;
    notices.push(
      `${externalReferences.length} relationship(s) point to tables outside this diagram: ${formatNoticeList(
        externalReferences,
        (reference) => {
          const source = tablesById.get(reference.fromTableId);
          const sourceName = source
            ? qualifySource
              ? formatQualifiedName(source.schema, source.table)
              : source.table
            : "";
          return `${formatColumnReference(
            sourceName,
            reference.fromColumns,
          )} → ${formatColumnReference(
            formatQualifiedName(reference.schema, reference.table),
            reference.columns,
          )}`;
        },
      )}`,
    );
  }

  return notices;
}

function retainPositions(
  nodes: ErdGraph["nodes"],
  saved: NodePositions,
): NodePositions {
  return Object.fromEntries(
    nodes.flatMap(({ id }) => {
      const position = saved[id];
      return isNodePosition(position) ? [[id, position]] : [];
    }),
  );
}

interface NodeRect extends NodePosition {
  height: number;
}

function hasHorizontalConflict(a: number, b: number): boolean {
  return Math.abs(a - b) < CARD_WIDTH + PIN_CLEARANCE;
}

function reflowAroundAnchors(
  positions: Map<string, NodePosition>,
  heights: Map<string, number>,
  anchors: NodePositions,
): void {
  const anchoredRects = Object.entries(anchors).map(([id, position]) => ({
    ...position,
    height: heights.get(id) ?? 220,
  }));
  if (anchoredRects.length === 0) return;

  const occupied: NodeRect[] = [...anchoredRects];
  const displaced: Array<[string, NodeRect]> = [];
  for (const [id, preferred] of positions) {
    if (Object.hasOwn(anchors, id)) {
      positions.set(id, anchors[id]);
      continue;
    }
    const rect = { ...preferred, height: heights.get(id) ?? 220 };
    const blocked = anchoredRects.some(
      (anchor) =>
        hasHorizontalConflict(rect.x, anchor.x) &&
        rect.y + rect.height + PIN_CLEARANCE > anchor.y &&
        anchor.y + anchor.height + PIN_CLEARANCE > rect.y,
    );
    if (blocked) {
      displaced.push([id, rect]);
    } else {
      occupied.push(rect);
    }
  }
  if (displaced.length === 0) return;

  // Leave unaffected Dagre slots alone. For a blocked slot, scan down its
  // column to the first free position, reserving that slot before the next one.
  occupied.sort((a, b) => a.y - b.y);
  for (const [id, { x, y: preferredY, height }] of displaced) {
    let y = preferredY;
    for (const rect of occupied) {
      if (y + height + PIN_CLEARANCE <= rect.y) break;
      if (hasHorizontalConflict(x, rect.x)) {
        y = Math.max(y, rect.y + rect.height + PIN_CLEARANCE);
      }
    }
    const insertion = occupied.findIndex((rect) => rect.y > y);
    occupied.splice(insertion === -1 ? occupied.length : insertion, 0, {
      x,
      y,
      height,
    });
    positions.set(id, { x, y });
  }
}

function layoutGraph(
  nodes: ErdGraph["nodes"],
  edges: ErdGraph["edges"],
  anchors: NodePositions,
): Map<string, NodePosition> {
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

  reflowAroundAnchors(positions, heights, anchors);
  return positions;
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
                  <span style={primaryKeyBadgeStyle(column.primaryKeyRole)}>
                    {formatPrimaryKeyBadgeLabel(column.primaryKeyRole)}
                  </span>
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
      start: "zeroOrOne",
      end: sourceNullable ? "zeroOrOne" : "oneOnly",
    };
  }
  if (cardinality === "many-to-one") {
    return {
      start: "zeroOrMany",
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
          d="M -6 -7 L -6 7"
          stroke={color}
          strokeWidth={strokeWidth}
          fill="none"
        />
      ) : null}
      {kind === "many" ? (
        <>
          <path
            d="M -12 0 L 0 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "oneOnly" ? (
        <>
          <path
            d="M -6 -7 L -6 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -14 -7 L -14 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "zeroOrOne" ? (
        <circle
          cx={-16}
          cy={0}
          r={5}
          stroke={color}
          strokeWidth={strokeWidth}
          fill="var(--vscode-editor-background)"
        />
      ) : null}

      {kind === "zeroOrOne" ? (
        <path
          d="M -6 -7 L -6 7"
          stroke={color}
          strokeWidth={strokeWidth}
          fill="none"
        />
      ) : null}

      {kind === "oneOrMany" ? (
        <>
          <path
            d="M -14 -7 L -14 7"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
        </>
      ) : null}

      {kind === "zeroOrMany" ? (
        <>
          <circle
            cx={-16}
            cy={0}
            r={5}
            stroke={color}
            strokeWidth={strokeWidth}
            fill="var(--vscode-editor-background)"
          />
          <path
            d="M -12 0 L 0 -5"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 0"
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
          />
          <path
            d="M -12 0 L 0 5"
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
    return { x: 0, y: 0, angle: 180 };
  }
  if (position === Position.Top) {
    return { x: 0, y: 0, angle: 90 };
  }
  if (position === Position.Bottom) {
    return { x: 0, y: 0, angle: -90 };
  }
  return { x: 0, y: 0, angle: 0 };
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

export function ErdView(props: Props): React.JSX.Element {
  const scopeKey = erdScopeKey(props);
  return <ScopedErdView key={scopeKey} scopeKey={scopeKey} />;
}

function ScopedErdView({ scopeKey }: { scopeKey: string }): React.JSX.Element {
  const [initialState] = useState(() => persistedState(scopeKey));
  const [diagram, setDiagram] = useState<{
    graph: ErdGraph;
    positions: Map<string, NodePosition>;
  } | null>(null);
  const graph = diagram?.graph ?? null;
  const layoutPositions = diagram?.positions;
  const hasCommittedGraph = diagram !== null;
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fromCache, setFromCache] = useState(false);
  const [search, setSearch] = useState(initialState.search ?? "");
  const [hideUnmatched, setHideUnmatched] = useState(
    initialState.hideUnmatched ?? false,
  );
  const [hideIsolated, setHideIsolated] = useState(
    initialState.hideIsolated ?? false,
  );
  const [flowNodes, setFlowNodes] = useState<ErdFlowNode[]>([]);
  const [lodLevel, setLodLevel] = useState<LODLevel>(() =>
    getLODLevel(initialState.viewport?.zoom ?? 1),
  );

  const latestLoadIdRef = useRef(0);
  const activeLoadIdRef = useRef<number | null>(null);
  const viewportRef = useRef(initialState.viewport);
  const flowNodesRef = useRef<ErdFlowNode[]>([]);
  const overridesRef = useRef<LayoutOverrides>({
    pinnedPositions: initialState.pinnedPositions,
    legacyPositions: initialState.legacyPositions,
  });
  const filterStateRef = useRef({ search, hideUnmatched, hideIsolated });
  const lodLevelRef = useRef(lodLevel);

  const normalizedSearch = search.trim().toLowerCase();
  const shouldHideUnmatched = hideUnmatched && Boolean(normalizedSearch);

  const saveViewState = useCallback(() => {
    updateWebviewState((current) => {
      const next: Record<string, unknown> = {
        ...current,
        erdView: {
          scopeKey,
          ...filterStateRef.current,
          ...overridesRef.current,
          viewport: viewportRef.current,
        },
      };
      delete next.nodePositions;
      return next;
    });
  }, [scopeKey]);

  useEffect(() => {
    filterStateRef.current = { search, hideUnmatched, hideIsolated };
    saveViewState();
  }, [hideIsolated, hideUnmatched, saveViewState, search]);

  useEffect(() => {
    lodLevelRef.current = lodLevel;
  }, [lodLevel]);

  useEffect(() => {
    const isCurrentReply = (
      payload: unknown,
    ): payload is Record<string, unknown> =>
      isRecord(payload) &&
      activeLoadIdRef.current !== null &&
      payload.loadId === activeLoadIdRef.current;
    const finishError = (message: string) => {
      activeLoadIdRef.current = null;
      setError(message);
      setLoading(false);
    };

    const unGraph = onMessage<unknown>("erdGraph", (payload) => {
      if (!isCurrentReply(payload)) return;
      if (!isErdGraphMessage(payload)) {
        finishError(INVALID_ERD_REPLY);
        return;
      }
      const { graph: nextGraph } = payload;
      const { pinnedPositions, legacyPositions } = overridesRef.current;
      const nextOverrides = {
        pinnedPositions: retainPositions(nextGraph.nodes, pinnedPositions),
        legacyPositions: retainPositions(nextGraph.nodes, legacyPositions),
      };
      let positions: Map<string, NodePosition>;
      try {
        positions = layoutGraph(nextGraph.nodes, nextGraph.edges, {
          ...nextOverrides.legacyPositions,
          ...nextOverrides.pinnedPositions,
        });
      } catch {
        finishError(INVALID_ERD_REPLY);
        return;
      }
      activeLoadIdRef.current = null;
      flowNodesRef.current = flowNodesRef.current.filter((node) =>
        positions.has(node.id),
      );
      overridesRef.current = nextOverrides;
      saveViewState();
      setDiagram({ graph: nextGraph, positions });
      setFromCache(payload.fromCache);
      setLoading(false);
      setError(null);
    });

    const unError = onMessage<unknown>("erdError", (payload) => {
      if (!isCurrentReply(payload)) return;
      finishError(
        typeof payload.error === "string" && payload.error.trim()
          ? payload.error
          : INVALID_ERD_REPLY,
      );
    });

    const unLoading = onMessage<unknown>("erdLoading", (payload) => {
      if (
        !isRecord(payload) ||
        typeof payload.loadId !== "number" ||
        !Number.isSafeInteger(payload.loadId) ||
        payload.loadId <= latestLoadIdRef.current
      )
        return;
      latestLoadIdRef.current = payload.loadId;
      activeLoadIdRef.current = payload.loadId;
      setError(null);
      setLoading(true);
    });

    postMessage("ready");

    return () => {
      unGraph();
      unError();
      unLoading();
    };
  }, [saveViewState]);

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

  const graphNotices = useMemo(() => buildGraphNotices(graph), [graph]);

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
      return {
        id: node.id,
        type: "tableNode",
        position: layoutPositions?.get(node.id) ?? node.position,
        data: {
          node,
          isDimmed,
          lodLevel,
          onOpenData: openData,
        },
      };
    });
  }, [
    focusedNodeIds,
    layoutPositions,
    normalizedSearch,
    openData,
    lodLevel,
    visibleGraph.nodes,
  ]);

  useEffect(() => {
    const draggingIds = new Set(
      flowNodesRef.current
        .filter((node) => node.dragging)
        .map((node) => node.id),
    );
    const { pinnedPositions } = overridesRef.current;
    const nodes = preparedFlowNodes.map((node) => ({
      ...node,
      dragging: draggingIds.has(node.id),
      // A user move may have landed between the graph reply and this effect.
      position: Object.hasOwn(pinnedPositions, node.id)
        ? pinnedPositions[node.id]
        : node.position,
    }));
    flowNodesRef.current = nodes;
    setFlowNodes(nodes);
  }, [preparedFlowNodes]);

  const flowEdges = useMemo<ErdFlowEdge[]>(() => {
    return visibleGraph.edges.map((edge) => {
      const isFocused =
        !normalizedSearch ||
        (focusedNodeIds.has(edge.fromTableId) &&
          focusedNodeIds.has(edge.toTableId));
      const isSelfRef = edge.fromTableId === edge.toTableId;

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
        zIndex: isSelfRef ? -1 : 5,
        data: {
          fromColumn: edge.fromColumn,
          toColumn: edge.toColumn,
          constraintName: edge.constraintName,
          cardinality: edge.cardinality,
          sourceNullable: edge.sourceNullable,
          renderCardinality: true,
        },
      };
    });
  }, [focusedNodeIds, normalizedSearch, visibleGraph.edges]);

  const handleReload = useCallback(() => {
    postMessage("reload");
  }, []);

  const handleNodesChange = useCallback(
    (changes: NodeChange<ErdFlowNode>[]) => {
      const previous = flowNodesRef.current;
      const nodes = applyNodeChanges(changes, previous);
      flowNodesRef.current = nodes;
      setFlowNodes(nodes);

      // In this read-only, flat flow, position changes come from user movement.
      // Measurement/selection changes and viewport changes must not pin cards.
      const moved = new Map<string, NodePosition>();
      let finishedMove = false;
      const { pinnedPositions, legacyPositions } = overridesRef.current;
      for (const change of changes) {
        if (change.type !== "position") continue;
        const oldPosition = previous.find(
          (node) => node.id === change.id,
        )?.position;
        const position = change.position;
        if (!oldPosition || !isNodePosition(position)) continue;
        const didMove =
          position.x !== oldPosition.x || position.y !== oldPosition.y;
        if (didMove) {
          moved.set(change.id, { ...position });
        }
        if (
          change.dragging !== true &&
          (didMove || Object.hasOwn(pinnedPositions, change.id))
        ) {
          finishedMove = true;
        }
      }
      if (moved.size > 0) {
        const nextLegacy = { ...legacyPositions };
        for (const id of moved.keys()) delete nextLegacy[id];
        overridesRef.current = {
          pinnedPositions: { ...pinnedPositions, ...Object.fromEntries(moved) },
          legacyPositions: nextLegacy,
        };
      }
      if (finishedMove) saveViewState();
    },
    [saveViewState],
  );

  const handleNodeDragStop = useCallback(
    (_event: React.MouseEvent | MouseEvent, node: ErdFlowNode) => {
      handleNodesChange([
        {
          type: "position",
          id: node.id,
          position: node.position,
          dragging: false,
        },
      ]);
    },
    [handleNodesChange],
  );

  const handleMoveEnd = useCallback(
    (_event: MouseEvent | TouchEvent | null, viewport: Viewport) => {
      viewportRef.current = viewport;
      saveViewState();
    },
    [saveViewState],
  );

  const handleMove = useCallback(
    (_event: MouseEvent | TouchEvent | null, viewport: Viewport) => {
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
        role="alert"
        style={{
          margin: 12,
          padding: "10px 14px",
          borderRadius: 3,
          fontSize: 13,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
        }}
      >
        <span>
          <strong>Error:</strong> {error}
        </span>
        <button
          type="button"
          onClick={handleReload}
          style={{
            flexShrink: 0,
            cursor: "pointer",
            fontSize: 12,
            padding: "4px 10px",
            border: "1px solid var(--vscode-button-border, transparent)",
            borderRadius: 4,
            background: "var(--vscode-button-background)",
            color: "var(--vscode-button-foreground)",
          }}
        >
          Retry
        </button>
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

      {graphNotices.length > 0 ? (
        <div
          role="status"
          aria-label="Diagram warnings"
          style={{
            margin: "8px 12px 0",
            padding: "8px 12px",
            fontSize: 12,
            background: "var(--vscode-inputValidation-warningBackground)",
            border: "1px solid var(--vscode-inputValidation-warningBorder)",
            borderRadius: 4,
            display: "flex",
            flexDirection: "column",
            gap: 4,
          }}
        >
          {graphNotices.map((notice) => (
            <div key={notice}>{notice}</div>
          ))}
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
              onNodeDragStop={handleNodeDragStop}
              onMove={handleMove}
              onMoveEnd={handleMoveEnd}
              defaultViewport={viewportRef.current}
              // Only fit a fresh canvas. A reload or filter change should not
              // move the user's camera while the scene is laid out again.
              fitView={!viewportRef.current}
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

function primaryKeyBadgeStyle(
  role?: "partition" | "sort",
): React.CSSProperties {
  const isSortKey = role === "sort";
  return {
    ...columnBadgeStyle,
    color: isSortKey
      ? "var(--vscode-textLink-foreground, #2f6f9f)"
      : "var(--vscode-editorWarning-foreground, #8f5b00)",
    background: isSortKey
      ? "rgba(47, 111, 159, 0.16)"
      : "rgba(143, 91, 0, 0.16)",
  };
}

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
  left: -11,
};

const rowRightHandleStyle: React.CSSProperties = {
  ...rowHandleStyle,
  right: -11,
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
