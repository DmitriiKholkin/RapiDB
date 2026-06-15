/**
 * ERD graph layout algorithm.
 *
 * Uses dagre for hierarchical layout within connected components,
 * then packs components into a grid to minimize canvas usage.
 */
import dagre from "@dagrejs/dagre";
import type { ErdGraph } from "../../../shared/webviewContracts";

// ─── Constants ──────────────────────────────────────────────────────────────

export const CARD_WIDTH = 320;

/** Height of the table-card header (title bar). */
const HEADER_HEIGHT = 60;

/** Per-column row height inside a table card. */
const COLUMN_ROW_HEIGHT = 20;

/** Padding inside a card above/below the column list. */
const CARD_VERTICAL_PADDING = 30; // 16 (top) + 14 (bottom)

/** dagre intra-component spacing. */
const DAGRE_NODESEP = 36;
const DAGRE_RANKSEP = 52;
const DAGRE_MARGIN = 24;

/** Fallback node height when a height is missing from the map. */
const FALLBACK_NODE_HEIGHT = 220;

/** Component grid packing parameters. */
const MIN_TARGET_ROW_WIDTH = 900;
const ROW_WIDTH_GROWTH_FACTOR = 1.2;
const COMPONENT_GAP_X = 56;
const COMPONENT_GAP_Y = 56;
const PACK_ORIGIN_X = 36;
const PACK_ORIGIN_Y = 36;

// ─── Node Height Calculation ────────────────────────────────────────────────

export function computeNodeHeight(node: ErdGraph["nodes"][number]): number {
  return (
    HEADER_HEIGHT +
    CARD_VERTICAL_PADDING +
    node.columns.length * COLUMN_ROW_HEIGHT
  );
}

// ─── Connected Components ───────────────────────────────────────────────────

function findConnectedComponents(
  nodes: ErdGraph["nodes"],
  edges: ErdGraph["edges"],
): string[][] {
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
    if (visited.has(node.id)) continue;

    const stack = [node.id];
    const component: string[] = [];
    visited.add(node.id);

    while (stack.length > 0) {
      const current = stack.pop();
      if (!current) continue;

      component.push(current);
      for (const neighbor of adjacency.get(current) ?? []) {
        if (visited.has(neighbor)) continue;
        visited.add(neighbor);
        stack.push(neighbor);
      }
    }

    components.push(component);
  }

  return components;
}

// ─── Single Component Layout ────────────────────────────────────────────────

interface PackedComponent {
  positions: Map<string, { x: number; y: number }>;
  width: number;
  height: number;
  area: number;
}

function layoutSingleComponent(
  componentIds: string[],
  edges: ErdGraph["edges"],
  heights: Map<string, number>,
  byId: Map<string, ErdGraph["nodes"][number]>,
): PackedComponent {
  const graph = new dagre.graphlib.Graph();
  graph.setDefaultEdgeLabel(() => ({}));
  graph.setGraph({
    rankdir: "TB",
    nodesep: DAGRE_NODESEP,
    ranksep: DAGRE_RANKSEP,
    marginx: DAGRE_MARGIN,
    marginy: DAGRE_MARGIN,
  });

  const componentIdSet = new Set(componentIds);
  for (const id of componentIds) {
    graph.setNode(id, {
      width: CARD_WIDTH,
      height: heights.get(id) ?? FALLBACK_NODE_HEIGHT,
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
    const height = heights.get(id) ?? FALLBACK_NODE_HEIGHT;

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

  // Normalize to (0,0) origin
  const width = Math.max(1, maxX - minX);
  const height = Math.max(1, maxY - minY);
  const normalized = new Map<string, { x: number; y: number }>();
  for (const [id, pos] of positions) {
    normalized.set(id, { x: pos.x - minX, y: pos.y - minY });
  }

  return { positions: normalized, width, height, area: width * height };
}

// ─── Component Packing ──────────────────────────────────────────────────────

function packComponents(
  packedComponents: PackedComponent[],
): Map<string, { x: number; y: number }> {
  packedComponents.sort((a, b) => b.area - a.area);

  const totalArea = packedComponents.reduce((sum, c) => sum + c.area, 0);
  const targetRowWidth = Math.max(
    MIN_TARGET_ROW_WIDTH,
    Math.sqrt(totalArea) * ROW_WIDTH_GROWTH_FACTOR,
  );

  const positions = new Map<string, { x: number; y: number }>();
  let cursorX = PACK_ORIGIN_X;
  let cursorY = PACK_ORIGIN_Y;
  let rowHeight = 0;

  for (const component of packedComponents) {
    if (cursorX > PACK_ORIGIN_X && cursorX + component.width > targetRowWidth) {
      cursorX = PACK_ORIGIN_X;
      cursorY += rowHeight + COMPONENT_GAP_Y;
      rowHeight = 0;
    }

    for (const [id, pos] of component.positions) {
      positions.set(id, { x: cursorX + pos.x, y: cursorY + pos.y });
    }

    cursorX += component.width + COMPONENT_GAP_X;
    rowHeight = Math.max(rowHeight, component.height);
  }

  return positions;
}

// ─── Main Layout Function ───────────────────────────────────────────────────

export function layoutGraph(
  nodes: ErdGraph["nodes"],
  edges: ErdGraph["edges"],
): {
  positions: Map<string, { x: number; y: number }>;
  heights: Map<string, number>;
} {
  const heights = new Map<string, number>();
  for (const node of nodes) {
    heights.set(node.id, computeNodeHeight(node));
  }

  const byId = new Map(nodes.map((node) => [node.id, node]));
  const components = findConnectedComponents(nodes, edges);

  const packedComponents = components.map((componentIds) =>
    layoutSingleComponent(componentIds, edges, heights, byId),
  );

  const positions = packComponents(packedComponents);

  return { positions, heights };
}
