/**
 * ERD view state management hook.
 *
 * Encapsulates all state variables, viewport persistence, and the
 * message handlers for the ERD panel.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ErdGraph } from "../../../shared/webviewContracts";
import { onMessage, postMessage } from "../../utils/messaging";
import { readWebviewState, writeWebviewState } from "../../utils/vscodeState";

// ─── Persisted State ────────────────────────────────────────────────────────

export interface ErdViewState {
  search?: string;
  hideUnmatched?: boolean;
  hideIsolated?: boolean;
  viewport?: { x: number; y: number; zoom: number };
  nodePositions?: Record<string, { x: number; y: number }>;
}

// ─── LOD Level ──────────────────────────────────────────────────────────────

export type LODLevel = "full" | "placeholder";

const LOD_ZOOM_THRESHOLD_FULL = 0.3;

export function getLODLevel(zoom: number): LODLevel {
  return zoom >= LOD_ZOOM_THRESHOLD_FULL ? "full" : "placeholder";
}

// ─── Message Types ──────────────────────────────────────────────────────────

interface ErdGraphMessage {
  graph: ErdGraph;
  fromCache: boolean;
  loadedAt: string;
}

// ─── Hook ───────────────────────────────────────────────────────────────────

export interface UseErdViewStateResult {
  // Graph data
  graph: ErdGraph | null;
  loading: boolean;
  hasCommittedGraph: boolean;
  error: string | null;
  fromCache: boolean;
  loadedAt: string | null;

  // Search & filter
  search: string;
  setSearch: (v: string) => void;
  hideUnmatched: boolean;
  setHideUnmatched: (v: boolean) => void;
  hideIsolated: boolean;
  setHideIsolated: (v: boolean) => void;

  // Layout
  lodLevel: LODLevel;
  setLodLevel: (level: LODLevel) => void;
  manualPositions: Record<string, { x: number; y: number }>;
  setManualPositions: React.Dispatch<
    React.SetStateAction<Record<string, { x: number; y: number }>>
  >;
  flowNodes: import("@xyflow/react").Node[];
  setFlowNodes: React.Dispatch<
    React.SetStateAction<import("@xyflow/react").Node[]>
  >;

  // Viewport
  viewportRef: React.MutableRefObject<
    { x: number; y: number; zoom: number } | undefined
  >;
  hasRestoredViewportRef: React.MutableRefObject<boolean>;
  lastSyncedLoadedAtRef: React.MutableRefObject<string | null>;

  // Actions
  handleReload: () => void;
}

export function useErdViewState(): UseErdViewStateResult {
  const initialState = useMemo(() => readWebviewState<ErdViewState>({}), []);

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
  const [manualPositions, setManualPositions] = useState(
    initialState.nodePositions ?? {},
  );
  const [lodLevel, setLodLevel] = useState<LODLevel>(() =>
    getLODLevel(initialState.viewport?.zoom ?? 1),
  );
  const [flowNodes, setFlowNodes] = useState<import("@xyflow/react").Node[]>(
    [],
  );

  const viewportRef = useRef(initialState.viewport);
  const hasRestoredViewportRef = useRef(false);
  const lastSyncedLoadedAtRef = useRef<string | null>(null);

  // Subscribe to ERD messages
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

  // Persist state on changes
  useEffect(() => {
    const nextState: ErdViewState = {
      search,
      hideUnmatched,
      hideIsolated,
      viewport: viewportRef.current,
      nodePositions: manualPositions,
    };
    writeWebviewState(nextState);
  }, [hideIsolated, hideUnmatched, manualPositions, search]);

  const handleReload = useCallback(() => {
    postMessage("reload");
  }, []);

  return {
    graph,
    loading,
    hasCommittedGraph,
    error,
    fromCache,
    loadedAt,
    search,
    setSearch,
    hideUnmatched,
    setHideUnmatched,
    hideIsolated,
    setHideIsolated,
    lodLevel,
    setLodLevel,
    manualPositions,
    setManualPositions,
    flowNodes,
    setFlowNodes,
    viewportRef,
    hasRestoredViewportRef,
    lastSyncedLoadedAtRef,
    handleReload,
  };
}
