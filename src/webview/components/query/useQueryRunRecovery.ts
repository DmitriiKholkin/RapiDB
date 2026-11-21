import { useCallback, useEffect, useRef, useState } from "react";
import {
  isRecord,
  parseQueryRunIdentity,
  parseQueryRunStatusReply,
  type QueryRunIdentity,
  type QueryRunOutcome,
  sameQueryRun,
} from "../../../shared/webviewContracts";
import { useQueryStore } from "../../store";
import { onMessage, postMessage } from "../../utils/messaging";
import { readWebviewState, updateWebviewState } from "../../utils/vscodeState";

const STATUS_POLL_MS = 2_000;
const STATUS_TIMEOUT_MS = 10_000;

interface RunMarker {
  run: QueryRunIdentity;
  phase: "pending" | "unknown";
}

function readMarker(panelId: string): RunMarker | null {
  const saved = readWebviewState<Record<string, unknown>>({}).queryRun;
  if (!isRecord(saved)) return null;
  const run = parseQueryRunIdentity(saved.run);
  if (run?.panelId !== panelId) return null;
  return { run, phase: saved.phase === "pending" ? "pending" : "unknown" };
}

function writeMarker(marker: RunMarker | null): void {
  if (!window.__vscode) throw new Error("Webview state is unavailable");
  updateWebviewState((state) => {
    const { queryRun: _previous, ...rest } = state;
    return marker ? { ...rest, queryRun: marker } : rest;
  });
}

export function useQueryRunRecovery(panelId: string) {
  const [marker, setMarker] = useState(() => readMarker(panelId));
  const markerRef = useRef(marker);
  const restoredRun = useRef(marker?.run);
  const [notice, setNotice] = useState<string | null>(null);
  const pendingProbe = useRef<string | null>(null);
  const lastReply = useRef(0);

  const applyMarker = useCallback((next: RunMarker | null) => {
    try {
      writeMarker(next);
    } catch {
      // An uncleared durable marker is conservative on the next reload.
    }
    pendingProbe.current = null;
    markerRef.current = next;
    setMarker(next);
  }, []);

  const markUnknown = useCallback(() => {
    const current = markerRef.current;
    if (current?.phase !== "pending") return;
    const { status, reset } = useQueryStore.getState();
    if (status === "running") reset();
    applyMarker({ ...current, phase: "unknown" });
  }, [applyMarker]);

  const settle = useCallback(
    (outcome: QueryRunOutcome) => {
      if (outcome === "unknown") markUnknown();
      else applyMarker(null);
    },
    [applyMarker, markUnknown],
  );

  const prepareRun = useCallback((run: QueryRunIdentity): boolean => {
    if (markerRef.current) return false;
    const next: RunMarker = { run, phase: "pending" };
    try {
      // Persist before executeQuery is posted. Never send if this fails.
      if (!parseQueryRunIdentity(run))
        throw new Error("Invalid query identity");
      writeMarker(next);
    } catch {
      useQueryStore
        .getState()
        .setError(
          "Could not save the query run state. The query was not sent.",
        );
      return false;
    }
    setNotice(null);
    markerRef.current = next;
    setMarker(next);
    return true;
  }, []);

  const acknowledgeUnknown = useCallback(() => {
    if (markerRef.current?.phase === "unknown") applyMarker(null);
  }, [applyMarker]);

  const pendingRun = marker?.phase === "pending" ? marker.run : null;
  useEffect(() => {
    if (!pendingRun || markerRef.current?.phase !== "pending") return;
    const probe = () => {
      const current = markerRef.current;
      if (current?.phase !== "pending" || current.run !== pendingRun) return;
      if (Date.now() - lastReply.current >= STATUS_TIMEOUT_MS) {
        markUnknown();
        return;
      }
      // A probe can be repeated; only its first matching reply is consumed.
      pendingProbe.current ??= crypto.randomUUID();
      try {
        postMessage("getQueryRunStatus", {
          run: pendingRun,
          requestId: pendingProbe.current,
        });
      } catch {
        // Missing transport/ACK is covered by the same deadline.
      }
    };
    const unsubscribe = onMessage<unknown>("queryRunStatus", (input) => {
      const current = markerRef.current;
      const reply = parseQueryRunStatusReply(input);
      if (
        !reply ||
        current?.phase !== "pending" ||
        reply.requestId !== pendingProbe.current ||
        !sameQueryRun(reply.run, current.run)
      )
        return;
      pendingProbe.current = null;
      lastReply.current = Date.now();
      if (reply.status === "accepted" || reply.status === "running") return;
      if (reply.status !== "unknown") {
        useQueryStore.getState().reset();
        setNotice(
          reply.status === "completed"
            ? "The previous query completed, but its result was not received. Results are not replayed."
            : "The previous query was not sent to the database. No query was retried.",
        );
      }
      settle(reply.status);
    });
    lastReply.current = Date.now();
    const interval = window.setInterval(probe, STATUS_POLL_MS);
    if (pendingRun === restoredRun.current) probe();
    return () => {
      window.clearInterval(interval);
      pendingProbe.current = null;
      unsubscribe();
    };
  }, [markUnknown, pendingRun, settle]);

  return {
    marker,
    markerRef,
    notice,
    prepareRun,
    settle,
    markUnknown,
    acknowledgeUnknown,
  };
}
