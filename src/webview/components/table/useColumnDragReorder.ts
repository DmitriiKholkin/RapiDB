import { useCallback, useEffect, useRef } from "react";

export interface UseColumnDragReorderOptions {
  getColumnOrder: () => string[];
  setColumnOrder: (updater: (previous: string[]) => string[]) => void;
  excludedIds?: readonly string[];
  dragActivationDistance?: number;
  onDragActivated?: () => void;
  onDragEnded?: (activated: boolean) => void;
}

export interface UseColumnDragReorderResult {
  onHeaderMouseDown: (columnId: string, event: React.MouseEvent) => void;
  isDragging: () => boolean;
  cancelDrag: () => void;
}

interface DragState {
  columnId: string;
  startX: number;
  activated: boolean;
  lastSwappedNeighbor: string | null;
  lastSwapDirection: "forward" | "backward" | null;
  draggedTh: HTMLElement;
  ghost: HTMLElement | null;
}

interface LiveOrderAccessors {
  getOrder: () => string[];
  setOrder: (updater: (previous: string[]) => string[]) => void;
}

export function useColumnDragReorder(
  options: UseColumnDragReorderOptions,
): UseColumnDragReorderResult {
  const {
    getColumnOrder,
    setColumnOrder,
    excludedIds,
    dragActivationDistance = 4,
    onDragActivated,
    onDragEnded,
  } = options;

  const getColumnOrderRef = useRef(getColumnOrder);
  const setColumnOrderRef = useRef(setColumnOrder);
  const excludedRef = useRef<Set<string>>(new Set(excludedIds ?? []));

  useEffect(() => {
    getColumnOrderRef.current = getColumnOrder;
  }, [getColumnOrder]);

  useEffect(() => {
    setColumnOrderRef.current = setColumnOrder;
  }, [setColumnOrder]);

  useEffect(() => {
    excludedRef.current = new Set(excludedIds ?? []);
  }, [excludedIds]);

  const onDragActivatedRef = useRef(onDragActivated);
  useEffect(() => {
    onDragActivatedRef.current = onDragActivated;
  }, [onDragActivated]);

  const onDragEndedRef = useRef(onDragEnded);
  useEffect(() => {
    onDragEndedRef.current = onDragEnded;
  }, [onDragEnded]);

  const dragStateRef = useRef<DragState | null>(null);
  const isDraggingRef = useRef(false);
  const gestureCleanupRef = useRef<(() => void) | null>(null);
  const cancelDrag = useCallback(() => {
    gestureCleanupRef.current?.();
  }, []);

  useEffect(() => cancelDrag, [cancelDrag]);

  const isDragging = useCallback(() => isDraggingRef.current, []);

  const onHeaderMouseDown = useCallback(
    (columnId: string, event: React.MouseEvent) => {
      if (event.button !== 0) return;
      if (excludedRef.current.has(columnId)) return;

      const draggedTh = (event.currentTarget as HTMLElement) ?? null;
      if (!draggedTh) return;

      const order = getColumnOrderRef.current();
      if (!order.includes(columnId)) return;

      cancelDrag();
      dragStateRef.current = {
        columnId,
        startX: event.clientX,
        activated: false,
        lastSwappedNeighbor: null,
        lastSwapDirection: null,
        draggedTh,
        ghost: null,
      };
      const previousCursor = document.body.style.cursor;
      const previousUserSelect = document.body.style.userSelect;
      const previousOpacity = draggedTh.style.opacity;
      const previousDraggingAttribute = draggedTh.getAttribute(
        "data-column-dragging",
      );

      const accessors: LiveOrderAccessors = {
        getOrder: () => getColumnOrderRef.current(),
        setOrder: (updater) => setColumnOrderRef.current(updater),
      };

      const onMouseMove = (moveEvent: MouseEvent) => {
        const state = dragStateRef.current;
        if (!state) return;

        const deltaX = moveEvent.clientX - state.startX;
        const absDelta = Math.abs(deltaX);

        if (!state.activated) {
          if (absDelta <= dragActivationDistance) return;
          state.activated = true;
          isDraggingRef.current = true;
          onDragActivatedRef.current?.();
          if (dragStateRef.current !== state) return;
          state.draggedTh.setAttribute("data-column-dragging", "true");
          document.body.style.cursor = "grabbing";
          document.body.style.userSelect = "none";
          const rect = state.draggedTh.getBoundingClientRect();
          const ghost = document.createElement("div");
          ghost.className = "rapidb-column-drag-ghost";
          ghost.innerHTML = state.draggedTh.innerHTML;
          ghost.style.left = `${moveEvent.clientX}px`;
          ghost.style.top = `${rect.top}px`;
          ghost.style.width = `${rect.width}px`;
          document.body.appendChild(ghost);
          state.ghost = ghost;
          state.draggedTh.style.opacity = "0.3";
        }

        updateGhost(state, moveEvent.clientX);
        attemptSwap(state, moveEvent.clientX, moveEvent.clientY, accessors);
      };

      const cleanup = () => {
        document.removeEventListener("mousemove", onMouseMove);
        document.removeEventListener("mouseup", onMouseUp);
        const state = dragStateRef.current;
        if (state?.activated) {
          state.draggedTh.style.opacity = previousOpacity;
          if (previousDraggingAttribute === null) {
            state.draggedTh.removeAttribute("data-column-dragging");
          } else {
            state.draggedTh.setAttribute(
              "data-column-dragging",
              previousDraggingAttribute,
            );
          }
          state.ghost?.remove();
          document.body.style.cursor = previousCursor;
          document.body.style.userSelect = previousUserSelect;
        }
        isDraggingRef.current = false;
        dragStateRef.current = null;
        gestureCleanupRef.current = null;
      };
      const onMouseUp = () => {
        const wasActivated = dragStateRef.current?.activated ?? false;
        cleanup();
        onDragEndedRef.current?.(wasActivated);
      };

      gestureCleanupRef.current = cleanup;
      document.addEventListener("mousemove", onMouseMove);
      document.addEventListener("mouseup", onMouseUp);
    },
    [dragActivationDistance, cancelDrag],
  );

  return { onHeaderMouseDown, isDragging, cancelDrag };
}

function updateGhost(state: DragState, clientX: number): void {
  const ghost = state.ghost;
  if (!ghost) return;
  const rect = state.draggedTh.getBoundingClientRect();
  ghost.style.left = `${clientX}px`;
  ghost.style.top = `${rect.top}px`;
}

function attemptSwap(
  state: DragState,
  clientX: number,
  clientY: number,
  accessors: LiveOrderAccessors,
): void {
  const currentOrder = accessors.getOrder();
  const draggedIndex = currentOrder.indexOf(state.columnId);
  if (draggedIndex === -1) return;

  const target = findNeighborUnderCursor(
    state.columnId,
    clientX,
    clientY,
    accessors,
    state.ghost,
  );
  if (!target) return;

  const targetIndex = currentOrder.indexOf(target);
  if (targetIndex === -1 || targetIndex === draggedIndex) return;

  const direction: "forward" | "backward" =
    draggedIndex < targetIndex ? "forward" : "backward";

  const targetTh = document.querySelector(
    `th[data-column-id="${cssEscape(target)}"]`,
  ) as HTMLElement | null;
  if (!targetTh) return;
  const rect = targetTh.getBoundingClientRect();
  const center = rect.left + rect.width / 2;
  const cursorPastCenter =
    direction === "forward" ? clientX >= center : clientX <= center;

  if (!cursorPastCenter) {
    return;
  }

  if (
    state.lastSwappedNeighbor === target &&
    state.lastSwapDirection === direction
  ) {
    return;
  }

  const newOrder = moveColumn(currentOrder, draggedIndex, targetIndex);
  if (newOrder === currentOrder) return;

  accessors.setOrder(() => newOrder);
  state.lastSwappedNeighbor = target;
  state.lastSwapDirection = direction;
}

function moveColumn(order: string[], from: number, to: number): string[] {
  if (from === to) return order;
  if (from < 0 || to < 0 || from >= order.length || to >= order.length) {
    return order;
  }
  const next = order.slice();
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  return next;
}

function findNeighborUnderCursor(
  excludeId: string,
  clientX: number,
  clientY: number,
  accessors: LiveOrderAccessors,
  ghost: HTMLElement | null,
): string | null {
  if (typeof document.elementFromPoint === "function") {
    if (ghost) ghost.style.pointerEvents = "none";
    const elementUnder = document.elementFromPoint(
      clientX,
      clientY,
    ) as HTMLElement | null;
    if (ghost) ghost.style.pointerEvents = "";

    if (elementUnder) {
      const th = elementUnder.closest("th[data-column-id]");
      if (th) {
        const id = th.getAttribute("data-column-id");
        if (id && id !== excludeId) return id;
      }
      const tdWithId = elementUnder.closest("td[data-column-id]");
      if (tdWithId) {
        const id = tdWithId.getAttribute("data-column-id");
        if (id && id !== excludeId) return id;
      }
      const td = elementUnder.closest("td[data-col]");
      if (td) {
        const colIndex = Number.parseInt(td.getAttribute("data-col") ?? "", 10);
        if (!Number.isNaN(colIndex)) {
          const order = accessors.getOrder();
          const id = order[colIndex] ?? null;
          if (id && id !== excludeId) return id;
        }
      }
    }
  }

  const allThs = document.querySelectorAll("th[data-column-id]");
  for (const th of Array.from(allThs)) {
    if ((th as HTMLElement).style.cursor === "col-resize") continue;
    const rect = th.getBoundingClientRect();
    if (
      clientX >= rect.left &&
      clientX <= rect.right &&
      clientY >= rect.top &&
      clientY <= rect.bottom
    ) {
      const id = th.getAttribute("data-column-id");
      if (id && id !== excludeId) return id;
      break;
    }
  }
  return null;
}

function cssEscape(value: string): string {
  if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
    return CSS.escape(value);
  }
  return value.replace(/([^a-zA-Z0-9_-])/g, "\\$1");
}
