import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useColumnDragReorder } from "../../src/webview/components/table/useColumnDragReorder";

interface MountedGrid {
  pressDown: (columnId: string, clientX: number) => void;
  moveTo: (clientX: number) => void;
  release: (clientX: number) => void;
  getOrder: () => string[];
  getSetOrderCallCount: () => number;
  getActivationCount: () => number;
  getEndCount: () => number;
  cleanup: () => void;
}

const latestOrderRef: { current: string[] } = { current: [] };
const latestSetOrderCallsRef: { current: string[][] } = { current: [] };
const latestActivationCountRef: { current: number } = { current: 0 };
const latestEndCountRef: { current: number } = { current: 0 };

function mountGrid(
  initialOrder: string[],
  options: {
    excludedIds?: readonly string[];
    widths?: Record<string, number>;
  } = {},
): MountedGrid {
  const widths = options.widths ?? {};
  const onDragActivated = vi.fn();
  const onDragEnded = vi.fn();
  const orderRef: { current: string[] } = { current: initialOrder.slice() };
  latestOrderRef.current = initialOrder.slice();
  latestSetOrderCallsRef.current = [];
  latestActivationCountRef.current = 0;
  latestEndCountRef.current = 0;

  const tree = (
    <TestHarness
      initialOrder={initialOrder}
      widths={widths}
      excludedIds={options.excludedIds}
      onDragActivated={() => {
        latestActivationCountRef.current += 1;
        onDragActivated();
      }}
      onDragEnded={() => {
        latestEndCountRef.current += 1;
        onDragEnded();
      }}
      orderRef={orderRef}
      setOrderCalls={latestSetOrderCallsRef.current}
    />
  );
  const utils = render(tree);

  const table = screen.getByRole("table");

  return {
    pressDown(columnId, clientX) {
      const th = table.querySelector(
        `th[data-column-id="${columnId}"]`,
      ) as HTMLElement;
      fireEvent.mouseDown(th, { clientX, clientY: 14, buttons: 1 });
    },
    moveTo(clientX) {
      fireEvent.mouseMove(document, { clientX, clientY: 14, buttons: 1 });
    },
    release(clientX) {
      fireEvent.mouseUp(document, { clientX, clientY: 14, buttons: 0 });
    },
    getOrder() {
      return latestOrderRef.current.slice();
    },
    getSetOrderCallCount() {
      return latestSetOrderCallsRef.current.length;
    },
    getActivationCount() {
      return latestActivationCountRef.current;
    },
    getEndCount() {
      return latestEndCountRef.current;
    },
    cleanup() {
      utils.unmount();
    },
  };
}

interface HarnessProps {
  initialOrder: string[];
  widths: Record<string, number>;
  excludedIds?: readonly string[];
  onDragActivated: () => void;
  onDragEnded: () => void;
  orderRef: { current: string[] };
  setOrderCalls: string[][];
}

function TestHarness({
  initialOrder,
  widths,
  excludedIds,
  onDragActivated,
  onDragEnded,
  orderRef,
  setOrderCalls,
}: HarnessProps) {
  const { onHeaderMouseDown } = useColumnDragReorder({
    getColumnOrder: () => orderRef.current,
    setColumnOrder: (updater) => {
      const previous = orderRef.current;
      const next = updater(previous);
      if (next !== previous) {
        orderRef.current = next;
        latestOrderRef.current = next.slice();
        setOrderCalls.push(next.slice());
      }
    },
    excludedIds,
    onDragActivated,
    onDragEnded,
  });

  return (
    <table>
      <thead>
        <tr>
          {initialOrder.map((id) => {
            return (
              <th
                key={id}
                data-column-id={id}
                style={{ width: widths[id] ?? 100, display: "table-cell" }}
                onMouseDown={(event) => onHeaderMouseDown(id, event)}
              >
                {id}
              </th>
            );
          })}
        </tr>
      </thead>
    </table>
  );
}

describe("useColumnDragReorder", () => {
  let originalGetBoundingClientRect: typeof HTMLElement.prototype.getBoundingClientRect;
  let originalElementFromPoint: typeof document.elementFromPoint;

  beforeEach(() => {
    originalGetBoundingClientRect = HTMLElement.prototype.getBoundingClientRect;
    originalElementFromPoint = document.elementFromPoint;
  });

  afterEach(() => {
    HTMLElement.prototype.getBoundingClientRect = originalGetBoundingClientRect;
    document.elementFromPoint = originalElementFromPoint;
    document.querySelectorAll(".rapidb-column-drag-ghost").forEach((node) => {
      node.remove();
    });
  });

  function installRectMock(widths: Record<string, number>) {
    HTMLElement.prototype.getBoundingClientRect = vi.fn(function (
      this: HTMLElement,
    ) {
      const id = this.getAttribute("data-column-id") ?? "";
      const table = document.querySelector("table");
      if (!table) {
        return {
          x: 0,
          y: 0,
          width: 0,
          height: 28,
          left: 0,
          top: 0,
          right: 0,
          bottom: 28,
          toJSON: () => ({}),
        };
      }
      const order = Array.from(
        table.querySelectorAll("thead tr:first-child th"),
      ).map((th) => th.getAttribute("data-column-id") ?? "");
      const indexInOrder = order.indexOf(id);
      if (indexInOrder === -1) {
        return {
          x: 0,
          y: 0,
          width: 0,
          height: 28,
          left: 0,
          top: 0,
          right: 0,
          bottom: 28,
          toJSON: () => ({}),
        };
      }
      let cursor = 0;
      for (let i = 0; i < indexInOrder; i++) {
        cursor += widths[order[i]] ?? 100;
      }
      const w = widths[id] ?? 100;
      return {
        x: cursor,
        y: 0,
        width: w,
        height: 28,
        left: cursor,
        top: 0,
        right: cursor + w,
        bottom: 28,
        toJSON: () => ({}),
      };
    });
  }

  it("does not swap while the cursor stays on the original side of the neighbor's center", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      rig.moveTo(60);
      rig.moveTo(100);
      rig.moveTo(130);
      expect(rig.getOrder()).toEqual(["a", "b", "c"]);
      expect(rig.getSetOrderCallCount()).toBe(0);
    } finally {
      rig.cleanup();
    }
  });

  it("swaps exactly once when the cursor crosses the center of a wider neighbor (no flicker)", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      for (let x = 150; x < 240; x += 5) {
        rig.moveTo(x);
      }
      expect(rig.getSetOrderCallCount()).toBe(1);
      expect(rig.getOrder()).toEqual(["b", "a", "c"]);
      rig.release(240);
    } finally {
      rig.cleanup();
    }
  });

  it("reverses a previous swap when the cursor crosses the center back to the left", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      for (let x = 60; x < 200; x += 5) {
        rig.moveTo(x);
      }
      expect(rig.getOrder()).toEqual(["b", "a", "c"]);
      for (let x = 200; x > 60; x -= 5) {
        rig.moveTo(x);
      }
      expect(rig.getOrder()).toEqual(["a", "b", "c"]);
      rig.release(60);
    } finally {
      rig.cleanup();
    }
  });

  it("calls onDragActivated exactly once per drag", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      rig.moveTo(60);
      rig.moveTo(200);
      rig.moveTo(220);
      rig.moveTo(60);
      rig.release(60);
      expect(rig.getActivationCount()).toBe(1);
    } finally {
      rig.cleanup();
    }
  });

  it("does not activate the drag when total movement stays below the threshold", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      rig.moveTo(22);
      rig.moveTo(23);
      rig.moveTo(21);
      rig.release(21);
      expect(rig.getOrder()).toEqual(["a", "b", "c"]);
      expect(rig.getActivationCount()).toBe(0);
    } finally {
      rig.cleanup();
    }
  });

  it("skips the excluded column id when finding a swap target", () => {
    const widths = { __sel: 40, a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["__sel", "a", "b", "c"], {
      widths,
      excludedIds: ["__sel"],
    });
    try {
      rig.pressDown("a", 60);
      for (let x = 60; x < 360; x += 10) {
        rig.moveTo(x);
      }
      const order = rig.getOrder();
      expect(order.indexOf("__sel")).toBe(0);
      rig.release(360);
    } finally {
      rig.cleanup();
    }
  });

  it("calls onDragEnded exactly once after a successful drag", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      for (let x = 60; x < 200; x += 5) {
        rig.moveTo(x);
      }
      expect(rig.getEndCount()).toBe(0);
      rig.release(200);
      expect(rig.getEndCount()).toBe(1);
    } finally {
      rig.cleanup();
    }
  });

  it("calls onDragEnded even when the drag never became active", () => {
    const widths = { a: 40, b: 200, c: 80 };
    installRectMock(widths);
    const rig = mountGrid(["a", "b", "c"], { widths });
    try {
      rig.pressDown("a", 20);
      rig.moveTo(22);
      rig.release(22);
      expect(rig.getActivationCount()).toBe(0);
      expect(rig.getEndCount()).toBe(1);
    } finally {
      rig.cleanup();
    }
  });
});
