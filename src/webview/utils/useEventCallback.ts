/**
 * `useEventCallback` — stable callback that always sees the latest closure.
 *
 * The codebase has a recurring pattern: a callback must be passed to a
 * long-lived effect (e.g. message bus subscription, `setInterval`, native
 * event listener) but should read the *latest* props/state without
 * re-subscribing on every render. The naïve solution is to mirror the
 * callback into a `useRef` and update it in a `useEffect`, which is
 * ~5 lines of boilerplate per callback. This hook condenses that into
 * a single line at the call site.
 *
 * Use cases in this codebase:
 *   - `useCellSelection` — 4 callbacks
 *   - `useTableDataController` — 8+ callbacks
 *   - `useTableMutationController` — 6+ callbacks
 *   - `useContextMenu` — 3 callbacks
 *   - `useColumnDragReorder` — 4 callbacks
 *
 * The returned function has a stable identity across renders, so it
 * will not invalidate `useEffect` / `useMemo` dependency lists.
 */
import { useCallback, useLayoutEffect, useRef } from "react";

/**
 * The function type mirrors the input but with explicit `void` return
 * to discourage accidental `await` of a "fire-and-forget" callback.
 */
export type EventCallback<Args extends unknown[]> = (...args: Args) => void;

export function useEventCallback<Args extends unknown[]>(
  fn: EventCallback<Args>,
): EventCallback<Args> {
  // `useLayoutEffect` keeps the ref in sync *before* the browser paints
  // so that any event that fires in the same commit (e.g. nested
  // synchronous dispatches) sees the fresh closure. This matches the
  // semantics of React's experimental `useEvent` proposal.
  const ref = useRef<EventCallback<Args>>(fn);
  useLayoutEffect(() => {
    ref.current = fn;
  }, [fn]);

  // `useCallback` gives us a stable identity, while the inner closure
  // delegates to the ref. The empty deps are intentional.
  return useCallback((...args: Args) => {
    ref.current(...args);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}
