/**
 * Shared focus-trap hook for modals and overlays.
 *
 * Two consumers today with subtly different shapes:
 *   - `GridLoadingOverlay` (focus on mount, swallow Tab, restore on unmount)
 *   - `TableDialogs` (Tab cycling, Escape, focus restore on unmount)
 *
 * `useFocusTrap` covers the union: a handler for keyboard events plus
 * a `containerRef` that is registered for focus restoration. Callers
 * pick which behaviours they want.
 */
import { useEffect, useRef } from "react";
import { useEventCallback } from "./useEventCallback";

/** Selector for elements that should be reachable via Tab in a dialog. */
const FOCUSABLE_SELECTOR =
  'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

function getFocusableElements(container: HTMLDivElement | null): HTMLElement[] {
  if (!container) {
    return [];
  }
  return Array.from(
    container.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
  );
}

export interface UseFocusTrapOptions {
  /**
   * When true, the dialog installs a keydown handler that:
   *   - Calls `onEscape` on Escape
   *   - Cycles focus on Tab (first↔last) and Shift+Tab
   *   - Calls `e.preventDefault()` to swallow Tab when there are no
   *     focusable elements inside the dialog
   */
  enabled: boolean;
  /** Called when the user presses Escape. Omit to ignore Escape. */
  onEscape?: () => void;
  /**
   * Optional ref to an element that should receive focus when the
   * dialog opens. Defaults to the container itself.
   */
  initialFocusRef?: React.RefObject<HTMLElement | null>;
}

export interface UseFocusTrapReturn {
  /** Attach to the dialog container's `onKeyDown` prop. */
  onKeyDown: (event: React.KeyboardEvent<HTMLDivElement>) => void;
  /**
   * Ref to attach to the dialog container. The hook owns this ref so
   * it can be passed directly without callers having to wire a parallel
   * `useRef`.
   */
  containerRef: React.RefObject<HTMLDivElement | null>;
}

export function useFocusTrap(options: UseFocusTrapOptions): UseFocusTrapReturn {
  const { enabled, onEscape, initialFocusRef } = options;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const restoreRef = useRef<HTMLElement | null>(null);

  // Snapshot the previously-focused element once, on mount, so we can
  // restore it on unmount. We deliberately key this effect on `enabled`
  // so toggling the trap (e.g. opening/closing) triggers a fresh capture.
  useEffect(() => {
    if (!enabled) {
      return;
    }
    restoreRef.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;

    // Focus the initial element (or the container) on next frame so
    // the dialog is in the DOM by then. We deliberately re-read the
    // ref on every run instead of memoising it; the consumer's
    // `initialFocusRef` is meant to point at a stable element across
    // the dialog's lifetime, so capturing it once at mount is correct.
    const target = initialFocusRef?.current ?? containerRef.current;
    const frame = requestAnimationFrame(() => {
      target?.focus();
    });

    return () => {
      cancelAnimationFrame(frame);
      restoreRef.current?.focus();
      restoreRef.current = null;
    };
  }, [enabled, initialFocusRef]);

  // Stable handler — the implementation reads the latest `onEscape`
  // through a ref so the parent does not need to memoize it.
  const onKeyDown = useEventCallback<[React.KeyboardEvent<HTMLDivElement>]>(
    (event) => {
      if (!enabled) {
        return;
      }
      if (event.key === "Escape") {
        onEscape?.();
        return;
      }
      if (event.key !== "Tab") {
        return;
      }
      const dialog = containerRef.current;
      const focusable = getFocusableElements(dialog);
      if (focusable.length === 0) {
        event.preventDefault();
        dialog?.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active =
        document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
      if (!active || !dialog?.contains(active)) {
        event.preventDefault();
        first.focus();
        return;
      }
      if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
        return;
      }
      if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    },
  );

  return { onKeyDown, containerRef };
}
