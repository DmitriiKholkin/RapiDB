import { useEffect, useRef } from "react";
import { useEventCallback } from "./useEventCallback";

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
  enabled: boolean;
  onEscape?: () => void;
  initialFocusRef?: React.RefObject<HTMLElement | null>;
}

export interface UseFocusTrapReturn {
  onKeyDown: (event: React.KeyboardEvent<HTMLDivElement>) => void;
  containerRef: React.RefObject<HTMLDivElement | null>;
}

export function useFocusTrap(options: UseFocusTrapOptions): UseFocusTrapReturn {
  const { enabled, onEscape, initialFocusRef } = options;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const restoreRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    restoreRef.current =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;

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
