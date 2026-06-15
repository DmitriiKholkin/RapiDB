/**
 * Reusable context menu hook.
 *
 * Manages open/close state, position, and dismiss-on-outside-click/Escape.
 * Menu items and actions stay in the consuming component — this hook only
 * handles the menu shell.
 *
 * Listeners are tied to the lifetime of the menu via an `AbortController`
 * so we never leak global `window` handlers when the menu unmounts.
 *
 * @typeParam TMeta - Arbitrary metadata attached to the menu (e.g. hasSelection).
 */
import { useCallback, useEffect, useRef, useState } from "react";

export interface ContextMenuState<TMeta = Record<string, unknown>> {
  x: number;
  y: number;
  meta: TMeta;
}

interface UseContextMenuOptions<TMeta> {
  /** Return current metadata when the menu opens. */
  buildMeta: () => TMeta;
  /**
   * Compute the menu position from the native event.
   * Defaults to fixed positioning (clientX / clientY).
   */
  getPosition?: (event: MouseEvent) => { x: number; y: number };
  /** Close on window blur (default: true). */
  closeOnBlur?: boolean;
}

interface UseContextMenuReturn<TMeta> {
  menu: ContextMenuState<TMeta> | null;
  open: (event: MouseEvent) => void;
  close: () => void;
  /** Ref to attach to the menu DOM node for outside-click detection. */
  menuRef: React.RefObject<HTMLDivElement | null>;
}

const DEFAULT_POSITION = (event: MouseEvent) => ({
  x: event.clientX,
  y: event.clientY,
});

const ESCAPE_KEY = "Escape";

export function useContextMenu<TMeta = Record<string, unknown>>({
  buildMeta,
  getPosition = DEFAULT_POSITION,
  closeOnBlur = true,
}: UseContextMenuOptions<TMeta>): UseContextMenuReturn<TMeta> {
  const [menu, setMenu] = useState<ContextMenuState<TMeta> | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);

  // `close` is stable; no need for a ref dance.
  const close = useCallback(() => setMenu(null), []);

  // Stash mutable options in refs so the effect can stay focused on the
  // menu-open transition without re-subscribing on every render.
  const buildMetaRef = useRef(buildMeta);
  const getPositionRef = useRef(getPosition);
  const closeOnBlurRef = useRef(closeOnBlur);
  useEffect(() => {
    buildMetaRef.current = buildMeta;
    getPositionRef.current = getPosition;
    closeOnBlurRef.current = closeOnBlur;
  }, [buildMeta, getPosition, closeOnBlur]);

  const open = useCallback((event: MouseEvent) => {
    event.preventDefault();
    const pos = getPositionRef.current(event);
    setMenu({ x: pos.x, y: pos.y, meta: buildMetaRef.current() });
  }, []);

  // Subscribe to global dismissal events only while the menu is open.
  useEffect(() => {
    if (!menu) {
      return;
    }

    const controller = new AbortController();
    const { signal } = controller;

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (target instanceof Node && menuRef.current?.contains(target)) {
        return;
      }
      close();
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === ESCAPE_KEY) {
        close();
      }
    };

    window.addEventListener("pointerdown", handlePointerDown, { signal });
    window.addEventListener("keydown", handleEscape, { signal });
    if (closeOnBlurRef.current) {
      window.addEventListener("blur", close, { signal });
    }

    return () => controller.abort();
  }, [menu, close]);

  return { menu, open, close, menuRef };
}
