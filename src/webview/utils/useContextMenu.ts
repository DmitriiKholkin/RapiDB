import { useCallback, useEffect, useRef, useState } from "react";

export interface ContextMenuState<TMeta = Record<string, unknown>> {
  x: number;
  y: number;
  meta: TMeta;
}

interface UseContextMenuOptions<TMeta> {
  buildMeta: () => TMeta;
  getPosition?: (event: MouseEvent) => { x: number; y: number };
  closeOnBlur?: boolean;
}

interface UseContextMenuReturn<TMeta> {
  menu: ContextMenuState<TMeta> | null;
  open: (event: MouseEvent) => void;
  close: () => void;
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

  const close = useCallback(() => setMenu(null), []);

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
