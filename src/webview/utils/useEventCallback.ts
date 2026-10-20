import { useCallback, useLayoutEffect, useRef } from "react";

export type EventCallback<Args extends unknown[]> = (...args: Args) => void;

export function useEventCallback<Args extends unknown[]>(
  fn: EventCallback<Args>,
): EventCallback<Args> {
  const ref = useRef<EventCallback<Args>>(fn);
  useLayoutEffect(() => {
    ref.current = fn;
  }, [fn]);

  return useCallback((...args: Args) => {
    ref.current(...args);
  }, []);
}
