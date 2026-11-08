import type { KeyboardEvent as ReactKeyboardEvent } from "react";

export function isImeComposing(event: ReactKeyboardEvent): boolean {
  return event.nativeEvent.isComposing || event.keyCode === 229;
}
