/**
 * Platform detection utilities.
 *
 * Centralises the `"MAC" in navigator.platform` check that previously
 * lived in `useUndoRedoKeyboard.ts` and `TableStatusBanners.tsx`. The
 * VS Code webview runs on the same engine as the host, so `navigator`
 * is always available and we do not need to guard it.
 */

/** Returns `true` when the user is on macOS (Darwin). */
export function isMac(): boolean {
  if (typeof navigator === "undefined") {
    return false;
  }
  return navigator.platform.toUpperCase().includes("MAC");
}

/** Returns the conventional shortcut modifier glyph for the platform. */
export function platformShortcutLabel(): string {
  return isMac() ? "\u2318" : "Ctrl+";
}
