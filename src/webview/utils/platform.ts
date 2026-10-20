export function isMac(): boolean {
  if (typeof navigator === "undefined") {
    return false;
  }
  return navigator.platform.toUpperCase().includes("MAC");
}

export function platformShortcutLabel(): string {
  return isMac() ? "\u2318" : "Ctrl+";
}

export function queryRunShortcutLabel(): string {
  return isMac() ? "\u2318+Return" : "Ctrl+Enter";
}
