export function cssVar(name: string): string {
  return getComputedStyle(document.body).getPropertyValue(name).trim();
}
