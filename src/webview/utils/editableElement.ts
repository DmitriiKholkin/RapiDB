export function isEditableElement(
  element: Element | EventTarget | null | undefined,
): boolean {
  if (!element) {
    return false;
  }

  const el = element as HTMLElement;
  if (!el.tagName) {
    return false;
  }

  if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
    return true;
  }

  if (el.isContentEditable) {
    return true;
  }

  return false;
}
