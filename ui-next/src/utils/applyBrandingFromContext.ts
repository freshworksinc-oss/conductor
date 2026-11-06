/**
 * Applies runtime branding from window.conductor (context.js) after the script loads.
 */
export function applyBrandingFromContext(): void {
  const conductor = window.conductor;
  if (!conductor) {
    return;
  }

  const faviconUrl =
    typeof conductor.FAVICON_URL === "string" && conductor.FAVICON_URL.trim()
      ? conductor.FAVICON_URL.trim()
      : undefined;

  if (faviconUrl) {
    document
      .querySelectorAll("link[rel='icon'], link[rel='shortcut icon']")
      .forEach((node) => node.remove());
    const link = document.createElement("link");
    link.rel = "icon";
    link.href = faviconUrl;
    document.head.appendChild(link);
  }

  if (
    typeof conductor.DOCUMENT_TITLE === "string" &&
    conductor.DOCUMENT_TITLE.trim()
  ) {
    document.title = conductor.DOCUMENT_TITLE.trim();
  }
}
