import { FEATURES, featureFlags } from "utils/flags";

/** Base URL for legacy CRA UI (separate DNS), from context.js or build env. */
export function getLegacyUiBaseUrl(): string | undefined {
  const raw = featureFlags.getValue(FEATURES.LEGACY_UI_BASE_URL)?.trim();
  if (!raw) {
    return undefined;
  }
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
      return undefined;
    }
    return parsed.origin;
  } catch {
    return undefined;
  }
}

/** Same-tab navigation target: legacy host + current path (tenant cookie may differ by host). */
export function buildLegacyUiSwitchUrl(
  pathname: string,
  search: string,
): string | undefined {
  const origin = getLegacyUiBaseUrl();
  if (!origin) {
    return undefined;
  }
  const path = pathname.startsWith("/") ? pathname : `/${pathname}`;
  return `${origin}${path}${search ?? ""}`;
}
