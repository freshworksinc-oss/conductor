import { FEATURES, featureFlags } from "utils/flags";

/** Must match sandbox auth-proxy ui_cluster_router.lua (conductor-ui-variant cookie). */
export const UI_VARIANT_COOKIE_NAME = "conductor-ui-variant";
export const UI_VARIANT_OLD = "old";
export const UI_VARIANT_NEW = "new";

/** Base URL for legacy CRA UI on a separate DNS (optional; cookie mode preferred). */
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

export function isUiVariantCookieSwitchEnabled(): boolean {
  return featureFlags.isEnabled(FEATURES.UI_VARIANT_COOKIE_ENABLED);
}

/** True when the workflow home banner should offer switching to classic UI. */
export function isClassicUiSwitchAvailable(): boolean {
  return isUiVariantCookieSwitchEnabled() || !!getLegacyUiBaseUrl();
}

export function getUiVariantCookieValue(): string | undefined {
  if (typeof document === "undefined") {
    return undefined;
  }
  const match = document.cookie.match(
    /(?:^|;\s*)conductor-ui-variant=([^;]*)/,
  );
  return match ? decodeURIComponent(match[1]) : undefined;
}

export function setUiVariantCookie(variant: typeof UI_VARIANT_OLD | typeof UI_VARIANT_NEW): void {
  const secure =
    typeof window !== "undefined" && window.location.protocol === "https:"
      ? "; Secure"
      : "";
  document.cookie = `${UI_VARIANT_COOKIE_NAME}=${variant}; path=/; SameSite=Lax${secure}`;
}

/** Remove UI variant cookie; auth-proxy treats absence as ui-next (new). */
export function clearUiVariantCookie(): void {
  document.cookie = `${UI_VARIANT_COOKIE_NAME}=; path=/; max-age=0; SameSite=Lax`;
}

/**
 * When cookie switch is on, persist default ui-next unless user chose old UI.
 * Call on ui-next boot (missing/invalid cookie → set "new").
 */
export function ensureDefaultNewUiVariantCookie(): void {
  if (!isUiVariantCookieSwitchEnabled()) {
    return;
  }
  const value = getUiVariantCookieValue();
  if (value === UI_VARIANT_OLD) {
    return;
  }
  if (value !== UI_VARIANT_NEW) {
    setUiVariantCookie(UI_VARIANT_NEW);
  }
}

/** Same origin: set new cookie and reload (auth-proxy routes to ui-next). */
export function switchToNewUiViaCookie(
  pathname: string,
  search: string,
): void {
  setUiVariantCookie(UI_VARIANT_NEW);
  const path = pathname.startsWith("/") ? pathname : `/${pathname}`;
  window.location.assign(`${path}${search ?? ""}`);
}

/** Same origin: set classic cookie and reload (auth-proxy routes to legacy UI deployment). */
export function switchToClassicUiViaCookie(
  pathname: string,
  search: string,
): void {
  setUiVariantCookie(UI_VARIANT_OLD);
  const path = pathname.startsWith("/") ? pathname : `/${pathname}`;
  window.location.assign(`${path}${search ?? ""}`);
}

/** Separate DNS cutover: navigate to legacy host + path. */
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

export function activateClassicUiSwitch(
  pathname: string,
  search: string,
): void {
  if (isUiVariantCookieSwitchEnabled()) {
    switchToClassicUiViaCookie(pathname, search);
    return;
  }
  const targetUrl = buildLegacyUiSwitchUrl(pathname, search);
  if (targetUrl) {
    window.location.assign(targetUrl);
  }
}
