/** Must match sandbox auth-proxy ui_cluster_router.lua (conductor-ui-variant cookie). */
export const UI_VARIANT_COOKIE_NAME = "conductor-ui-variant";
export const UI_VARIANT_OLD = "old";
export const UI_VARIANT_NEW = "new";

export function isUiVariantCookieSwitchEnabled() {
  return process.env.REACT_APP_UI_VARIANT_COOKIE_ENABLED === "true";
}

export function setUiVariantCookie(variant) {
  const secure =
    typeof window !== "undefined" && window.location.protocol === "https:"
      ? "; Secure"
      : "";
  document.cookie = `${UI_VARIANT_COOKIE_NAME}=${variant}; path=/; SameSite=Lax${secure}`;
}

/** Same origin: set new cookie and reload (auth-proxy routes to ui-next). */
export function switchToNewUiViaCookie(pathname, search) {
  setUiVariantCookie(UI_VARIANT_NEW);
  const path = pathname.startsWith("/") ? pathname : `/${pathname}`;
  window.location.assign(`${path}${search ?? ""}`);
}

export function activateNewUiSwitch(pathname, search) {
  if (isUiVariantCookieSwitchEnabled()) {
    switchToNewUiViaCookie(pathname, search);
  }
}
