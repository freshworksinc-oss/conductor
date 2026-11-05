import { describe, expect, it, vi, beforeEach } from "vitest";

vi.mock("utils/flags", () => ({
  FEATURES: {
    LEGACY_UI_BASE_URL: "LEGACY_UI_BASE_URL",
    UI_VARIANT_COOKIE_ENABLED: "UI_VARIANT_COOKIE_ENABLED",
  },
  featureFlags: {
    getValue: vi.fn(),
    isEnabled: vi.fn(),
  },
}));

import { featureFlags } from "utils/flags";
import {
  buildLegacyUiSwitchUrl,
  ensureDefaultNewUiVariantCookie,
  getLegacyUiBaseUrl,
  isClassicUiSwitchAvailable,
  isUiVariantCookieSwitchEnabled,
  setUiVariantCookie,
  UI_VARIANT_NEW,
  UI_VARIANT_OLD,
  UI_VARIANT_COOKIE_NAME,
} from "./legacyUiSwitch";

describe("legacyUiSwitch", () => {
  beforeEach(() => {
    vi.mocked(featureFlags.getValue).mockReset();
    vi.mocked(featureFlags.isEnabled).mockReset();
    document.cookie = `${UI_VARIANT_COOKIE_NAME}=; path=/; max-age=0`;
  });

  it("returns undefined when LEGACY_UI_BASE_URL is unset", () => {
    vi.mocked(featureFlags.getValue).mockReturnValue(undefined);
    expect(getLegacyUiBaseUrl()).toBeUndefined();
  });

  it("normalizes base URL to origin", () => {
    vi.mocked(featureFlags.getValue).mockReturnValue(
      "https://sandbox-caas-ui.freshworkscorp.com/extra",
    );
    expect(getLegacyUiBaseUrl()).toBe(
      "https://sandbox-caas-ui.freshworkscorp.com",
    );
  });

  it("builds switch URL with path and query", () => {
    vi.mocked(featureFlags.getValue).mockReturnValue(
      "https://sandbox-caas-ui.freshworkscorp.com",
    );
    expect(buildLegacyUiSwitchUrl("/executions", "?freeText=foo")).toBe(
      "https://sandbox-caas-ui.freshworkscorp.com/executions?freeText=foo",
    );
  });

  it("detects cookie switch when UI_VARIANT_COOKIE_ENABLED is on", () => {
    vi.mocked(featureFlags.isEnabled).mockImplementation((key) =>
      key === "UI_VARIANT_COOKIE_ENABLED" ? true : false,
    );
    vi.mocked(featureFlags.getValue).mockReturnValue(undefined);
    expect(isUiVariantCookieSwitchEnabled()).toBe(true);
    expect(isClassicUiSwitchAvailable()).toBe(true);
  });

  it("sets conductor-ui-variant cookie", () => {
    setUiVariantCookie(UI_VARIANT_OLD);
    expect(document.cookie).toContain(
      `${UI_VARIANT_COOKIE_NAME}=${UI_VARIANT_OLD}`,
    );
  });

  it("sets new cookie when variant cookie is missing and switch enabled", () => {
    vi.mocked(featureFlags.isEnabled).mockImplementation((key) =>
      key === "UI_VARIANT_COOKIE_ENABLED" ? true : false,
    );
    ensureDefaultNewUiVariantCookie();
    expect(document.cookie).toContain(
      `${UI_VARIANT_COOKIE_NAME}=${UI_VARIANT_NEW}`,
    );
  });
});
