import { describe, expect, it, vi, beforeEach } from "vitest";

vi.mock("utils/flags", () => ({
  FEATURES: { LEGACY_UI_BASE_URL: "LEGACY_UI_BASE_URL" },
  featureFlags: {
    getValue: vi.fn(),
  },
}));

import { featureFlags } from "utils/flags";
import { buildLegacyUiSwitchUrl, getLegacyUiBaseUrl } from "./legacyUiSwitch";

describe("legacyUiSwitch", () => {
  beforeEach(() => {
    vi.mocked(featureFlags.getValue).mockReset();
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
});
