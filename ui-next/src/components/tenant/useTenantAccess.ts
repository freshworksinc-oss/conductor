import { FEATURES, featureFlags } from "utils/flags";

import { TenantRole, useTenant } from "./TenantContext";

/**
 * Freshworks auth-proxy tenant roles. When {@link FEATURES.TENANT_SELECTION} is
 * off, editing is unrestricted (OSS / local dev without userinfo).
 */
export function useTenantAccess() {
  const tenantSelectionEnabled = featureFlags.isEnabled(
    FEATURES.TENANT_SELECTION,
  );
  const tenant = useTenant();

  const canAccess = (minRole: TenantRole) => {
    if (!tenantSelectionEnabled) {
      return true;
    }
    return tenant.canAccess(minRole);
  };

  return {
    ...tenant,
    tenantSelectionEnabled,
    canAccess,
  };
}
