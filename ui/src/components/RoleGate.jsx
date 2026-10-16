import { useTenant } from "./TenantContext";

export default function RoleGate({ minRole, children }) {
  const { canAccess } = useTenant();
  return canAccess(minRole) ? children : null;
}
