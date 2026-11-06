import { ReactNode } from "react";

import { TenantRole } from "./TenantContext";
import { useTenantAccess } from "./useTenantAccess";

type Props = {
  minRole: TenantRole;
  children: ReactNode;
};

export default function RoleGate({ minRole, children }: Props) {
  const { canAccess } = useTenantAccess();
  return canAccess(minRole) ? <>{children}</> : null;
}
