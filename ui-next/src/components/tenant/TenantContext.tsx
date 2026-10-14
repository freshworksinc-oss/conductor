import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useState,
} from "react";
import { useQuery } from "react-query";

export type TenantRole = "Viewer" | "Editor" | "SuperAdmin";

export type TenantEntry = {
  id: string;
  role: TenantRole;
};

type UserInfoResponse = {
  email?: string;
  tenants?: Record<string, TenantRole>;
};

const ROLE_LEVELS: Record<TenantRole, number> = {
  Viewer: 0,
  Editor: 1,
  SuperAdmin: 2,
};

type TenantContextValue = {
  tenants: TenantEntry[];
  selectedTenant: string;
  role: TenantRole | "";
  email: string;
  isLoading: boolean;
  setTenant: (tenantId: string) => void;
  canAccess: (minRole: TenantRole) => boolean;
};

const TenantContext = createContext<TenantContextValue | null>(null);

function setTenantCookie(tenantId: string) {
  document.cookie = `x-tenant-id=${encodeURIComponent(tenantId)}; path=/; SameSite=Strict`;
}

function getTenantCookie(): string | null {
  const match = document.cookie.match(/(?:^|;\s*)x-tenant-id=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : null;
}

async function fetchUserInfo(): Promise<UserInfoResponse> {
  const response = await fetch("/api/userinfo");
  if (!response.ok) {
    throw new Error(`userinfo returned ${response.status}`);
  }
  return response.json();
}

function parseTenants(data: UserInfoResponse): TenantEntry[] {
  const tenantsMap = data.tenants || {};
  return Object.entries(tenantsMap).map(([id, role]) => ({ id, role }));
}

export function TenantProvider({ children }: { children: ReactNode }) {
  const [selectedTenantId, setSelectedTenantId] = useState(
    () => getTenantCookie() || "",
  );

  const { data, isLoading } = useQuery("userinfo", fetchUserInfo, {
    staleTime: Infinity,
    cacheTime: Infinity,
    retry: 1,
    onSuccess: (userInfo) => {
      const tenantList = parseTenants(userInfo);
      if (tenantList.length > 0 && !selectedTenantId) {
        const initial = tenantList[0].id;
        setSelectedTenantId(initial);
        setTenantCookie(initial);
      }
    },
  });

  const tenants = data ? parseTenants(data) : [];
  const email = data?.email || "";

  const validTenant = tenants.find((t) => t.id === selectedTenantId);
  const activeTenantId =
    validTenant?.id || (tenants.length > 0 ? tenants[0].id : "");
  const activeRole: TenantRole | "" =
    validTenant?.role || tenants[0]?.role || "";

  const setTenant = useCallback((tenantId: string) => {
    setSelectedTenantId(tenantId);
    setTenantCookie(tenantId);
    window.location.reload();
  }, []);

  const canAccess = useCallback(
    (minRole: TenantRole) =>
      (ROLE_LEVELS[activeRole as TenantRole] ?? -1) >=
      (ROLE_LEVELS[minRole] ?? 0),
    [activeRole],
  );

  const value: TenantContextValue = {
    tenants,
    selectedTenant: activeTenantId,
    role: activeRole,
    email,
    isLoading,
    setTenant,
    canAccess,
  };

  return (
    <TenantContext.Provider value={value}>{children}</TenantContext.Provider>
  );
}

export function useTenant(): TenantContextValue {
  const context = useContext(TenantContext);
  if (!context) {
    throw new Error("useTenant must be used within a TenantProvider");
  }
  return context;
}
