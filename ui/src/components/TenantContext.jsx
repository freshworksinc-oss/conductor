import React, { createContext, useContext, useState, useCallback } from "react";
import { useQuery } from "react-query";
import { getBasename } from "../utils/helpers";

const TenantContext = createContext(null);

// Role hierarchy: higher index = more permissions
const ROLE_LEVELS = {
  Viewer: 0,
  Editor: 1,
  SuperAdmin: 2,
};

function setTenantCookie(tenantId) {
  document.cookie = `x-tenant-id=${encodeURIComponent(tenantId)}; path=/; SameSite=Strict`;
}

function getTenantCookie() {
  const match = document.cookie.match(/(?:^|;\s*)x-tenant-id=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : null;
}

function fetchUserInfo() {
  const basename = getBasename();
  return fetch(`${basename}api/userinfo`).then((response) => {
    if (!response.ok) {
      throw new Error(`userinfo returned ${response.status}`);
    }
    return response.json();
  });
}

function parseTenants(data) {
  const tenantsMap = data.tenants || {};
  return Object.entries(tenantsMap).map(([id, role]) => ({ id, role }));
}

export function TenantProvider({ children }) {
  const [selectedTenantId, setSelectedTenantId] = useState(
    () => getTenantCookie() || ""
  );

  const { data, isLoading } = useQuery("userinfo", fetchUserInfo, {
    staleTime: Infinity,
    cacheTime: Infinity,
    retry: 1,
    onSuccess: (data) => {
      const tenantList = parseTenants(data);
      if (tenantList.length > 0 && !selectedTenantId) {
        const initial = tenantList[0].id;
        setSelectedTenantId(initial);
        setTenantCookie(initial);
      }
    },
  });

  const tenants = data ? parseTenants(data) : [];
  const email = data?.email || "";

  // Validate saved cookie against actual tenant list
  const validTenant = tenants.find((t) => t.id === selectedTenantId);
  const activeTenantId =
    validTenant?.id || (tenants.length > 0 ? tenants[0].id : "");
  const activeRole = validTenant?.role || tenants[0]?.role || "";

  const setTenant = useCallback(
    (tenantId) => {
      setSelectedTenantId(tenantId);
      setTenantCookie(tenantId);
      window.location.reload();
    },
    []
  );

  // Check if current role meets minimum required role
  // Usage: canAccess("Editor") → true if role is Editor or SuperAdmin
  const canAccess = useCallback(
    (minRole) => (ROLE_LEVELS[activeRole] ?? -1) >= (ROLE_LEVELS[minRole] ?? 0),
    [activeRole]
  );

  const value = {
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

export function useTenant() {
  const context = useContext(TenantContext);
  if (!context) {
    throw new Error("useTenant must be used within a TenantProvider");
  }
  return context;
}
