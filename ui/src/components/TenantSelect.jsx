import React, { useState, useEffect } from "react";
import { makeStyles } from "@material-ui/styles";
import {
  Select,
  MenuItem,
  FormControl,
  CircularProgress,
} from "@material-ui/core";
import { getBasename } from "../utils/helpers";

const useStyles = makeStyles((theme) => ({
  wrapper: {
    display: "flex",
    alignItems: "center",
    gap: 8,
  },
  label: {
    fontSize: 13,
    fontWeight: 500,
    color: theme.palette.text.secondary,
    whiteSpace: "nowrap",
  },
  formControl: {
    minWidth: 160,
  },
  select: {
    fontSize: 13,
    fontWeight: 500,
    color: theme.palette.text.primary,
    backgroundColor: theme.palette.background.paper,
    borderRadius: theme.shape.borderRadius,
    padding: "6px 12px",
    "&:focus": {
      backgroundColor: theme.palette.background.paper,
      borderRadius: theme.shape.borderRadius,
    },
  },
  loading: {
    display: "flex",
    alignItems: "center",
    padding: "6px 12px",
  },
}));

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

export default function TenantSelect() {
  const classes = useStyles();
  const [tenants, setTenants] = useState([]);
  const [selectedTenant, setSelectedTenant] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    console.log("[TenantSelect] Component mounted, fetching userinfo...");
    fetchUserInfo()
      .then((data) => {
        console.log("[TenantSelect] userinfo response:", JSON.stringify(data));
        // Response: { email: "...", tenants: { "default": "SuperAdmin", "sip": "Editor" } }
        const tenantsMap = data.tenants || {};
        const tenantList = Object.entries(tenantsMap).map(([id, role]) => ({
          id,
          role,
        }));
        console.log("[TenantSelect] Parsed tenant list:", JSON.stringify(tenantList));
        setTenants(tenantList);
        if (tenantList.length > 0) {
          const savedTenant = getTenantCookie();
          const initial =
            tenantList.find((t) => t.id === savedTenant)
              ? savedTenant
              : tenantList[0].id;
          setSelectedTenant(initial);
          setTenantCookie(initial);
          console.log("[TenantSelect] Tenant set to:", initial);
        }
      })
      .catch((err) => {
        console.warn("[TenantSelect] Failed to load tenants, using defaults:", err.message);
      })
      .finally(() => setLoading(false));
  }, []);

  const handleChange = (event) => {
    const tenantId = event.target.value;
    setSelectedTenant(tenantId);
    setTenantCookie(tenantId);
    console.log("[TenantSelect] Tenant changed to:", tenantId);
    window.location.reload();
  };

  if (loading) {
    return (
      <div className={classes.loading}>
        <CircularProgress size={20} />
      </div>
    );
  }

  if (error || tenants.length === 0) {
    return null;
  }

  return (
    <div className={classes.wrapper}>
      <span className={classes.label}>Tenant</span>
      <FormControl variant="outlined" className={classes.formControl}>
        <Select
          value={selectedTenant}
          onChange={handleChange}
          classes={{ select: classes.select }}
        >
          {tenants.map((tenant) => (
            <MenuItem key={tenant.id} value={tenant.id}>
              {tenant.id}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </div>
  );
}
