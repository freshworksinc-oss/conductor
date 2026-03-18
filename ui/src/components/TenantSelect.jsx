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

function fetchUserInfo() {
  const basename = getBasename();
  return fetch(`${basename}userinfo`).then((response) => {
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
          setSelectedTenant(tenantList[0].id);
          console.log("[TenantSelect] Default tenant set to:", tenantList[0].id);
        }
      })
      .catch((err) => {
        console.error("[TenantSelect] Failed to load tenants:", err.message);
        setError(err.message);
      })
      .finally(() => setLoading(false));
  }, []);

  const handleChange = (event) => {
    setSelectedTenant(event.target.value);
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
  );
}
