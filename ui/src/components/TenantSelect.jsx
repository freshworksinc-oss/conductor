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

function fetchUserEmail() {
  const basename = getBasename();
  return fetch(`${basename}health`).then((response) => {
    const userEmail = response.headers.get("x-user-email");
    if (!userEmail) {
      throw new Error("x-user-email header not found in health response");
    }
    return userEmail;
  });
}

function fetchTenants(email) {
  const basename = getBasename();
  return fetch(
    `${basename}usermanagement/v2/auth/${encodeURIComponent(
      email
    )}/conductor?details=true`
  ).then((response) => {
    if (!response.ok) {
      throw new Error(`Usermanagement API returned ${response.status}`);
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
    fetchUserEmail()
      .then((email) => fetchTenants(email))
      .then((data) => {
        // Response: { message: { "default": "SuperAdmin", "sip": "Editor" } }
        const tenantsMap = data.message || {};
        const tenantList = Object.entries(tenantsMap).map(([id, role]) => ({
          id,
          role,
        }));
        setTenants(tenantList);
        if (tenantList.length > 0) {
          setSelectedTenant(tenantList[0].id);
        }
      })
      .catch((err) => {
        console.error("Failed to load tenants:", err);
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
