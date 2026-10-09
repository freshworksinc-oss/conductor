import React from "react";
import { makeStyles } from "@material-ui/styles";
import {
  Select,
  MenuItem,
  FormControl,
  CircularProgress,
} from "@material-ui/core";
import { useTenant } from "./TenantContext";

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

export default function TenantSelect() {
  const classes = useStyles();
  const { tenants, selectedTenant, isLoading, setTenant } = useTenant();

  if (isLoading) {
    return (
      <div className={classes.loading}>
        <CircularProgress size={20} />
      </div>
    );
  }

  if (tenants.length === 0) {
    return null;
  }

  return (
    <div className={classes.wrapper}>
      <span className={classes.label}>Tenant</span>
      <FormControl variant="outlined" className={classes.formControl}>
        <Select
          value={selectedTenant}
          onChange={(e) => setTenant(e.target.value)}
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
