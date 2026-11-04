import {
  CircularProgress,
  FormControl,
  MenuItem,
  Select,
  Stack,
  Typography,
} from "@mui/material";

import { FEATURES, featureFlags } from "utils/flags";

import { useTenant } from "./TenantContext";

export default function TenantSelect() {
  const enabled = featureFlags.isEnabled(FEATURES.TENANT_SELECTION);
  const { tenants, selectedTenant, isLoading, setTenant } = useTenant();

  if (!enabled) {
    return null;
  }

  if (isLoading) {
    return (
      <Stack direction="row" alignItems="center" sx={{ py: 0.5 }}>
        <CircularProgress size={20} />
      </Stack>
    );
  }

  if (tenants.length === 0) {
    return null;
  }

  return (
    <Stack spacing={0.75} sx={{ width: "100%", px: 0.5, py: 0.5 }}>
      <Typography
        variant="subtitle2"
        component="label"
        htmlFor="conductor-tenant-select"
        sx={{
          color: "text.primary",
          fontWeight: 600,
          fontSize: "0.8125rem",
          letterSpacing: "0.02em",
        }}
      >
        Tenant
      </Typography>
      <FormControl size="small" fullWidth>
        <Select
          id="conductor-tenant-select"
          value={selectedTenant}
          onChange={(e) => setTenant(String(e.target.value))}
          displayEmpty
          sx={{
            fontSize: "0.875rem",
            fontWeight: 500,
            color: "text.primary",
            "& .MuiSelect-select": {
              py: 1,
            },
          }}
        >
          {tenants.map((tenant) => (
            <MenuItem
              key={tenant.id}
              value={tenant.id}
              sx={{ fontSize: "0.875rem", fontWeight: 500 }}
            >
              {tenant.id}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Stack>
  );
}
