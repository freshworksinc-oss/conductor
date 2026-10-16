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
    <Stack spacing={0.5} sx={{ width: "100%" }}>
      <Typography variant="caption" color="text.secondary" fontWeight={500}>
        Tenant
      </Typography>
      <FormControl size="small" fullWidth>
        <Select
          value={selectedTenant}
          onChange={(e) => setTenant(String(e.target.value))}
          displayEmpty
        >
          {tenants.map((tenant) => (
            <MenuItem key={tenant.id} value={tenant.id}>
              {tenant.id}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Stack>
  );
}
