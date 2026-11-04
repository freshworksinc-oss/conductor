import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import { Alert, Button } from "@mui/material";
import { useLocation } from "react-router-dom";
import { buildLegacyUiSwitchUrl } from "utils/legacyUiSwitch";

/**
 * Shown on workflow home when LEGACY_UI_BASE_URL is set (Freshworks UI cutover).
 * Navigates to the legacy UI hostname in the same tab, keeping path/query when possible.
 */
export default function LegacyUiSwitchBanner() {
  const { pathname, search } = useLocation();
  const targetUrl = buildLegacyUiSwitchUrl(pathname, search);

  if (!targetUrl) {
    return null;
  }

  return (
    <Alert
      severity="info"
      sx={{ mx: 3, mt: 2, mb: 0, alignItems: "center" }}
      action={
        <Button
          color="inherit"
          size="small"
          variant="outlined"
          startIcon={<SwapHorizIcon />}
          onClick={() => {
            window.location.assign(targetUrl);
          }}
        >
          Switch to classic UI
        </Button>
      }
    >
      This is the updated Conductor UI. Old UI will be deprecated soon
    </Alert>
  );
}
