import SwapHorizIcon from "@mui/icons-material/SwapHoriz";
import { Alert, Button } from "@mui/material";
import { useLocation } from "react-router-dom";
import {
  activateClassicUiSwitch,
  isClassicUiSwitchAvailable,
} from "utils/legacyUiSwitch";

/**
 * Shown on workflow home when classic UI switch is configured (cookie or legacy DNS).
 */
export default function LegacyUiSwitchBanner() {
  const { pathname, search } = useLocation();

  if (!isClassicUiSwitchAvailable()) {
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
            activateClassicUiSwitch(pathname, search);
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
