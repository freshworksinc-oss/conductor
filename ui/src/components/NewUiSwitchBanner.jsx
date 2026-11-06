import React from "react";
import { Button } from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import SwapHorizIcon from "@material-ui/icons/SwapHoriz";
import { useLocation } from "react-router-dom";
import { makeStyles } from "@material-ui/styles";
import {
  activateNewUiSwitch,
  isUiVariantCookieSwitchEnabled,
} from "../utils/legacyUiSwitch";

const useStyles = makeStyles({
  banner: {
    margin: "12px 24px 0",
    alignItems: "center",
  },
  action: {
    whiteSpace: "nowrap",
  },
});

/**
 * Shown on classic UI when cookie-based ui-next / legacy routing is enabled.
 */
export default function NewUiSwitchBanner() {
  const classes = useStyles();
  const { pathname, search } = useLocation();

  if (!isUiVariantCookieSwitchEnabled()) {
    return null;
  }

  return (
    <Alert
      severity="info"
      className={classes.banner}
      action={
        <Button
          color="inherit"
          size="small"
          variant="outlined"
          className={classes.action}
          startIcon={<SwapHorizIcon />}
          onClick={() => activateNewUiSwitch(pathname, search)}
        >
          Switch to updated UI
        </Button>
      }
    >
      You are using the classic Conductor UI. Try the updated experience when
      you are ready.
    </Alert>
  );
}
