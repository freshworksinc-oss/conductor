import { Alert, AlertTitle } from "@mui/material";
import SectionContainer from "components/ui/layout/SectionContainer";

/** Shown when scheduler nav is on but the server has no scheduler backend (e.g. Scylla). */
export function SchedulerInProgress() {
  return (
    <SectionContainer>
      <Alert severity="info" sx={{ maxWidth: 720 }}>
        <AlertTitle>Scheduler — in progress</AlertTitle>
        Scheduled workflows are not available in this environment yet.
      </Alert>
    </SectionContainer>
  );
}
