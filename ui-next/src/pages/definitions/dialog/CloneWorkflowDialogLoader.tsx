import { Dialog, DialogContent, DialogTitle } from "@mui/material";
import { useFetchContext } from "plugins/fetch";
import { useMemo } from "react";
import { useQueryClient } from "react-query";
import { WorkflowDef } from "types/WorkflowDef";
import { useFetch } from "utils/query";
import {
  WORKFLOW_NAMES_AND_VERSIONS_URL,
  WORKFLOW_NAMES_URL,
  WorkflowDefSummary,
  workflowVersionsUrl,
} from "utils/workflowDefLists";
import CloneWorkflowDialog, {
  CloneWorkflowDialogProps,
} from "./CloneWorkflowDialog";

/**
 * Feeds CloneWorkflowDialog without the full definitions list: every workflow name (to refuse an
 * existing one) and the versions of the selected workflow, neither with definition bodies. The
 * dialog computes its defaults once, so it is shown when both have arrived. Same props as the
 * dialog, so it is a drop-in.
 */
const CloneWorkflowDialogLoader = ({
  selectedWorkflow,
  onClose,
  onSuccess,
}: CloneWorkflowDialogProps) => {
  // a passed workflowList (the current page only) is not used: names and versions are loaded
  const fetchContext = useFetchContext();
  const queryClient = useQueryClient();
  const { data: names, isFetched: namesFetched } =
    useFetch<string[]>(WORKFLOW_NAMES_URL);
  const { data: versions, isFetched: versionsFetched } = useFetch<
    WorkflowDefSummary[]
  >(workflowVersionsUrl(selectedWorkflow.name));

  // the dialog reads names and per-name versions from a list of definitions
  const workflowList = useMemo<WorkflowDef[]>(() => {
    const selectedVersions =
      Array.isArray(versions) && versions.length > 0
        ? versions.map((summary) => summary.version)
        : [selectedWorkflow.version];
    const others = (Array.isArray(names) ? names : [])
      .filter((name) => name !== selectedWorkflow.name)
      .map((name) => ({ name, version: 1 }));
    const selected = selectedVersions.map((version) => ({
      name: selectedWorkflow.name,
      version,
    }));
    return [...others, ...selected] as WorkflowDef[];
  }, [names, versions, selectedWorkflow]);

  if (!namesFetched || !versionsFetched) {
    return (
      <Dialog fullWidth maxWidth="sm" open onClose={onClose}>
        <DialogTitle>Clone Workflow Confirmation</DialogTitle>
        <DialogContent>Loading workflows…</DialogContent>
      </Dialog>
    );
  }

  return (
    <CloneWorkflowDialog
      selectedWorkflow={selectedWorkflow}
      workflowList={workflowList}
      onClose={onClose}
      onSuccess={() => {
        // the clone is a new name: pickers should see it without waiting for the stale time
        queryClient.removeQueries([fetchContext.stack, WORKFLOW_NAMES_URL]);
        queryClient.removeQueries([
          fetchContext.stack,
          WORKFLOW_NAMES_AND_VERSIONS_URL,
        ]);
        onSuccess();
      }}
    />
  );
};

export default CloneWorkflowDialogLoader;
