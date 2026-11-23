import { StartSubWfNameVersionMachineContext } from "./types";

import { queryClient } from "queryClient";
import { fetchWithContext, fetchContextNonHook } from "plugins/fetch";

import { logger } from "utils/logger";
import {
  namesAndVersionsToMap,
  WORKFLOW_NAMES_AND_VERSIONS_URL,
} from "utils/workflowDefLists";

const fetchContext = fetchContextNonHook();

export const fetchWfNamesAndVersions = async ({
  authHeaders: headers,
}: StartSubWfNameVersionMachineContext) => {
  const url = WORKFLOW_NAMES_AND_VERSIONS_URL;
  try {
    const response = await queryClient.fetchQuery(
      [fetchContext.stack, url],
      () => fetchWithContext(url, fetchContext, { headers }),
    );
    return namesAndVersionsToMap(response);
  } catch (error) {
    logger.error("Fetching Wf short", error);
    return Promise.reject({ message: "Error fetching wf short" });
  }
};
