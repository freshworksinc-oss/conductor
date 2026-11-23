/**
 * Lightweight workflow-definition listings: names, versions and pages, so pickers and lists don't
 * download every definition body.
 *
 * - names:               GET /metadata/workflow/names
 * - names and versions:  GET /metadata/workflow/names-and-versions
 * - versions of one:     GET /metadata/workflow/{name}/versions
 * - one page (latest):   GET /metadata/workflow/latest-versions?start&size&filterField&filterValue&classifier
 * - one definition:      GET /metadata/workflow/{name}?version=
 */
import _sortBy from "lodash/sortBy";
import { fetchWithContext, useFetchContext } from "plugins/fetch";
import qs from "qs";
import { useCallback, useMemo, useRef } from "react";
import { UseQueryOptions, UseQueryResult, useQueryClient } from "react-query";
import { WorkflowDef } from "types/WorkflowDef";
import { WORKFLOW_METADATA_BASE_URL } from "utils/constants/api";
import { logger } from "utils/logger";
import {
  DEFAULT_STALE_TIME,
  STALE_TIME_WORKFLOW_DEFS,
  useAuthHeaders,
  useFetch,
} from "utils/query";

// as in utils/query: any Response-like object with a status
type FetchError = any;

export const WORKFLOW_NAMES_URL = `${WORKFLOW_METADATA_BASE_URL}/names`;
export const WORKFLOW_NAMES_AND_VERSIONS_URL = `${WORKFLOW_METADATA_BASE_URL}/names-and-versions`;
export const WORKFLOW_LATEST_VERSIONS_URL = `${WORKFLOW_METADATA_BASE_URL}/latest-versions`;
export const workflowVersionsUrl = (name: string) =>
  `${WORKFLOW_METADATA_BASE_URL}/${encodeURIComponent(name)}/versions`;
export const workflowDefVersionUrl = (name: string, version: string | number) =>
  `${WORKFLOW_METADATA_BASE_URL}/${encodeURIComponent(name)}?version=${version}`;

/** Version summary as returned by the names-and-versions and versions endpoints. */
export interface WorkflowDefSummary {
  name: string;
  version: number;
  createTime?: number;
}

/** A page of latest workflow definitions. */
export interface WorkflowDefPage {
  totalHits: number;
  results: WorkflowDef[];
}

export interface LatestWorkflowDefsPageParams {
  start: number;
  size: number;
  /** Case-insensitive substring of the workflow name; empty for no filter. */
  nameFilter?: string;
  classifier?: "workflow" | "agent";
}

export const latestWorkflowDefsPagePath = ({
  start,
  size,
  nameFilter,
  classifier,
}: LatestWorkflowDefsPageParams): string =>
  `${WORKFLOW_LATEST_VERSIONS_URL}${qs.stringify(
    {
      start,
      size,
      ...(nameFilter ? { filterField: "name", filterValue: nameFilter } : {}),
      ...(classifier ? { classifier } : {}),
    },
    { addQueryPrefix: true },
  )}`;

/** `{ name: [summaries] }` as name → ascending version numbers (names sorted). */
export const namesAndVersionsToMap = (
  data?: Record<string, WorkflowDefSummary[]> | null,
): Map<string, number[]> => {
  const result = new Map<string, number[]>();
  if (data && typeof data === "object") {
    Object.keys(data)
      .sort()
      .forEach((name) => {
        result.set(
          name,
          _sortBy((data[name] ?? []).map((summary) => summary.version)),
        );
      });
  }
  return result;
};

/** Workflow names, sorted. */
export function useWorkflowNames(
  optionsOverride: Partial<UseQueryOptions<string[], FetchError>> = {},
): string[] {
  const { data } = useFetch<string[]>(WORKFLOW_NAMES_URL, {
    staleTime: STALE_TIME_WORKFLOW_DEFS,
    ...optionsOverride,
  });
  return useMemo(() => (Array.isArray(data) ? data : []), [data]);
}

/** name → ascending version numbers, every version of every workflow. */
export function useWorkflowNamesAndVersions(): Map<string, number[]> {
  const { data } = useFetch<Record<string, WorkflowDefSummary[]>>(
    WORKFLOW_NAMES_AND_VERSIONS_URL,
    { staleTime: STALE_TIME_WORKFLOW_DEFS },
  );
  return useMemo(() => namesAndVersionsToMap(data), [data]);
}

/** One page of latest workflow definitions, paged and filtered by the server. */
export function useLatestWorkflowDefsPage(
  params: LatestWorkflowDefsPageParams,
  optionsOverride: Partial<UseQueryOptions<WorkflowDefPage, FetchError>> = {},
): UseQueryResult<WorkflowDefPage, FetchError> {
  return useFetch<WorkflowDefPage>(latestWorkflowDefsPagePath(params), {
    staleTime: DEFAULT_STALE_TIME,
    ...optionsOverride,
  });
}

/** Loads one version of a workflow definition (cached by react-query); undefined on failure. */
export function useWorkflowDefLoader(): (
  name?: string | null,
  version?: string | number | null,
) => Promise<WorkflowDef | undefined> {
  const fetchContext = useFetchContext();
  const headers = useAuthHeaders();
  const queryClient = useQueryClient();
  return useCallback(
    async (name, version) => {
      if (!name || version == null || version === "") {
        return undefined;
      }
      const path = workflowDefVersionUrl(name, version);
      try {
        return await queryClient.fetchQuery<WorkflowDef>(
          [fetchContext.stack, path],
          () => fetchWithContext(path, fetchContext, { headers }),
          { staleTime: STALE_TIME_WORKFLOW_DEFS },
        );
      } catch (error) {
        logger.error("Fetching workflow definition version", error);
        return undefined;
      }
    },
    [fetchContext, headers, queryClient],
  );
}

/**
 * Drop-in for `useWorkflowDefsByVersions` (same `Map{ lookups, values }` shape) that downloads only
 * names and versions. `values` starts empty; call the returned `loadVersion(name, version)` before
 * reading `values.get(name).get(version)` and it is filled with that one definition (or an empty
 * one when it can't be loaded, so readers never hit an undefined map).
 */
export function useWorkflowDefsByVersionsOnDemand(): [
  Map<string, any>,
  (name?: string | null, version?: string | null) => Promise<void>,
] {
  const namesAndVersions = useWorkflowNamesAndVersions();
  const loadWorkflowDef = useWorkflowDefLoader();
  const values = useRef(new Map<string, Map<string, any>>());

  const workflowDefByVersions = useMemo(() => {
    const lookups = new Map<string, string[]>();
    namesAndVersions.forEach((versions, name) => {
      lookups.set(
        name,
        versions.map((version) => version.toString()),
      );
    });
    return new Map<string, any>([
      ["lookups", lookups],
      ["values", values.current],
    ]);
  }, [namesAndVersions]);

  const loadVersion = useCallback(
    async (name?: string | null, version?: string | null) => {
      if (!name || version == null) {
        return;
      }
      const byVersion = values.current.get(name) ?? new Map<string, any>();
      values.current.set(name, byVersion);
      if (!byVersion.has(version)) {
        const def = await loadWorkflowDef(name, version);
        byVersion.set(version, def ?? { inputParameters: [] });
      }
    },
    [loadWorkflowDef],
  );

  return [workflowDefByVersions, loadVersion];
}
