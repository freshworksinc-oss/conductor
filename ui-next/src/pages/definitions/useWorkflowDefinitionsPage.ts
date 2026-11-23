import { useCallback, useEffect, useRef, useState } from "react";
import { WorkflowDef } from "types/WorkflowDef";
import { pluralizeResults } from "utils/helpers";
import useCustomPagination from "utils/hooks/useCustomPagination";
import { useLatestWorkflowDefsPage } from "utils/workflowDefLists";

const DEFAULT_ROWS_PER_PAGE = 15;
const SEARCH_DEBOUNCE_MS = 300;

/**
 * The workflow definitions list, paged and searched (by name) on the server: only the visible page
 * of latest definitions is downloaded. `data` is that page as an array, like the full list was;
 * spread `tableProps` on the DataTable (after its own props) to page through the server.
 */
export function useWorkflowDefinitionsPage(classifier: "workflow" | "agent") {
  const [{ pageParam, searchParam }, { handlePageChange }] =
    useCustomPagination();
  const [rowsPerPage, setRowsPerPage] = useState(DEFAULT_ROWS_PER_PAGE);
  const currentPage = pageParam ? Math.max(Number(pageParam) || 1, 1) : 1;

  // search by name on the server, debounced so typing doesn't send a request per key
  const [nameFilter, setNameFilter] = useState((searchParam ?? "").trim());
  useEffect(() => {
    const timer = setTimeout(
      () => setNameFilter((searchParam ?? "").trim()),
      SEARCH_DEBOUNCE_MS,
    );
    return () => clearTimeout(timer);
  }, [searchParam]);

  // a new search starts at the first page (not the first render, which may come from the URL)
  const previousNameFilter = useRef(nameFilter);
  useEffect(() => {
    if (previousNameFilter.current !== nameFilter) {
      previousNameFilter.current = nameFilter;
      handlePageChange(1);
    }
  }, [nameFilter, handlePageChange]);

  const { data, isFetching, refetch } = useLatestWorkflowDefsPage({
    start: (currentPage - 1) * rowsPerPage,
    size: rowsPerPage,
    nameFilter,
    classifier,
  });
  const results: WorkflowDef[] | undefined = data?.results;
  const totalHits = data?.totalHits ?? 0;

  const handleRowsPerPageChange = useCallback(
    (newRowsPerPage: number) => {
      setRowsPerPage(newRowsPerPage);
      handlePageChange(1);
    },
    [handlePageChange],
  );

  const tableProps = {
    title: `${results?.length ?? 0} of ${pluralizeResults(totalHits)}`,
    sortByDefault: false, // the server sorts by name
    paginationServer: true,
    paginationTotalRows: totalHits,
    paginationPerPage: rowsPerPage,
    useGlobalRowsPerPage: false,
    onChangeRowsPerPage: handleRowsPerPageChange,
    // onChangePage / paginationDefaultPage: the page already binds them to the same URL param
  };

  return { data: results, isFetching, refetch, tableProps };
}
