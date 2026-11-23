import {
  latestWorkflowDefsPagePath,
  namesAndVersionsToMap,
  workflowDefVersionUrl,
  workflowVersionsUrl,
} from "./workflowDefLists";

describe("namesAndVersionsToMap", () => {
  it("maps names-and-versions to sorted names with ascending version numbers", () => {
    const map = namesAndVersionsToMap({
      b_flow: [
        { name: "b_flow", version: 3 },
        { name: "b_flow", version: 1 },
      ],
      a_flow: [{ name: "a_flow", version: 2 }],
    });

    expect(Array.from(map.keys())).toEqual(["a_flow", "b_flow"]);
    expect(map.get("b_flow")).toEqual([1, 3]);
    expect(map.get("a_flow")).toEqual([2]);
  });

  it("is empty for a missing or empty response", () => {
    expect(namesAndVersionsToMap(undefined).size).toBe(0);
    expect(namesAndVersionsToMap(null).size).toBe(0);
    expect(namesAndVersionsToMap({}).size).toBe(0);
  });
});

describe("latestWorkflowDefsPagePath", () => {
  it("asks for one page, with the name filter and classifier when given", () => {
    expect(
      latestWorkflowDefsPagePath({
        start: 30,
        size: 15,
        nameFilter: "bill ing",
        classifier: "workflow",
      }),
    ).toBe(
      "/metadata/workflow/latest-versions?start=30&size=15&filterField=name&filterValue=bill%20ing&classifier=workflow",
    );
  });

  it("leaves out an empty filter and classifier", () => {
    expect(latestWorkflowDefsPagePath({ start: 0, size: 15 })).toBe(
      "/metadata/workflow/latest-versions?start=0&size=15",
    );
  });
});

describe("version URLs", () => {
  it("encodes the workflow name", () => {
    expect(workflowVersionsUrl("a/b c")).toBe(
      "/metadata/workflow/a%2Fb%20c/versions",
    );
    expect(workflowDefVersionUrl("a/b", 2)).toBe(
      "/metadata/workflow/a%2Fb?version=2",
    );
  });
});
