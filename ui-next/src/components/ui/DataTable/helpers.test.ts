import {
  defaultFilterItemsSorter,
  dynamicSort,
  formatForColumn,
} from "./helpers";
import { ColumnCustomType, LegacyColumn } from "./types";

const cases = [
  {
    name: "A greater than B",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "My Test integration",
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "target.type",
    expected: 1,
  },
  {
    name: "A lesser than B",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "target.id",
    expected: -1,
  },
  {
    name: "A equal B",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
        number: 9,
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
        number: 9,
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "target.number",
    expected: 0,
  },
  {
    name: "A and B have undefined value",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
        number: 9,
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
        number: 9,
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "target.someProp.a.b.c",
    expected: 0,
  },
  {
    name: "A and B are arrays and have different length",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
        number: 9,
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
        number: 9,
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "access",
    expected: 1,
  },
  {
    name: "A and B are different objects",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
        number: 9,
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
        number: 9,
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
    },
    propertyPath: "target",
    expected: 1,
  },
  {
    name: "A and B are equal objects",
    objA: {
      target: {
        type: "INTEGRATION_PROVIDER",
        id: "0_My Test integration",
        number: 9,
      },
      access: ["UPDATE", "READ", "EXECUTE", "CREATE"],
      tag: "test:test",
      equal: {
        a: 1,
        b: 2,
      },
    },
    objB: {
      target: {
        type: "APPLICATION",
        id: "app:8ee1b276-28a1-443c-b5a4-43892f87e222",
        number: 9,
      },
      access: ["READ", "CREATE"],
      tag: "auto:test",
      equal: {
        a: 1,
        b: 2,
      },
    },
    propertyPath: "equal",
    expected: 0,
  },
];

describe("Compare 2 objects for sorting", () => {
  test.each(cases)(
    "testing '$name', returns $expected",
    ({ objA, objB, propertyPath, expected }) => {
      const result = dynamicSort({ objA, objB, propertyPath });

      expect(result).toEqual(expected);
    },
  );
});

describe("defaultFilterItemsSorter", () => {
  test("does not throw when a row is null", () => {
    const items = [
      { taskId: "a", updateTime: 2 },
      null,
      { taskId: "b", updateTime: 1 },
    ];

    expect(() => defaultFilterItemsSorter([...items])).not.toThrow();
    const sorted = defaultFilterItemsSorter([...items]);
    expect(sorted[0]?.taskId).toBe("a");
    expect(sorted[1]?.taskId).toBe("b");
    expect(sorted[2]).toBeNull();
  });

  test("still sorts by updateTime descending when rows are valid", () => {
    const items = [
      { updateTime: 1 },
      { updateTime: 3 },
      { updateTime: 2 },
    ];
    const sorted = defaultFilterItemsSorter([...items]);

    expect(sorted.map((row) => row.updateTime)).toEqual([3, 2, 1]);
  });
});

describe("formatForColumn", () => {
  const booleanColumn = {
    id: "restartable",
    name: "restartable",
    label: "Restartable",
    type: ColumnCustomType.BOOLEAN,
  } as LegacyColumn;

  test.each([
    { value: true, expected: "Yes" },
    { value: false, expected: "No" },
  ])("labels $value as $expected", ({ value, expected }) => {
    const format = formatForColumn(booleanColumn);

    expect(format({ restartable: value }, 0)).toEqual(expected);
  });

  test("leaves a missing boolean value untouched", () => {
    const format = formatForColumn(booleanColumn);

    expect(format({}, 0)).toBeUndefined();
  });

  test("prefers an explicit renderer over the boolean label", () => {
    const format = formatForColumn({
      ...booleanColumn,
      renderer: (value: boolean) => (value ? "hit" : "miss"),
    } as LegacyColumn);

    expect(format({ restartable: true }, 0)).toEqual("hit");
  });
});
