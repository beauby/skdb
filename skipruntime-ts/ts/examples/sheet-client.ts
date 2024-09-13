import { run, type Step, type Table, subscribe } from "./utils.js";

function scenarios() {
  return [
    [
      {
        type: "write",
        payload: [
          {
            collection: "cells",
            entries: [
              { key: "A1", value: "23" },
              { key: "A2", value: "2" },
            ],
          },
        ],
      },
      {
        type: "write",
        payload: [
          {
            collection: "cells",
            entries: [{ key: "A3", value: "=A1 + A2" }],
          },
        ],
      },
      {
        type: "write",
        payload: [
          {
            collection: "cells",
            entries: [{ key: "A1", value: "5" }],
          },
        ],
      },
      {
        type: "write",
        payload: [
          {
            collection: "cells",
            entries: [{ key: "A4", value: "=A3 * A2" }],
          },
        ],
      },
      {
        type: "delete",
        payload: [
          {
            collection: "cells",
            keys: ["A3"],
          },
        ],
      },
    ] as Step[],
  ];
}

if (process?.argv?.length > 2 && process?.argv[2] == "true") {
  await subscribe(
    (rows: Table) => {
      console.table(rows);
    },
    (added: Table, removed: Table) => {
      console.log("Added");
      console.table(added);
      console.log("Removed");
      console.table(removed);
    },
  );
}

run(scenarios());
