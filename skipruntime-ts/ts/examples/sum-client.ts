import { run, type Step, type Table, subscribe } from "./utils.js";

function scenarios() {
  return [
    [
      {
        type: "write",
        payload: [{ collection: "input1", entries: [{ key: "v1", value: 2 }] }],
      },
      {
        type: "write",
        payload: [{ collection: "input2", entries: [{ key: "v1", value: 3 }] }],
      },
      {
        type: "request",
        payload: "/add/v1",
      },
      {
        type: "delete",
        payload: [{ collection: "input1", keys: ["v1"] }],
      },
      {
        type: "write",
        payload: [
          {
            collection: "input1",
            entries: [
              { key: "v1", value: 2 },
              { key: "v2", value: 6 },
            ],
          },
        ],
      },
      {
        type: "write",
        payload: [{ collection: "input2", entries: [{ key: "v2", value: 0 }] }],
      },
      {
        type: "write",
        payload: [{ collection: "input1", entries: [{ key: "v1", value: 8 }] }],
      },
      {
        type: "request",
        payload: "/add/v1",
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
