import type { SimpleSkipService } from "../skipruntime_service.js";
import type { createSKStore as CreateSKStore } from "../skip-runtime.js";
import type { Database, EntryPoint, TJSON } from "../skipruntime_api.js";
import express from "express";

import { runSimpleService } from "../skipruntime_runner.js";

async function getCreds(database: string, entryPoint: EntryPoint) {
  try {
    const creds = new Map();
    const resp = await fetch(
      `http${entryPoint.secured ? "s" : ""}://${entryPoint.host}:${
        entryPoint.port
      }/dbs/${database}/users`,
    );
    const data = await resp.text();
    const users = data
      .split("\n")
      .filter((line) => line.trim() != "")
      .map((line) => JSON.parse(line));
    for (const user of users) {
      creds.set(user.accessKey, user.privateKey);
    }
    return creds;
  } catch (ex: any) {
    return null;
  }
}

async function checkCreds(
  database: string,
  user: string,
  entryPoint: EntryPoint,
  retry = 10,
) {
  var count = 0;
  const waitandcheck = (
    resolve: (pk: string) => void,
    reject: (reason?: any) => void,
  ) => {
    if (count == retry)
      reject("Could not fetch from the dev server, is it running?");
    else {
      count++;
      getCreds(database, entryPoint).then((creds) => {
        if (creds != null) {
          const pk = creds.get(user);
          if (pk != null) {
            resolve(pk);
          } else {
            reject(`Unable to find ${user} credential`);
          }
        } else {
          setTimeout(waitandcheck, 500, resolve, reject);
        }
      });
    }
  };
  return new Promise(waitandcheck);
}

type Entry = {
  key: string;
  value: TJSON;
};

type Write = {
  collection: string;
  entries: Entry[];
};

type Delete = {
  collection: string;
  keys: string[];
};

function isEntry(value: TJSON) {
  if (typeof value != "object") return false;
  if (!("key" in value) || typeof value["key"] != "string") return false;
  if (!("value" in value)) return false;
  return true;
}

function isWrite(value: TJSON) {
  if (typeof value != "object") return false;
  if (!("collection" in value) || typeof value["collection"] != "string")
    return false;
  if (!("entries" in value) || !Array.isArray(value["entries"])) return false;
  for (const entry of value["entries"]) {
    if (entry == null || !isEntry(entry)) {
      return false;
    }
  }
  return true;
}

function isDelete(value: TJSON) {
  if (typeof value != "object") return false;
  if (!("collection" in value) || typeof value["collection"] != "string")
    return false;
  if (!("keys" in value) || !Array.isArray(value["keys"])) return false;
  for (const entry of value["keys"]) {
    if (entry == null || typeof entry != "string") {
      return false;
    }
  }
  return true;
}

export async function runWithRESTServer_(
  service: SimpleSkipService,
  createSKStore: typeof CreateSKStore,
  options: Record<string, any>,
) {
  let database: Database | undefined = undefined;
  if (process.argv.length > 2) {
    const port = parseInt(process.argv[2]);
    if (!isNaN(port)) {
      const name = "skstore";
      try {
        const pvalue = await checkCreds(name, "root", {
          host: "localhost",
          port,
        });
        database = {
          name,
          access: "root",
          private: pvalue,
          endpoint: { host: "localhost", port },
        };
      } catch (e) {
        throw e;
      }
    }
  }
  // eslint-disable-next-line  @typescript-eslint/no-unsafe-call
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  const [inputs, outputs] = await runSimpleService(
    service,
    createSKStore,
    database,
  );
  // READS
  app.get("/v1/one/:collection/:id", (req, res) => {
    const id = decodeURIComponent(req.params.id);
    let key: TJSON = id;
    try {
      key = JSON.parse(id);
    } catch (_e: any) {
      // Nothing To do string key
    }
    const collectionName = req.params.collection;
    if (collectionName != "__query") {
      const collection = outputs[collectionName];
      if (!collection) {
        res.status(400).json("Bad request");
      }
      try {
        const data = collection.getOne(key);
        res.status(200).json(data);
      } catch (ex: any) {
        res.status(500).json(ex.message);
      }
    } else {
      // temporary before session management
      inputs["__sk_requests"].set(key as string, key);
      try {
        const data = outputs["__sk_responses"].getOne(key);
        res.status(200).json(data);
      } catch (ex: any) {
        res.status(500).json(ex.message);
      }
    }
  });
  app.get("/v1/array/:collection/:id", (req, res) => {
    const id = decodeURIComponent(req.params.id);
    let key: TJSON = id;
    try {
      key = JSON.parse(id);
    } catch (_e: any) {
      // Nothing To do string key
    }
    const collectionName = req.params.collection;
    const collection = outputs[collectionName];
    if (!collection) {
      res.status(400).json("Bad request");
      return;
    }
    try {
      const data = collection.getArray(key);
      res.status(200).json(data);
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  app.get("/v1/:collection", (req, res) => {
    const collectionName = req.params.collection;
    const collection = outputs[collectionName];
    if (!collection) {
      res.status(400).json("Bad request");
      return;
    }
    try {
      const data = collection.getAll();
      res.status(200).json(data);
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  // WRITES
  app.post("/v1/many", (req, res) => {
    const data: TJSON = req.body;
    if (!Array.isArray(data)) {
      res.status(400).json("Bad request");
      return;
    }
    const todo: (() => void)[] = [];
    for (const write of data) {
      if (write == null || !isWrite(write)) {
        res.status(400).json("Bad request");
        return;
      }
      const collectionName = (write as Write).collection;
      const collection = inputs[collectionName];
      if (!collection) {
        res.status(400).json("Bad request");
        return;
      }
      todo.push(() => {
        for (const entry of (write as Write).entries) {
          collection.set(entry.key, entry.value);
        }
      });
    }
    try {
      for (const action of todo) {
        action();
      }
      res.status(200).json({});
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  app.post("/v1/:collection/:id", (req, res) => {
    const key = req.params.id;
    const data: TJSON = req.body;
    const collectionName = req.params.collection;
    const collection = inputs[collectionName];
    if (!collection) {
      res.status(400).json("Bad request");
      return;
    }
    try {
      collection.set(key, data);
      res.status(200).json({});
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  // DELETE
  app.delete("/v1/:collection/:id", (req, res) => {
    const key = req.params.id;
    const collectionName = req.params.collection;
    const collection = inputs[collectionName];
    if (!collection) {
      res.status(400).json("Bad request");
      return;
    }
    try {
      collection.delete([key]);
      res.status(200).json({});
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  app.delete("/v1/many", (req, res) => {
    const data: TJSON = req.body;
    if (!Array.isArray(data)) {
      res.status(400).json("Bad request");
      return;
    }
    const todo: (() => void)[] = [];
    for (const d of data) {
      if (d == null || !isDelete(d)) {
        res.status(400).json("Bad request");
        return;
      }
      const collectionName = (d as Delete).collection;
      const collection = inputs[collectionName];
      if (!collection) {
        res.status(400).json("Bad request");
        return;
      }
      todo.push(() => {
        collection.delete((d as Delete).keys);
      });
    }
    try {
      for (const action of todo) {
        action();
      }
      res.status(200).json({});
    } catch (ex: any) {
      res.status(500).json(ex.message);
    }
  });
  const httpPort = "port" in options ? options["port"] : 3587;
  app.listen(httpPort, () => {
    console.log(`Serve at http://localhost:${httpPort}`);
  });
}
