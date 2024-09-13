import type {
  CollectionAccess,
  Database,
  EagerCollection,
  JSONObject,
  NonEmptyIterator,
  Opt,
  OutputMapper,
  Remote,
  Schema,
  SKStore,
  Table,
  TableCollection,
  TableReader,
  TJSON,
} from "./skipruntime_api.js";

import type {
  GenericSkipService,
  InputDefinition,
  RemoteInputs,
  SimpleSkipService,
  Writer,
} from "./skipruntime_service.js";

import { ctext as text, cjson as json } from "./skipruntime_utils.js";

import {
  type createSKStore as CreateSKStore,
  type InputMapper,
} from "./skip-runtime.js";
import type { TableCollectionImpl } from "./internals/skipruntime_impl.js";

class FromInput implements InputMapper<TJSON[], TJSON, TJSON> {
  mapElement(entry: TJSON[], _occ: number): Iterable<[TJSON, TJSON]> {
    return Array([entry[0], entry[1]]);
  }
}

class ToOutput implements OutputMapper<TJSON[], TJSON, TJSON> {
  mapElement(key: TJSON, it: NonEmptyIterator<TJSON>) {
    const v = it.first();
    return Array([key, v, "root", "read-write"]);
  }
}

const requestSchema = {
  name: "__sk_requests",
  columns: [
    text("id", false, true),
    json("request"),
    text("skdb_author"),
    text("skdb_access"),
  ],
};

function inputSchema(name: string) {
  return {
    name,
    columns: [
      text("key", false, true),
      json("value"),
      text("skdb_author"),
      text("skdb_access"),
    ],
  };
}

const responseSchema = {
  name: "__sk_responses",
  columns: [
    text("id", false, true),
    json("response"),
    text("skdb_author"),
    text("skdb_access"),
  ],
};

class WriterImpl implements Writer {
  constructor(private table: Table<TJSON[]>) {}

  set(key: string, value: TJSON): void {
    this.table.insert(
      [[key, JSON.stringify(value), "root", "read-write"]],
      true,
    );
  }

  delete(keys: string[]): void {
    this.table.deleteWhere({ key: keys });
  }
}

class TReaderImpl implements CollectionAccess<TJSON, TJSON> {
  constructor(private table: TableReader) {}

  getAll(): { key: TJSON; value: TJSON }[] {
    const cols =
      this.table.getName() == "__sk_responses"
        ? ["id", "response"]
        : ["key", "value"];
    return this.table.select({}, cols) as {
      key: TJSON;
      value: TJSON;
    }[];
  }

  getArray(key: string): TJSON[] {
    const where: JSONObject = {};
    const cols: string[] = [];
    let vName: keyof JSONObject = "value";
    if (this.table.getName() == "__sk_responses") {
      vName = "";
      where["id" as keyof JSONObject] = key;
      cols.push("response");
      vName = "response";
    } else {
      where["key"] = key;
      cols.push("value");
    }
    const result = this.table.select(where, cols);
    if (result.length > 0) {
      const row = result[0];
      return [row[vName]!];
    }
    return [];
  }

  getOne(key: string): TJSON {
    const value = this.maybeGetOne(key);
    if (!value) {
      throw new Error(`'${key}' not found`);
    }
    return value;
  }

  maybeGetOne(key: string): Opt<TJSON> {
    const where: JSONObject = {};
    const cols: string[] = [];
    let kName: keyof JSONObject = "key";
    let vName: keyof JSONObject = "value";
    if (this.table.getName() == "__sk_responses") {
      where["id"] = key;
      cols.push("id");
      cols.push("response");
      vName = "response";
      kName = "id";
    } else {
      where["key"] = key;
      cols.push("key");
      cols.push("value");
    }
    const result = this.table.select(where, cols);
    if (result.length > 0) {
      const row = result[0];
      return row[vName];
    }
    return null;
  }
}

function toWriters(
  tables: Record<string, Table<TJSON[]>>,
): Record<string, Writer> {
  const writers: Record<string, Writer> = {};
  for (const key of Object.keys(tables)) {
    writers[key] = new WriterImpl(tables[key]);
  }
  return writers;
}

function toReaders(
  tables: Record<string, TableReader>,
): Record<string, CollectionAccess<TJSON, TJSON>> {
  const readers: Record<string, CollectionAccess<TJSON, TJSON>> = {};
  for (const key of Object.keys(tables)) {
    readers[key] = new TReaderImpl(tables[key]);
  }
  return readers;
}

class SimpleToGenericSkipService implements GenericSkipService {
  tokens?: Record<string, number>;

  constructor(private simple: SimpleSkipService) {
    if (simple.tokens) this.tokens = simple.tokens;
  }

  localInputs() {
    const inputs: Record<string, InputDefinition> = {};
    if (this.simple.inputTables) {
      this.simple.inputTables.map((table) => {
        inputs[table] = {
          schema: inputSchema(table),
          fromTableRow: FromInput,
          params: [],
        };
      });
    }
    return {
      __sk_requests: {
        schema: requestSchema,
        fromTableRow: FromInput,
        params: [],
      },
      ...inputs,
    };
  }

  remoteInputs(): Record<string, RemoteInputs> {
    const inputs: Record<string, RemoteInputs> = {};
    if (this.simple.remoteTables) {
      for (const [key, sri] of Object.entries(this.simple.remoteTables)) {
        const rInputs: Record<string, InputDefinition> = {};
        sri.inputs.map((table) => {
          rInputs[table] = {
            schema: inputSchema(table),
            fromTableRow: FromInput,
            params: [],
          };
        });
        inputs[key] = {
          database: sri.database,
          inputs: rInputs,
        };
      }
    }
    return inputs;
  }

  outputs() {
    return {
      __sk_responses: {
        schema: responseSchema,
        toTableRow: ToOutput,
        params: [],
      },
    };
  }

  reactiveCompute(
    store: SKStore,
    inputs: Record<string, EagerCollection<TJSON, TJSON>>,
  ) {
    const requests = inputs["__sk_requests"];
    delete inputs["__sk_requests"];
    const result = this.simple.reactiveCompute(store, requests, inputs);
    return {
      outputs: { __sk_responses: result.output },
      observables: result.observables,
    };
  }
}

export async function runService(
  gService: GenericSkipService,
  createSKStore: typeof CreateSKStore,
  database?: Database,
): Promise<
  [
    Record<string, Table<TJSON[]>>,
    Record<string, TableReader>,
    Record<string, CollectionAccess<TJSON, TJSON>>,
  ]
> {
  const localInputs = gService.localInputs();
  const remoteInputs = gService.remoteInputs();
  const outputs = gService.outputs();
  const schemas: Schema[] = [];
  for (const [key, value] of Object.entries(localInputs)) {
    const schema = value.schema;
    if (schema.name != key) {
      schema.alias = key;
    }
    schemas.push(schema);
  }
  for (const [key, value] of Object.entries(outputs)) {
    const schema = value.schema;
    if (schema.name != key) {
      schema.alias = key;
    }
    schemas.push(schema);
  }
  const remotes: Record<string, Remote> = {};
  for (const [key, ri] of Object.entries(remoteInputs)) {
    const schemas: Schema[] = [];
    for (const [key, value] of Object.entries(ri.inputs)) {
      const schema = value.schema;
      if (schema.name != key) {
        schema.alias = key;
      }
      schemas.push(schema);
    }
    remotes[key] = { database: ri.database, tables: schemas };
  }
  const iTables: Record<string, Table<TJSON[]>> = {};
  const oTables: Record<string, TableReader> = {};
  const readers: Record<string, CollectionAccess<TJSON, TJSON>> = {};
  const initSKStore = (
    store: SKStore,
    tables: Record<string, TableCollection<TJSON[]>>,
  ) => {
    const iHandles: Record<string, EagerCollection<TJSON, TJSON>> = {};
    for (const [key, value] of Object.entries(localInputs)) {
      const table = tables[key];
      // eslint-disable-next-line  @typescript-eslint/no-unsafe-argument
      iHandles[key] = table.map(value.fromTableRow, ...value.params);
      iTables[key] = (table as TableCollectionImpl<TJSON[]>).toTable();
    }
    for (const remove of Object.values(remoteInputs)) {
      for (const [key, value] of Object.entries(remove.inputs)) {
        const table = tables[key];
        // eslint-disable-next-line  @typescript-eslint/no-unsafe-argument
        iHandles[key] = table.map(value.fromTableRow, ...value.params);
      }
    }
    const result = gService.reactiveCompute(store, iHandles);
    const rkeys = Object.keys(result.outputs);
    const okeys = Object.keys(outputs);
    if (rkeys.length != okeys.length) {
      throw new Error(
        `The number of output collections (${rkeys.length})` +
          ` must correspond to number of output tables (${okeys.length}).`,
      );
    }
    for (const [key, output] of Object.entries(outputs)) {
      const ehandle = result.outputs[key];
      // eslint-disable-next-line  @typescript-eslint/no-unnecessary-condition
      if (!ehandle) {
        throw new Error(`The ${key} must be return by reactiveCompute.`);
      }
      const table = tables[key];
      // eslint-disable-next-line  @typescript-eslint/no-unnecessary-condition
      if (!table) {
        throw new Error(`Unable to retrieve ${key} table.`);
      }
      oTables[key] = (table as TableCollectionImpl<TJSON[]>).toTable();
      // eslint-disable-next-line  @typescript-eslint/no-unsafe-argument
      ehandle.mapTo(tables[key], output.toTableRow, ...output.params);
    }
    if (result.observables) {
      for (const [key, observable] of Object.entries(result.observables)) {
        readers[key] = observable.toCollectionAccess();
      }
    }
  };
  await createSKStore(
    initSKStore,
    database
      ? {
          tables: schemas,
          database,
        }
      : { tables: schemas },
    remotes,
    gService.tokens,
  );
  return [iTables, oTables, readers];
}

export async function runSimpleService(
  service: SimpleSkipService,
  createSKStore: typeof CreateSKStore,
  database?: Database,
): Promise<
  [Record<string, Writer>, Record<string, CollectionAccess<TJSON, TJSON>>]
> {
  const gService = new SimpleToGenericSkipService(service as SimpleSkipService);
  const [iTables, oTables, readers] = await runService(
    gService,
    createSKStore,
    database,
  );
  return [toWriters(iTables), { ...toReaders(oTables), ...readers }];
}
