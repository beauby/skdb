import type {
  EagerCollection,
  InputMapper,
  Schema,
  OutputMapper,
  Param,
  SKStore,
  TJSON,
  Database,
} from "./skipruntime_api.js";

export interface Writer {
  set(key: string, value: TJSON): void;
  delete(keys: string[]): void;
}

export type ServiceOutput = {
  outputs: Record<string, EagerCollection<TJSON, TJSON>>;
  observables?: Record<string, EagerCollection<TJSON, TJSON>>;
};

export type SimpleServiceOutput = {
  output: EagerCollection<string, TJSON>;
  observables?: Record<string, EagerCollection<TJSON, TJSON>>;
};

export type InputDefinition = {
  schema: Schema;
  fromTableRow: new (...params: Param[]) => InputMapper<TJSON[], TJSON, TJSON>;
  params: Param[];
};

export type RemoteInputs = {
  database: Database;
  inputs: Record<string, InputDefinition>;
};

export type OutputDefinition = {
  schema: Schema;
  toTableRow: new (...params: Param[]) => OutputMapper<TJSON[], TJSON, TJSON>;
  params: Param[];
};

export interface GenericSkipService {
  // name / duration in milliseconds
  tokens?: Record<string, number>;
  localInputs(): Record<string, InputDefinition>;
  remoteInputs(): Record<string, RemoteInputs>;

  outputs(): Record<string, OutputDefinition>;

  reactiveCompute(
    store: SKStore,
    inputs: Record<string, EagerCollection<TJSON, TJSON>>,
  ): ServiceOutput;
}

export type SimpleRemoteInputs = {
  database: Database;
  inputs: string[];
};

export interface SimpleSkipService {
  inputTables?: string[];
  remoteTables?: Record<string, SimpleRemoteInputs>;
  // name / duration in milliseconds
  tokens?: Record<string, number>;

  reactiveCompute(
    store: SKStore,
    requests: EagerCollection<string, TJSON>,
    inputCollections: Record<string, EagerCollection<string, TJSON>>,
  ): SimpleServiceOutput;
}
