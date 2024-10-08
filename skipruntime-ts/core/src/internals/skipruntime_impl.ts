import { type int, type ptr, type Opt, cloneIfProxy } from "std";
import type { Context } from "./skipruntime_types.js";
import type * as Internal from "./skipruntime_internal_types.js";
import type {
  Accumulator,
  EagerCollection,
  LazyCollection,
  AsyncLazyCollection,
  Mapper,
  SKStore,
  SKStoreFactory,
  Loadable,
  JSONObject,
  TJSON,
  Param,
  LazyCompute,
  AsyncLazyCompute,
  NonEmptyIterator,
  EntryPoint,
  ExternalCall,
  CollectionReader,
  SkipRuntime,
  Entry,
  CollectionWriter,
  CallResourceCompute,
  Watermarked,
  Notifier,
  ReactiveResponse,
  SkipBuilder,
} from "../skipruntime_api.js";
import { UnknownCollectionError } from "../skipruntime_errors.js";

function assertNoKeysNaN<K extends TJSON, V extends TJSON>(
  kv_pairs: Iterable<[K, V]>,
): Iterable<[K, V]> {
  for (const [k, _v] of kv_pairs) {
    if (Number.isNaN(k)) {
      // NaN is forbidden since it breaks comparison reflexivity, ordering, etc.
      throw new Error("NaN is forbidden as a Skip collection key");
    }
  }
  return kv_pairs;
}
export const serverResponseSuffix = "__skdb_mirror_feedback";

export const sk_frozen: unique symbol = Symbol();

export interface Constant {
  [sk_frozen]: true;
}

function sk_freeze<T extends object>(x: T): T & Constant {
  return Object.defineProperty(x, sk_frozen, {
    enumerable: false,
    writable: false,
    value: true,
  }) as T & Constant;
}

function isSkFrozen(x: any): x is Constant {
  return sk_frozen in x && x[sk_frozen] === true;
}

abstract class SkFrozen implements Constant {
  // tsc misses that Object.defineProperty in the constructor inits this
  [sk_frozen]!: true;

  constructor() {
    sk_freeze(this);
    // Inheriting classes should call Object.freeze at the end of their
    // constructor
  }
}

export class EagerCollectionImpl<K extends TJSON, V extends TJSON>
  extends SkFrozen
  implements EagerCollection<K, V>
{
  constructor(
    private context: Context,
    private eagerHdl: string,
  ) {
    super();
    Object.freeze(this);
  }

  getId(): string {
    return this.eagerHdl;
  }

  getArray(key: K): V[] {
    return this.context.getArray(this.eagerHdl, key);
  }

  getOne(key: K): V {
    return this.context.getOne(this.eagerHdl, key);
  }

  maybeGetOne(key: K): Opt<V> {
    return this.context.maybeGetOne(this.eagerHdl, key);
  }

  protected derive<K2 extends TJSON, V2 extends TJSON>(
    eagerHdl: string,
  ): EagerCollection<K2, V2> {
    return new EagerCollectionImpl<K2, V2>(this.context, eagerHdl);
  }

  size = () => {
    return this.context.size(this.eagerHdl);
  };

  slice(ranges: readonly [K, K][]): EagerCollection<K, V> {
    const sliceName =
      "range_" +
      ranges
        .map(
          ([k1, k2]) =>
            `${this.context.keyOfJSON(k1)}--${this.context.keyOfJSON(k2)}`,
        )
        .join("_");
    const eagerHdl = this.context.slice(this.eagerHdl, sliceName, ranges);
    return this.derive<K, V>(eagerHdl);
  }

  take(limit: int): EagerCollection<K, V> {
    const limitName = `limit_${limit}`;
    const eagerHdl = this.context.take(this.eagerHdl, limitName, limit);
    return this.derive<K, V>(eagerHdl);
  }

  map<K2 extends TJSON, V2 extends TJSON, Params extends Param[]>(
    mapper: new (...params: Params) => Mapper<K, V, K2, V2>,
    ...params: Params
  ): EagerCollection<K2, V2> {
    params.forEach(check);
    const mapperObj = new mapper(...params);
    Object.freeze(mapperObj);
    if (!mapperObj.constructor.name) {
      throw new Error("Mapper classes must be defined at top-level.");
    }
    const eagerHdl = this.context.map(
      this.eagerHdl,
      mapperObj.constructor.name,
      (key: K, it: NonEmptyIterator<V>) =>
        assertNoKeysNaN(mapperObj.mapElement(key, it)),
    );
    return this.derive<K2, V2>(eagerHdl);
  }

  mapReduce<
    K2 extends TJSON,
    V2 extends TJSON,
    V3 extends TJSON,
    Params extends Param[],
  >(
    mapper: new (...params: Params) => Mapper<K, V, K2, V2>,
    accumulator: Accumulator<V2, V3>,
    ...params: Params
  ) {
    params.forEach(check);
    const mapperObj = new mapper(...params);
    Object.freeze(mapperObj);
    if (!mapperObj.constructor.name) {
      throw new Error("Mapper classes must be defined at top-level.");
    }
    const eagerHdl = this.context.mapReduce(
      this.eagerHdl,
      mapperObj.constructor.name,
      (key: K, it: NonEmptyIterator<V>) =>
        assertNoKeysNaN(mapperObj.mapElement(key, it)),
      accumulator,
    );
    return this.derive<K2, V3>(eagerHdl);
  }

  merge(...others: EagerCollection<K, V>[]): EagerCollection<K, V> {
    if (others.length == 0) return this;
    const collections = [this, ...others];
    const eagerHdl = this.context.merge(collections);
    return new EagerCollectionImpl<K, V>(this.context, eagerHdl);
  }

  toCollectionReader(): CollectionReader<K, V> {
    return new EagerCollectionReader<K, V>(this.context, this.eagerHdl);
  }

  toCollectionWriter(): CollectionWriter<K, V> {
    return new EagerCollectionWriter<K, V>(this.context, this.eagerHdl);
  }
}

class LazyCollectionImpl<K extends TJSON, V extends TJSON>
  extends SkFrozen
  implements LazyCollection<K, V>
{
  constructor(
    protected context: Context,
    protected lazyHdl: string,
  ) {
    super();
    Object.freeze(this);
  }

  getArray(key: K): V[] {
    return this.context.getArrayLazy(this.lazyHdl, key);
  }

  getOne(key: K): V {
    return this.context.getOneLazy(this.lazyHdl, key);
  }

  maybeGetOne(key: K): Opt<V> {
    return this.context.maybeGetOneLazy(this.lazyHdl, key);
  }
}

export class LSelfImpl<K extends TJSON, V extends TJSON>
  extends SkFrozen
  implements LazyCollection<K, V>
{
  constructor(
    protected context: Context,
    protected lazyHdl: ptr<Internal.LHandle>,
  ) {
    super();
    Object.freeze(this);
  }

  getArray(key: K): V[] {
    return this.context.getArraySelf(this.lazyHdl, key);
  }

  getOne(key: K): V {
    return this.context.getOneSelf(this.lazyHdl, key);
  }

  maybeGetOne(key: K): Opt<V> {
    return this.context.maybeGetOneSelf(this.lazyHdl, key);
  }
}

export class SKStoreImpl extends SkFrozen implements SKStore {
  constructor(private context: Context) {
    super();
    Object.freeze(this);
  }

  lazy<K extends TJSON, V extends TJSON, Params extends Param[]>(
    compute: new (...params: Params) => LazyCompute<K, V>,
    ...params: Params
  ): LazyCollection<K, V> {
    params.forEach(check);
    const computeObj = new compute(...params);
    Object.freeze(computeObj);
    const name = computeObj.constructor.name;
    const lazyHdl = this.context.lazy(
      name,
      computeObj.compute.bind(computeObj),
    );
    return new LazyCollectionImpl<K, V>(this.context, lazyHdl);
  }

  asyncLazy<
    K extends TJSON,
    V extends TJSON,
    P extends TJSON,
    M extends TJSON,
    Params extends Param[],
  >(
    compute: new (...params: Params) => AsyncLazyCompute<K, V, P, M>,
    ...params: Params
  ): AsyncLazyCollection<K, V, M> {
    params.forEach(check);
    const computeObj = new compute(...params);
    const name = computeObj.constructor.name;
    Object.freeze(computeObj);
    const lazyHdl = this.context.asyncLazy<K, V, P, M>(
      name,
      (key: K) => computeObj.params(key),
      (key: K, params: P) => computeObj.call(key, params),
    );
    return new LazyCollectionImpl<K, Loadable<V, M>>(this.context, lazyHdl);
  }

  external<
    K extends TJSON,
    V extends TJSON,
    Metadata extends TJSON,
    Params extends Param[],
  >(
    refreshToken: string,
    compute: new (...params: Params) => ExternalCall<K, V, Metadata>,
    ...params: Params
  ): AsyncLazyCollection<K, V, Metadata> {
    params.forEach(check);
    const computeObj = new compute(...params);
    const name = computeObj.constructor.name;
    Object.freeze(computeObj);
    const lazyHdl = this.context.asyncLazy<K, V, number, TJSON>(
      name,
      (_key: K) => this.getRefreshToken(refreshToken),
      (key: K, timestamp: number) => computeObj.call(key, timestamp),
    );
    return new LazyCollectionImpl<K, Loadable<V, Metadata>>(
      this.context,
      lazyHdl,
    );
  }

  getRefreshToken(key: string) {
    return this.context.getToken(key);
  }

  jsonExtract(value: JSONObject, pattern: string): TJSON[] {
    return this.context.jsonExtract(value, pattern);
  }

  log(object: any): void {
    console.log(cloneIfProxy(object));
  }
}

export class SKStoreFactoryImpl implements SKStoreFactory {
  //
  constructor(
    private context: () => Context,
    private create: (
      init: (
        collections: Record<string, EagerCollection<TJSON, TJSON>>,
      ) => CallResourceCompute,
      inputs: string[],
      tokens: Record<string, number>,
      initValues: Record<string, [TJSON, TJSON][]>,
    ) => void,
  ) {}

  getName() {
    return "SKStore";
  }

  runSKStore(
    init: (
      skstore: SKStore,
      collections: Record<string, EagerCollection<TJSON, TJSON>>,
    ) => CallResourceCompute,
    inputs: Record<string, [TJSON, TJSON][]> = {},
    remotes: Record<string, EntryPoint> = {},
    tokens: Record<string, number> = {},
  ): SkipBuilder {
    const context = this.context();
    const skstore = new SKStoreImpl(context);
    this.create(
      (collections) => init(skstore, collections),
      [...Object.keys(inputs), ...Object.keys(remotes)],
      tokens,
      inputs,
    );
    return (iCollections: Record<string, CollectionWriter<TJSON, TJSON>>) => {
      return new SkipRuntimeImpl(context, iCollections);
    };
  }
}

export function check<T>(value: T): void {
  if (
    typeof value == "string" ||
    typeof value == "number" ||
    typeof value == "boolean"
  ) {
    return;
  } else if (typeof value == "object") {
    if (value === null || isSkFrozen(value)) {
      return;
    }
    if (Object.isFrozen(value)) {
      if (Array.isArray(value)) {
        value.forEach(check);
      } else {
        Object.values(value).forEach(check);
      }
    } else {
      throw new Error("Invalid object: must be frozen.");
    }
  } else {
    throw new Error(`'${typeof value}' cannot be managed by skstore.`);
  }
}

/**
 * _Deep-freeze_ an object, returning the same object that was passed in.
 *
 * This function is similar to
 * {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/freeze | `Object.freeze()`}
 * but freezes the object and deep-freezes all its properties,
 * recursively. The object is then not only _immutable_ but also
 * _constant_. Note that as a result all objects reachable from the
 * parameter will be frozen and no longer mutable or extensible, even from
 * other references.
 *
 * The primary use for this function is to satisfy the requirement that all
 * parameters to Skip `Mapper` constructors must be deep-frozen: objects
 * that have not been constructed by Skip can be passed to `freeze()` before
 * passing them to a `Mapper` constructor.
 *
 * @param value - The object to deep-freeze.
 * @returns The same object that was passed in.
 */
export function freeze<T>(value: T): T {
  if (
    typeof value == "string" ||
    typeof value == "number" ||
    typeof value == "boolean"
  ) {
    return value;
  } else if (typeof value == "object") {
    if (value === null) {
      return value;
    } else if (isSkFrozen(value)) {
      return value;
    } else if (Object.isFrozen(value)) {
      check(value);
      return value;
    } else if (Array.isArray(value)) {
      const length: number = value.length;
      for (let i = 0; i < length; i++) {
        value[i] = freeze(value[i]);
      }
      return Object.freeze(sk_freeze(value));
    } else {
      const jso = value as Record<string, any>;
      for (const key of Object.keys(jso)) {
        jso[key] = freeze(jso[key]);
      }
      return Object.freeze(sk_freeze(jso)) as T;
    }
  } else {
    throw new Error(`'${typeof value}' cannot be frozen.`);
  }
}

export class EagerCollectionReader<K extends TJSON, V extends TJSON>
  implements CollectionReader<K, V>
{
  constructor(
    private context: Context,
    private eagerHdl: string,
  ) {}

  getArray(key: K): V[] {
    return this.context.getArray(this.eagerHdl, key);
  }

  getOne(key: K): V {
    return this.context.getOne(this.eagerHdl, key);
  }

  maybeGetOne(key: K): Opt<V> {
    return this.context.maybeGetOne(this.eagerHdl, key);
  }

  getAll(): Entry<K, V>[] {
    return this.context.getAll<K, V>(this.eagerHdl);
  }

  getDiff(from: string): Watermarked<K, V> {
    return this.context.getDiff(this.eagerHdl, from);
  }

  subscribe(from: string, notify: Notifier<K, V>): bigint {
    return this.context.subscribe(this.eagerHdl, from, notify);
  }
}

export class EagerCollectionWriter<K extends TJSON, V extends TJSON>
  implements CollectionWriter<K, V>
{
  constructor(
    private context: Context,
    private eagerHdl: string,
  ) {}

  write(key: K, value: V[]): void {
    this.context.write(this.eagerHdl, key, value);
  }

  writeAll(values: Entry<K, V>[]): void {
    this.context.writeAll(this.eagerHdl, values);
  }

  deleteKeys(keys: K[]): void {
    this.context.deleteKeys(this.eagerHdl, keys);
  }
}

export class SkipRuntimeImpl implements SkipRuntime {
  constructor(
    private context: Context,
    private writables: Record<string, CollectionWriter<TJSON, TJSON>>,
  ) {}

  getAll<K extends TJSON, V extends TJSON>(
    resource: string,
    params: JSONObject,
    reactiveAuth?: Uint8Array,
  ): { values: Entry<K, V>[]; reactive?: ReactiveResponse } {
    if (!reactiveAuth) {
      reactiveAuth = new Uint8Array([]);
    }
    const [name, reader] = this.context.createReactiveRequest<K, V>(
      resource,
      params,
      reactiveAuth,
    );
    const result = reader.getDiff("0");
    return {
      reactive: { collection: name, watermark: result.watermark },
      values: result.values,
    };
  }

  head(
    resource: string,
    params: JSONObject,
    reactiveAuth: Uint8Array,
  ): ReactiveResponse {
    const [name, _reader] = this.context.createReactiveRequest(
      resource,
      params,
      reactiveAuth,
    );
    return { collection: name, watermark: "0" };
  }

  getOne<V extends TJSON>(
    resource: string,
    params: JSONObject,
    key: string,
    reactiveAuth?: Uint8Array,
  ): V[] {
    if (!reactiveAuth) {
      reactiveAuth = new Uint8Array([]);
    }
    const [_, reader] = this.context.createReactiveRequest<string, V>(
      resource,
      params,
      reactiveAuth,
    );
    return reader.getArray(key);
  }

  put<V extends TJSON>(collectionName: string, key: string, value: V[]): void {
    const collection = this.writables[collectionName];
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (!collection) {
      throw new UnknownCollectionError(
        `Collection ${collectionName} not found`,
      );
    }
    collection.write(key, value);
  }

  patch<K extends TJSON, V extends TJSON>(
    collectionName: string,
    values: Entry<K, V>[],
  ): void {
    const collection = this.writables[collectionName];
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (!collection) {
      throw new UnknownCollectionError(
        `Collection ${collectionName} not found`,
      );
    }
    collection.writeAll(values);
  }

  deleteKey(collectionName: string, key: string): void {
    const collection = this.writables[collectionName];
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (!collection) {
      throw new UnknownCollectionError(
        `Collection ${collectionName} not found`,
      );
    }
    collection.deleteKeys([key]);
  }

  subscribe<K extends TJSON, V extends TJSON>(
    collectionName: string,
    from: string,
    notify: Notifier<K, V>,
  ) {
    return this.context.subscribe(collectionName, from, notify);
  }

  unsubscribe(id: bigint): void {
    this.context.unsubscribe(id);
  }
}
