import type {
  SKStore,
  TJSON,
  Mapper,
  EagerCollection,
  NonEmptyIterator,
  SkipService,
  Resource,
} from "skip-runtime";

import { runService } from "skip-runtime";

class Plus implements Mapper<string, TJSON, string, TJSON> {
  constructor(private other: EagerCollection<string, TJSON>) {}

  mapElement(
    key: string,
    it: NonEmptyIterator<TJSON>,
  ): Iterable<[string, TJSON]> {
    const v = it.first() as number;
    const ev = this.other.maybeGetOne(key) as number;
    if (ev !== null) {
      return Array([key, v + (ev ?? 0)]);
    }
    return Array();
  }
}

class Minus implements Mapper<string, TJSON, string, TJSON> {
  constructor(private other: EagerCollection<string, TJSON>) {}

  mapElement(
    key: string,
    it: NonEmptyIterator<TJSON>,
  ): Iterable<[string, TJSON]> {
    const v = it.first() as number;
    const ev = this.other.maybeGetOne(key) as number;
    if (ev !== null) {
      return Array([key, v - (ev ?? 0)]);
    }
    return Array();
  }
}

class Add implements Resource {
  reactiveCompute(
    _store: SKStore,
    collections: {
      input1: EagerCollection<string, TJSON>;
      input2: EagerCollection<string, TJSON>;
    },
  ): EagerCollection<string, TJSON> {
    return collections.input1.map(Plus, collections.input2);
  }
}

class Sub implements Resource {
  reactiveCompute(
    _store: SKStore,
    collections: {
      input1: EagerCollection<string, TJSON>;
      input2: EagerCollection<string, TJSON>;
    },
  ): EagerCollection<string, TJSON> {
    return collections.input1.map(Minus, collections.input2);
  }
}

class Service implements SkipService {
  inputCollections = ["input1", "input2"];
  resources = { add: Add, sub: Sub };

  async init() {
    console.log("Init called returns no values");
    return {};
  }

  reactiveCompute(
    _store: SKStore,
    inputCollections: Record<string, EagerCollection<string, TJSON>>,
  ) {
    return inputCollections;
  }
}

runService(new Service(), 3587);
