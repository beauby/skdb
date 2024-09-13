import type {
  SKStore,
  TJSON,
  Mapper,
  EagerCollection,
  NonEmptyIterator,
  SimpleSkipService,
  SimpleServiceOutput,
} from "skip-runtime";

import { runWithRESTServer } from "skip-runtime";

class Request implements Mapper<string, TJSON, string, TJSON> {
  constructor(private source: EagerCollection<string, TJSON>) {}

  mapElement(
    key: string,
    _it: NonEmptyIterator<TJSON>,
  ): Iterable<[string, TJSON]> {
    let computed = 0;
    const elements = key.split("/");
    if (elements.length == 3 && elements[1] == "add") {
      const value = this.source.maybeGetOne(elements[2]) as number;
      computed = value ? value : 0;
    }
    return Array([key, computed]);
  }
}

class Add implements Mapper<string, TJSON, string, TJSON> {
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

class Service implements SimpleSkipService {
  inputTables = ["input1", "input2"];

  reactiveCompute(
    _store: SKStore,
    requests: EagerCollection<string, TJSON>,
    inputCollections: Record<string, EagerCollection<string, TJSON>>,
  ): SimpleServiceOutput {
    const addResult = inputCollections.input1.map(Add, inputCollections.input2);
    const output = requests.map(Request, addResult);
    return { output };
  }
}

runWithRESTServer(new Service());
