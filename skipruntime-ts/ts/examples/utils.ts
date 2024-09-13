import type { TJSON, EntryPoint } from "skip-runtime";
import { createInterface } from "readline";
import { connect, Table, type Creds } from "skdb-ts-thin";

export { Table };

export interface ClientDefinition {
  port: number;
  scenarios: () => Step[][];
}

function toHttp(entrypoint: EntryPoint) {
  if (entrypoint.secured)
    return `https://${entrypoint.host}:${entrypoint.port}`;
  return `http://${entrypoint.host}:${entrypoint.port}`;
}

async function send(
  url: string,
  method: "POST" | "GET" | "PUT" | "DELETE" = "GET",
  data?: TJSON | undefined,
) {
  if (typeof XMLHttpRequest != "undefined") {
    return new Promise<TJSON>(function (resolve, reject) {
      let xhr = new XMLHttpRequest();
      xhr.open(method, url);
      xhr.setRequestHeader("Accept", "application/json");
      xhr.setRequestHeader("Content-Type", "application/json");

      xhr.onreadystatechange = function () {
        if (xhr.readyState === 4) {
          try {
            if (xhr.status == 200) resolve(JSON.parse(xhr.responseText));
            else
              reject(
                new Error(
                  `An HTTP error occurred: ${xhr.status} ${xhr.responseText}`,
                ),
              );
          } catch (e) {
            reject(e);
          }
        }
      };
      xhr.onerror = function () {
        reject(
          new Error(`An error occurred while sending the request '${url}'.`),
        );
      };
      xhr.ontimeout = function () {
        reject(new Error(`The request '${url}' timed out.`));
      };
      xhr.onabort = function () {
        reject(new Error(`The request '${url}' aborted.`));
      };
      xhr.send(data ? JSON.stringify(data) : undefined);
    });
  } else {
    try {
      const body = data ? JSON.stringify(data) : undefined;
      console.log("fetch", url, method, body);
      const response = await fetch(url, {
        method,
        body,
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(1000),
      });
      const responseText = await response.text();
      return JSON.parse(responseText);
    } catch (e: any) {
      throw e;
    }
  }
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

class SkipHttpAccessV1 {
  private entrypoint: string;
  constructor(
    entrypoint: EntryPoint = {
      host: "localhost",
      port: 3587,
    },
  ) {
    this.entrypoint = toHttp(entrypoint);
  }

  async read(collection: string, id: string) {
    return send(`${this.entrypoint}/v1/one/${collection}/${id}`);
  }

  async readAll(collection: string) {
    return send(`${this.entrypoint}/v1/${collection}`);
  }

  async write(collection: string, id: string, data: TJSON) {
    return send(`${this.entrypoint}/v1/${collection}/${id}`, "POST", data);
  }

  async writeMany(data: Write[]) {
    return send(`${this.entrypoint}/v1/many`, "POST", data);
  }

  async detele(collection: string, id: string) {
    return send(`${this.entrypoint}/v1/${collection}/${id}`, "DELETE");
  }

  async deteleMany(data: Delete[]) {
    return send(`${this.entrypoint}/v1/many`, "DELETE", data);
  }

  async request(id: string) {
    return send(
      `${this.entrypoint}/v1/one/__query/${encodeURIComponent(encodeURIComponent(id))}`,
    );
  }
}

type RequestQuery = { type: "request"; payload: string };
type WriteQuery = { type: "write"; payload: Write[] };
type DeleteQuery = { type: "delete"; payload: Delete[] };

export type Step = RequestQuery | WriteQuery | DeleteQuery;

class Session {
  scenario: Step[];
  perform: (l: Step) => void;
  error: (e: string) => void;
  current = 0;
  on = false;

  constructor(
    scenario: Step[],
    perform: (l: Step) => void,
    error: (e: string) => void,
  ) {
    this.scenario = scenario;
    this.perform = perform;
    this.error = error;
  }

  next(): boolean {
    if (this.current >= this.scenario.length) {
      this.error("The scenario as no more entries.");
      return false;
    }
    const step = this.scenario[this.current++];
    console.log(">>", step.type, JSON.stringify(step.payload));
    this.perform(step);
    return this.current < this.scenario.length;
  }

  play(): void {
    this.on = this.current < this.scenario.length;
    while (this.on) {
      this.on = this.next();
    }
  }

  pause(): void {
    this.on = false;
  }

  reset(): void {
    this.on = false;
    this.current = 0;
  }
}

class Player {
  running?: Session;

  constructor(
    private scenarios: Step[][],
    private perform: (l: string) => void,
    private send: (l: Step) => void,
    private error: (e: string) => void,
  ) {}

  start(idx: number) {
    const scenario = this.scenarios[idx - 1];
    if (!scenario) {
      this.error(`The scenario ${idx} does not exist`);
      return false;
    } else {
      this.running = new Session(scenario, this.send, this.error);
      return true;
    }
  }

  play(idx?: number) {
    let run = true;
    if (idx ?? 0 > 0) {
      run = this.start(idx!);
    }
    if (run) {
      if (this.running) {
        this.running.play();
      } else {
        this.error(`No current scenario session`);
      }
    }
  }

  step(idx?: number) {
    const running = this.running;
    if (!running) {
      this.error(`No current scenario session`);
      return;
    }
    let steps = Math.min(idx ?? 1, 1);
    while (steps > 0) {
      if (!running.next()) {
        break;
      }
      steps--;
    }
  }

  reset() {
    const running = this.running;
    if (running) running.reset();
  }

  stop() {
    this.running = undefined;
  }

  online(line: string) {
    if (line.trim().length == 0 && this.running) {
      this.step();
      return;
    }
    const patterns: [RegExp, (...args: string[]) => any][] = [
      [/^start ([a-z_0-9]+)$/g, (str: string) => this.start(parseInt(str))],
      [/^reset$/g, () => this.reset()],
      [/^step ([a-z_0-9]+)$/g, (str: string) => this.step(parseInt(str))],
      [/^step$/g, () => this.step()],
      [/^play ([a-z_0-9]+)$/g, (str: string) => this.play(parseInt(str))],
      [/^play$/g, () => this.play()],
      [/^stop$/g, () => this.stop()],
    ];
    let done = false;
    for (let i = 0; i < patterns.length; i++) {
      const pattern = patterns[i];
      const matches = [...line.matchAll(pattern[0])];
      if (matches.length > 0) {
        done = true;
        const args = matches[0].map((v) => v.toString());
        args.shift();
        pattern[1].apply(null, args);
      }
    }
    if (!done) this.perform(line);
  }
}

export async function run(scenarios: Step[][], port: number = 3587) {
  const access = new SkipHttpAccessV1({
    host: "localhost",
    port,
  });
  const online = async (line: string) => {
    const error = console.error;
    try {
      const patterns: [RegExp, (...args: string[]) => void][] = [
        [
          /^request (.*)$/g,
          (query: string) => {
            access.request(query).then(console.log).catch(console.error);
          },
        ],
        [
          /^write (.*)$/g,
          async (query: string) => {
            const jsquery = JSON.parse(query);
            access.writeMany(jsquery).then(console.log).catch(console.error);
          },
        ],
        [
          /^delete (.*)$/g,
          async (query: string) => {
            const jsquery = JSON.parse(query);
            access.deteleMany(jsquery).then(console.log).catch(console.error);
          },
        ],
      ];
      let done = false;
      for (let i = 0; i < patterns.length; i++) {
        const pattern = patterns[i];
        const matches = [...line.matchAll(pattern[0])];
        if (matches.length > 0) {
          done = true;
          const args = matches[0].map((v) => v.toString());
          args.shift();
          pattern[1].apply(null, args);
        }
      }
      if (!done) {
        error(`Unknow command line '${line}'`);
      }
    } catch (e: any) {
      const message = e instanceof Error ? e.message : JSON.stringify(e);
      error(message);
    }
  };
  const player = new Player(
    scenarios,
    online,
    (step) => {
      if (step.type == "request") {
        access.request(step.payload).then(console.log).catch(console.error);
      } else if (step.type == "write") {
        access.writeMany(step.payload).then(console.log).catch(console.error);
      } else {
        access.deteleMany(step.payload).then(console.log).catch(console.error);
      }
    },
    console.error,
  );
  const rl = createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "> ",
  });
  rl.prompt();
  rl.on("line", (line: string) => {
    if (line == "exit") {
      process.exit(0);
    } else {
      player.online(line);
      rl.prompt();
    }
  });
}

export const LOCAL_SERVER: string = "ws://localhost:3586";

async function getCreds(
  database: string,
  entryPoint: EntryPoint = { host: "localhost", port: 3586 },
) {
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
}

async function creds(): Promise<Creds> {
  const dbinfo = await getCreds("skstore");
  const keyBytes = Buffer.from(dbinfo.get("root"), "base64");
  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return {
    accessKey: "root",
    privateKey: key,
    deviceUuid: crypto.randomUUID(),
  };
}

export async function subscribe(
  init_cb: (rows: Table) => void,
  update_cb: (added: Table, removed: Table) => void,
) {
  const skipclient = await connect(LOCAL_SERVER, "skstore", await creds(), [
    { table: "__sk_responses" },
  ]);

  return skipclient.subscribe("__sk_responses", init_cb, update_cb);
}
