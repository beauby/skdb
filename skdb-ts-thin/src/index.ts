import { WebSocket, MessageEvent, CloseEvent } from "ws";
import * as util from "util";
import { webcrypto } from "node:crypto";

const decoder = new util.TextDecoder("utf8");

function decodeUTF8(v: ArrayBuffer): string {
  return decoder.decode(v)
}

function csv2array(v: string): (string|number|null)[] {
  const res = [];
  let pos = 0;
  while (pos < v.length) {
    if (v[pos] == '"') {
      let end_pos = pos + 1;
      let str = "";
      while (v[end_pos] != '"') {
        if (v[end_pos] == '\\') end_pos++;
        str += v[end_pos];
        end_pos++;
      }
      res.push(str);
      pos = end_pos + 1;
    } else {
      let haystack = v.substring(pos);
      const end_pos = haystack.indexOf(',');
      if (end_pos != -1) {
        haystack = haystack.substring(0, end_pos);
      }
      if (v[pos] == '.' || ('0' <= v[pos] && v[pos] <= '9')) {
        res.push(parseFloat(haystack));
      } else {
        res.push(null)
      }
      pos += haystack.length;
    }

    pos += 1;
  }

  return res;
}

export type Params =
  | Map<string, string | number | boolean | null>
  | Record<string, string | number | boolean | null>;

export interface MirrorDefn {
  table: string;
  expectedColumns: string;
  filterExpr?: string;
  filterParams?: Params;
}

export type SKDBValue = string | number | boolean | null;

/* ***************************************************************************/
/* Class for query results, extending Array<Record<>> with some common
   selectors and utility functions for ease of use. */
/* ***************************************************************************/
export class SKDBTable extends Array<Record<string, SKDBValue>> {
  static ofRows(data: [number, Record<string, SKDBValue>][]): SKDBTable {
    const rows = data.flatMap(r => new Array<Record<string, SKDBValue>>(r[0]).fill(r[1]));
    return rows as SKDBTable;
  }

  scalarValue(): SKDBValue | undefined {
    const row = this.firstRow();
    if (row === undefined) {
      return undefined;
    }
    const cols = Object.keys(row);
    if (cols.length < 1) {
      return undefined;
    }
    if (cols.length > 1) {
      throw new Error(
        `Can't extract scalar: query yielded ${cols.length} columns`,
      );
    }
    return row[cols[0]];
  }

  onlyRow(): Record<string, SKDBValue> {
    if (this.length != 1) {
      throw new Error(`Can't extract only row: got ${this.length} rows`);
    }
    return this[0];
  }

  firstRow(): Record<string, SKDBValue> | undefined {
    if (this.length < 1) {
      return undefined;
    }
    return this[0];
  }

  lastRow(): Record<string, SKDBValue> | undefined {
    if (this.length < 1) {
      return undefined;
    }
    return this[this.length - 1];
  }

  onlyColumn(): SKDBValue[] {
    const row = this.firstRow();
    if (row === undefined) {
      return [];
    }
    const cols = Object.keys(row);
    if (cols.length < 1) {
      return []; // unlikely to have rows but no cols
    }
    if (cols.length > 1) {
      throw new Error("Too many columns");
    }
    const col = cols[0];
    return this.column(col);
  }

  column(col: string): SKDBValue[] {
    return this.map((row) => {
      if (!row[col]) {
        throw new Error("Missing column: " + col);
      }
      return row[col];
    });
  }
}

const npmVersion = "";

/* ***************************************************************************/
/* Orchestration protocol. */
/* ***************************************************************************/

interface ProtoQuery {
  type: "query";
  query: string;
  format: "json" | "raw" | "csv";
}

interface ProtoQuerySchema {
  type: "schema";
  name?: string;
  scope: "all" | "table" | "view" | "legacy_schema";
  suffix?: string;
}

interface ProtoRequestTail {
  type: "tail";
  table: string;
  expectedColumns: string;
  since: bigint;
  filterExpr: string;
  params: Params;
}

interface ProtoRequestTailBatch {
  type: "tailBatch";
  requests: ProtoRequestTail[];
}

interface ProtoPushPromise {
  type: "pushPromise";
  schemas: Record<string, string>;
}

interface ProtoRequestCreateDb {
  type: "createDatabase";
  name: string;
}

interface ProtoRequestCreateUser {
  type: "createUser";
}

interface ProtoResponseCreds {
  type: "credentials";
  accessKey: string;
  privateKey: Uint8Array;
}

type ProtoCtrlMsg =
  | ProtoQuery
  | ProtoQuerySchema
  | ProtoRequestCreateDb
  | ProtoRequestTail
  | ProtoPushPromise
  | ProtoRequestCreateUser
  | ProtoRequestTailBatch;

interface ProtoData {
  type: "data";
  payload: ArrayBuffer;
}

type ProtoResponse = ProtoResponseCreds | ProtoData;

type ProtoMsg = ProtoCtrlMsg | ProtoResponse;

function encodeProtoMsg(msg: ProtoMsg): ArrayBuffer {
  switch (msg.type) {
    case "query": {
      const buf = new ArrayBuffer(6 + msg.query.length * 4);
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      const textEncoder = new TextEncoder();
      const encodeResult = textEncoder.encodeInto(
        msg.query,
        uint8View.subarray(6),
      );
      dataView.setUint8(0, 0x1); // type
      const formatLookup = new Map([
        ["json", 0x0],
        ["raw", 0x1],
        ["csv", 0x2],
      ]);
      const format = formatLookup.get(msg.format);
      if (format === undefined) {
        throw new Error(`Cannot serialize format ${msg.format}`);
      }
      dataView.setUint8(1, format);
      dataView.setUint32(2, encodeResult.written || 0, false);
      return buf.slice(0, 6 + (encodeResult.written || 0));
    }
    case "schema": {
      const name = msg.name || "";
      const suffix = msg.suffix || "";
      const buf = new ArrayBuffer(6 + name.length * 4 + suffix.length * 4);
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      const textEncoder = new TextEncoder();
      let encodeResult = textEncoder.encodeInto(name, uint8View.subarray(4));
      const suffixIdx = 4 + (encodeResult.written || 0);
      dataView.setUint8(0, 0x4); // type
      const scopeLookup = new Map([
        ["all", 0x0],
        ["table", 0x1],
        ["view", 0x2],
        ["legacy_schema", 0x3],
      ]);
      const scope = scopeLookup.get(msg.scope);
      if (scope === undefined) {
        throw new Error(`Cannot serialize scope ${msg.scope}`);
      }
      dataView.setUint8(1, scope);
      dataView.setUint16(2, encodeResult.written || 0, false);
      encodeResult = textEncoder.encodeInto(
        suffix,
        uint8View.subarray(suffixIdx + 2),
      );
      dataView.setUint16(suffixIdx, encodeResult.written || 0, false);
      return buf.slice(0, suffixIdx + 2 + (encodeResult.written || 0));
    }
    case "tail": {
      const params =
        msg.params instanceof Map ? Object.fromEntries(msg.params) : msg.params;
      const serialisedParams = JSON.stringify(params);

      const buf = new ArrayBuffer(
        20 +
          msg.table.length * 4 +
          msg.expectedColumns.length * 4 +
          msg.filterExpr.length * 4 +
          serialisedParams.length * 4,
      );
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      const textEncoder = new TextEncoder();
      let encodeResult = textEncoder.encodeInto(
        msg.table,
        uint8View.subarray(14),
      );
      dataView.setUint8(0, 0x2); // type
      dataView.setBigUint64(4, msg.since, false);
      dataView.setUint16(12, encodeResult.written || 0, false);

      const expectedColsOffset = 14 + (encodeResult.written || 0);
      encodeResult = textEncoder.encodeInto(
        msg.expectedColumns,
        uint8View.subarray(expectedColsOffset + 2),
      );
      dataView.setUint16(expectedColsOffset, encodeResult.written || 0, false);

      const filterExprOffset =
        expectedColsOffset + 2 + (encodeResult.written || 0);
      encodeResult = textEncoder.encodeInto(
        msg.filterExpr,
        uint8View.subarray(filterExprOffset + 2),
      );
      dataView.setUint16(filterExprOffset, encodeResult.written || 0, false);

      const paramsOffset = filterExprOffset + 2 + (encodeResult.written || 0);
      encodeResult = textEncoder.encodeInto(
        serialisedParams,
        uint8View.subarray(paramsOffset + 2),
      );
      dataView.setUint16(paramsOffset, encodeResult.written || 0, false);

      return buf.slice(0, paramsOffset + 2 + (encodeResult.written || 0));
    }
    case "tailBatch": {
      const buffers = msg.requests.map(encodeProtoMsg);
      const size = buffers.reduce((acc, b) => acc + b.byteLength, 0);
      const buf = new ArrayBuffer(4 + size);
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      dataView.setUint8(0, 0x7); // type
      dataView.setUint16(2, msg.requests.length, false);
      let offset = 4;
      for (const tail of buffers) {
        uint8View.set(new Uint8Array(tail), offset);
        offset = offset + tail.byteLength;
      }
      return buf;
    }
    case "pushPromise": {
      const serializedSchemas = JSON.stringify(msg.schemas);
      const buf = new ArrayBuffer(4 + serializedSchemas.length * 4);
      const dataView = new DataView(buf);
      const uint8View = new Uint8Array(buf);
      const textEncoder = new TextEncoder();
      const encodeResult = textEncoder.encodeInto(
        serializedSchemas,
        uint8View.subarray(6),
      );
      dataView.setUint8(0, 0x3); // type
      dataView.setUint16(4, encodeResult.written || 0, false);
      return buf.slice(0, 6 + (encodeResult.written || 0));
    }
    case "createDatabase": {
      const buf = new ArrayBuffer(3 + msg.name.length * 4);
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      const textEncoder = new TextEncoder();
      const encodeResult = textEncoder.encodeInto(
        msg.name,
        uint8View.subarray(3),
      );
      dataView.setUint8(0, 0x5); // type
      dataView.setUint16(1, encodeResult.written || 0, false);
      return buf.slice(0, 3 + (encodeResult.written || 0));
    }
    case "createUser": {
      const buf = new ArrayBuffer(1);
      const dataView = new DataView(buf);
      dataView.setUint8(0, 0x6); // type
      return buf;
    }
    case "credentials": {
      throw new Error("Encoding credentials unsupported");
    }
    case "data": {
      const buf = new ArrayBuffer(2 + msg.payload.byteLength);
      const uint8View = new Uint8Array(buf);
      const dataView = new DataView(buf);
      dataView.setUint8(0, 0x0); // type
      // fin flag always set - we currently assume that JS doesn't stream chunks
      dataView.setUint8(1, 0x1);
      uint8View.set(new Uint8Array(msg.payload), 2);
      return buf;
    }
  }
}

class ProtoMsgDecoder {
  private bufs: Uint8Array[] = [];
  private msgs: (ProtoMsg | null)[] = [];

  private popBufs(): ArrayBuffer {
    if (this.bufs.length == 1) {
      // avoid copying for the common case of single buffer
      const buf = this.bufs.pop();
      if (!buf) {
        throw new Error("invariant violation");
      }
      return buf;
    }

    let bytes = 0;
    for (const buf of this.bufs) {
      bytes += buf.byteLength;
    }

    const acc = new ArrayBuffer(bytes);
    const uint8View = new Uint8Array(acc);
    let offset = 0;
    for (const buf of this.bufs) {
      uint8View.set(buf, offset);
      offset += buf.byteLength;
    }

    this.bufs = [];
    return acc;
  }

  // like a stack machine, you push bytes in until the machine pops
  // them all, turns them in to a msg, and pushes this on to the stack
  // to be popped. push returns true when a new msg is ready to be
  // popped.
  push(msg: ArrayBuffer): boolean {
    const dv = new DataView(msg);
    const type = dv.getUint8(0);
    switch (type) {
      // credentials response
      case 0x80: {
        const accessKeyFixedWidthBytes = new Uint8Array(msg, 1, 27);
        // access key is a fixed-width but potentially zero-terminated string
        const zeroIndex = accessKeyFixedWidthBytes.findIndex((x) => x == 0);
        const accessKeyBytes = accessKeyFixedWidthBytes.slice(
          0,
          zeroIndex < 0 ? 27 : zeroIndex,
        );
        const decoder = new TextDecoder();
        const accessKey = decoder.decode(accessKeyBytes);
        this.msgs.push({
          type: "credentials",
          accessKey: accessKey,
          privateKey: new Uint8Array(msg, 28, 32),
        });
        return true;
      }
      // streaming data
      case 0x0: {
        const flags = dv.getUint8(1);
        const fin = (flags & 0x01) === 1;
        this.bufs.push(new Uint8Array(msg, 2));
        if (fin) {
          this.msgs.push({
            type: "data",
            payload: this.popBufs(),
          });
          return true;
        }
        return false;
      }
      default: {
        this.msgs.push(null);
        return true;
      }
    }
  }

  // returns the last message assembled and clears it off the stack.
  // null represents a message from a future schema that we don't understand
  pop(): ProtoMsg | null {
    const msg = this.msgs.pop();
    if (msg === undefined) {
      throw new Error("Popping an empty stack.");
    }
    return msg;
  }

  // returns the last message assembled and clears it off the stack.
  // useful if tracking the return from push is annoying. null
  // ambiguously represents receiving a message from a future schema
  // that we don't understand or empty stack.
  tryPop(): ProtoMsg | null {
    const msg = this.msgs.pop();
    if (msg === undefined) {
      return null;
    }
    return msg;
  }
}

/* ***************************************************************************/
/* Resilient connection abstraction
/* ***************************************************************************/

interface ResiliencyPolicy {
  notifyFailedStream: () => void;
  shouldReconnect: (socket: ResilientMuxedSocket) => Promise<boolean>;
}

class ResilientMuxedSocket {
  private uri: string;
  private creds: Creds;
  private policy: ResiliencyPolicy;
  private socket?: MuxedSocket;
  private socketQueue: any[] = [];
  private permanentFailureReason?: string;

  // streams from the server are not resilient
  onStream?: (stream: Stream) => void;

  async openStream(): Promise<Stream> {
    const socket = await this.getSocket();
    return socket.openStream();
  }

  async openResilientStream(): Promise<ResilientStream> {
    const socket = await this.getSocket();
    const stream = await socket.openStream();
    return new ResilientStream(this, stream);
  }

  async closeSocket(): Promise<void> {
    const socket = this.socket ?? (await this.getSocket());
    this.socket = undefined;
    this.socketQueue = [];
    socket.closeSocket();
  }

  async errorSocket(errorCode: number, msg: string): Promise<void> {
    const socket = this.socket ?? (await this.getSocket());
    this.socket = undefined;
    this.socketQueue = [];
    socket.errorSocket(errorCode, msg);
  }

  async isSocketResponsive(): Promise<boolean> {
    if (!this.socket) {
      return false;
    }
    return this.socket.pingSocket();
  }

  static async connect(
    policy: ResiliencyPolicy,
    uri: string,
    creds: Creds,
  ): Promise<ResilientMuxedSocket> {
    const socket = await MuxedSocket.connect(uri, creds);
    return new ResilientMuxedSocket(policy, uri, creds, socket);
  }

  isSocketConsideredHealthy(): boolean {
    return this.socket !== undefined;
  }

  private constructor(
    policy: ResiliencyPolicy,
    uri: string,
    creds: Creds,
    initialSocket: MuxedSocket,
  ) {
    this.attachSocket(initialSocket);
    this.policy = policy;
    this.uri = uri;
    this.creds = creds;
  }

  private async getSocket(): Promise<MuxedSocket> {
    if (this.permanentFailureReason !== undefined) {
      throw new Error(this.permanentFailureReason);
    }
    if (this.socket) {
      return this.socket;
    }
    return new Promise((resolve, reject) => {
      this.socketQueue.push({ resolve: resolve, reject: reject });
    });
  }

  private isSocketErrorRetryable(errorCode: number): boolean {
    if (errorCode === 1002) {
      // auth failure - no point in retrying.
      return false;
    }
    if (errorCode === 1004) {
      // connection request failure - user error - bad uri? - no point
      // in retrying
      return false;
    }
    return true;
  }

  private attachSocket(socket: MuxedSocket): void {
    socket.onStream = (stream) => {
      if (this.onStream) {
        this.onStream(stream);
      }
    };
    socket.onClose = () => {
      this.replaceFailedSocket();
    };
    socket.onError = (errorCode, msg) => {
      if (!this.isSocketErrorRetryable(errorCode)) {
        // we do not have a way of communicating upward that we're in
        // a non-retryable state. there are very few cases where this
        // can happen and they're checked for explicitly.
        this.permanentFailureReason = msg;
        this.socket = undefined;
        return;
      }
      this.replaceFailedSocket();
    };
    this.socket = socket;
    for (const promise of this.socketQueue) {
      promise.resolve(socket);
    }
    this.socketQueue = [];
  }

  private async replaceFailedSocket(): Promise<void> {
    if (this.permanentFailureReason !== undefined) {
      return;
    }
    if (!this.socket) {
      return; // already reconnecting
    }

    const oldSocket = this.socket;
    this.socket.onStream = undefined;
    this.socket.onClose = undefined;
    this.socket.onError = undefined;
    this.socket = undefined;
    oldSocket.errorSocket(0, "Socket suspected to have failed");

    const backoffMs = 500 + Math.random() * 1000;
    await new Promise((resolve) => setTimeout(resolve, backoffMs));

    while (true) {
      try {
        const socket = await MuxedSocket.connect(
          this.uri,
          this.creds,
        );
        this.attachSocket(socket);
        return;
      } catch (_error) {
        const backoffMs = 500 + Math.random() * 1000;
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }
  }

  // interface used by ResilientStream

  async replaceFailedStream(): Promise<Stream> {
    this.policy.notifyFailedStream();
    if (await this.policy.shouldReconnect(this)) {
      this.replaceFailedSocket();
    }

    const socket = await this.getSocket();

    try {
      return socket.openStream();
    } catch {
      await this.replaceFailedSocket();
      return this.replaceFailedStream();
    }
  }
}

class ResilientStream {
  private socket: ResilientMuxedSocket;
  private stream?: Stream;

  private failureDetectionTimeout?: number;

  private setFailureDetectionTimeout(timeout?: number): void {
    clearTimeout(this.failureDetectionTimeout);
    this.failureDetectionTimeout = timeout;
  }

  onData?: (data: ArrayBuffer) => void;

  onReconnect?: () => void;

  close(): void {
    if (this.stream !== undefined) {
      this.stream.onClose = undefined;
      this.stream.onData = undefined;
      this.stream.onError = undefined;
      this.stream.close();
      this.stream = undefined;
    }
    this.setFailureDetectionTimeout(undefined);
  }

  send(data: ArrayBuffer): void {
    if (!this.stream) {
      // black hole the data. we're reconnecting and will call
      // onReconnect that should address the gap
      return;
    }
    this.stream.send(data);
  }

  expectingData(): void {
    if (this.failureDetectionTimeout) {
      // already expecting a response and hasn't arrived
      return;
    }

    if (!this.stream) {
      // we're reconnecting
      return;
    }

    const failureThresholdMs = 60000;
    const timeout = setTimeout(() => {
      this.replaceFailedStream();
    }, failureThresholdMs);
    // @ts-expect-error TODO: Fix the following error.
    this.setFailureDetectionTimeout(timeout);
  }

  private attachStream(stream: Stream): void {
    stream.onData = (data) => {
      // data received; connection is healthy
      this.setFailureDetectionTimeout(undefined);
      if (this.onData) {
        this.onData(data);
      }
    };
    stream.onClose = () => {
      this.replaceFailedStream();
    };
    stream.onError = (_errorCode, _msg) => {
      // we ignore the error code and attempt to re-establish the
      // stream from scratch, which should resolve the issue even if
      // it wasn't in a retryable state.
      this.replaceFailedStream();
    };
    this.stream = stream;
  }

  private async replaceFailedStream(): Promise<void> {
    if (!this.stream) {
      return; // already reconnecting
    }
    const oldStream = this.stream;
    oldStream.onData = undefined;
    oldStream.onClose = undefined;
    oldStream.onError = undefined;
    this.stream = undefined;
    this.setFailureDetectionTimeout(undefined);
    // if it _has_ failed, this is a no-op, otherwise it protects invariants
    oldStream.error(0, "Stream suspected to have failed");
    const newStream = await this.socket.replaceFailedStream();
    this.attachStream(newStream);
    if (this.onReconnect) {
      this.onReconnect();
    }
  }

  constructor(socket: ResilientMuxedSocket, stream: Stream) {
    this.socket = socket;
    this.attachStream(stream);
  }
}

/* ***************************************************************************/
/* Stream MUX protocol */
/* ***************************************************************************/

enum MuxedSocketState {
  IDLE,
  AUTH_SENT,
  CLOSING, // can receive data
  CLOSEWAIT, // can send data
  CLOSED,
}

interface MuxAuth {
  type: "auth";
}
interface MuxGoaway {
  type: "goaway";
  lastStream: number;
  errorCode: number;
  msg: string;
}
interface MuxStreamData {
  type: "data";
  stream: number;
  payload: ArrayBuffer;
}
interface MuxStreamClose {
  type: "close";
  stream: number;
}
interface MuxStreamReset {
  type: "reset";
  stream: number;
  errorCode: number;
  msg: string;
}
type MuxMessage =
  | MuxAuth
  | MuxGoaway
  | MuxStreamData
  | MuxStreamClose
  | MuxStreamReset;

export interface Creds {
  accessKey: string;
  privateKey: webcrypto.CryptoKey;
  deviceUuid: string;
}

export class MuxedSocket {
  // constants
  private socket: WebSocket;
  private creds: Creds;
  private reauthTimeoutMs = 5 * 60 * 1000; // 5 mins - half of the 10 min window

  // state
  private state: MuxedSocketState = MuxedSocketState.IDLE;
  private reauthTimer?: any;
  // streams in the open or closing state
  private activeStreams = new Map<number, Stream>();
  private serverStreamWatermark = 0;
  private nextStream = 1;

  private healthChecks: ((isOk: boolean) => void)[] = [];

  // user facing interface /////////////////////////////////////////////////////

  onStream?: (stream: Stream) => void;
  onClose?: () => void;
  onError?: (errorCode: number, msg: string) => void;

  private constructor(socket: WebSocket, creds: Creds) {
    // pre-condition: socket is open
    this.socket = socket;
    this.creds = creds;
  }

  openStream(): Promise<Stream> {
    const openTimeoutMs = 10;
    const fn = (resolve: any, reject: any) => {
      switch (this.state) {
        case MuxedSocketState.AUTH_SENT: {
          const streamId = this.nextStream;
          this.nextStream = this.nextStream + 2; // client uses odd-numbered streams
          const stream = new Stream(this, streamId);
          this.activeStreams.set(streamId, stream);
          resolve(stream);
          return;
        }
        case MuxedSocketState.IDLE:
          setTimeout(() => fn(resolve, reject), openTimeoutMs);
          return;
        case MuxedSocketState.CLOSING:
        case MuxedSocketState.CLOSEWAIT:
          reject(new Error("Connection closing"));
          return;
        case MuxedSocketState.CLOSED:
          reject(new Error("Connection not established"));
          return;
      }
    };
    return new Promise(fn);
  }

  closeSocket(): void {
    switch (this.state) {
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.IDLE:
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
        this.socket.close();
        break;
      case MuxedSocketState.CLOSEWAIT: {
        for (const stream of this.activeStreams.values()) {
          stream.close();
        }
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
        this.socket.close();
        break;
      }
      case MuxedSocketState.AUTH_SENT: {
        for (const stream of this.activeStreams.values()) {
          stream.close();
        }
        this.state = MuxedSocketState.CLOSING;
        this.socket.close();
        break;
      }
    }
  }

  errorSocket(errorCode: number, msg: string): void {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
        break;
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSEWAIT: {
        for (const stream of this.activeStreams.values()) {
          // this is different to closing. we just immediately trigger
          // callbacks on streams and only send the goaway for socket.
          // this is because erroring is not reciprocated by the server
          stream.onStreamError(errorCode, msg);
        }
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
        const lastStream = Math.max(
          this.nextStream - 2,
          this.serverStreamWatermark,
        );
        this.socket.send(this.encodeGoawayMsg(lastStream, errorCode, msg));
        this.socket.close(4000);
        break;
      }
    }
  }

  async pingSocket(): Promise<boolean> {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        return false;
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSEWAIT:
        return new Promise((resolve, _reject) => {
          this.socket.send(this.encodePingMsg());
          const pingTimeoutMs = 10000;
          const timeout = setTimeout(() => resolve(false), pingTimeoutMs);
          this.healthChecks.push((isOk) => {
            clearTimeout(timeout);
            resolve(isOk);
          });
        });
    }
  }

  static async connect(
    uri: string,
    creds: Creds,
    timeoutMs: number = 60000,
  ): Promise<MuxedSocket> {
    return new Promise((resolve, reject) => {
      let failed = false;
      const timeout = setTimeout(() => {
        failed = true;
        reject(new Error("Timeout waiting to connect"));
      }, timeoutMs);
      const socket = new WebSocket(uri);
      socket.binaryType = "arraybuffer";
      socket.onclose = (_event) =>
        reject(new Error("Socket closed before open"));
      socket.onerror = (event) => reject(event);
      socket.onmessage = (_event) =>
        reject(new Error("Socket messaged before open"));
      socket.onopen = (_event) => {
        clearTimeout(timeout);
        if (failed) {
          socket.close();
          return;
        }
        const muxSocket = new MuxedSocket(socket, creds);
        socket.onclose = (event) => muxSocket.onSocketClose(event);
        socket.onerror = (_event) => muxSocket.onSocketError(0, "socket error");
        socket.onmessage = (event) => muxSocket.onSocketMessage(event);
        muxSocket
          .sendAuth()
          .then(() => resolve(muxSocket))
          .catch((reason) => reject(reason));
      };
    });
  }

  // interface used by Stream //////////////////////////////////////////////////

  streamClose(stream: number, nowClosed: boolean): void {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSEWAIT: {
        this.socket.send(this.encodeStreamCloseMsg(stream));
        if (nowClosed) {
          this.activeStreams.delete(stream);
        }
        break;
      }
    }
  }

  streamError(stream: number, errorCode: number, msg: string): void {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSEWAIT:
        this.socket.send(this.encodeStreamResetMsg(stream, errorCode, msg));
        this.activeStreams.delete(stream);
        break;
    }
  }

  streamSend(stream: number, data: ArrayBuffer): void {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSEWAIT:
        this.socket.send(this.encodeStreamDataMsg(stream, data));
    }
  }

  // private ///////////////////////////////////////////////////////////////////

  private onSocketClose(_event: CloseEvent): void {
    switch (this.state) {
      case MuxedSocketState.CLOSEWAIT:
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.IDLE:
      case MuxedSocketState.AUTH_SENT:
        for (const stream of this.activeStreams.values()) {
          stream.onStreamClose();
        }
        if (this.onClose) {
          this.onClose();
        }
        this.state = MuxedSocketState.CLOSEWAIT;
        break;
      case MuxedSocketState.CLOSING:
        for (const stream of this.activeStreams.values()) {
          stream.onStreamClose();
        }
        if (this.onClose) {
          this.onClose();
        }
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
        break;
    }
  }

  private onSocketError(errorCode: number, msg: string): void {
    switch (this.state) {
      case MuxedSocketState.CLOSED:
        break;
      case MuxedSocketState.IDLE:
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSEWAIT:
        for (const stream of this.activeStreams.values()) {
          stream.onStreamError(errorCode, msg);
        }
        if (this.onError) {
          this.onError(errorCode, msg);
        }
        this.activeStreams.clear();
        clearTimeout(this.reauthTimer);
        this.state = MuxedSocketState.CLOSED;
    }
  }

  private onSocketMessage(event: MessageEvent): void {
    switch (this.state) {
      case MuxedSocketState.AUTH_SENT:
      case MuxedSocketState.CLOSING: {
        if (!(event.data instanceof ArrayBuffer)) {
          throw new Error("Received unexpected text data");
        }
        const msg = this.decode(event.data);
        if (msg === null) {
          // for robustness we ignore messages we don't understand, or
          // may have been handled e.g. ping
          return;
        }
        switch (msg.type) {
          case "auth":
            throw new Error("Unexepected auth message from server");
          case "goaway":
            this.onSocketError(msg.errorCode, msg.msg);
            break;
          case "data": {
            let stream = this.activeStreams.get(msg.stream);

            if (stream == undefined && this.state == MuxedSocketState.CLOSING) {
              // we don't accept new streams while closing
              break;
            }

            // TODO: is the watermark condition necesary? we don't want to
            // reuse streams but this doesn't allow for creating them with
            // non-deterministic scheduling. if we don't accept them,
            // should probably send a stream reset
            if (
              stream === undefined &&
              msg.stream % 2 == 0 &&
              msg.stream > this.serverStreamWatermark
            ) {
              // new server-initiated stream
              this.serverStreamWatermark = msg.stream;
              stream = new Stream(this, msg.stream);
              this.activeStreams.set(msg.stream, stream);
              if (this.onStream) {
                this.onStream(stream);
              }
            }
            stream?.onStreamData(msg.payload);
            break;
          }
          case "close": {
            const closed = this.activeStreams.get(msg.stream)?.onStreamClose();
            if (closed) {
              this.activeStreams.delete(msg.stream);
            }
            break;
          }
          case "reset":
            this.activeStreams
              .get(msg.stream)
              ?.onStreamError(msg.errorCode, msg.msg);
            this.activeStreams.delete(msg.stream);
            break;
          default:
            throw new Error("Unexpected message type");
        }
        break;
      }
      case MuxedSocketState.IDLE:
      case MuxedSocketState.CLOSEWAIT:
      case MuxedSocketState.CLOSED:
        break;
    }
  }

  private async sendAuth(): Promise<void> {
    switch (this.state) {
      case MuxedSocketState.IDLE:
      case MuxedSocketState.AUTH_SENT: {
        const auth = await MuxedSocket.encodeAuthMsg(this.creds);
        this.socket.send(auth);
        this.state = MuxedSocketState.AUTH_SENT;
        clearTimeout(this.reauthTimer);
        this.reauthTimer = setTimeout(() => {
          this.sendAuth();
        }, this.reauthTimeoutMs);
        break;
      }
      case MuxedSocketState.CLOSING:
      case MuxedSocketState.CLOSEWAIT:
      case MuxedSocketState.CLOSED:
        break;
    }
  }

  private static async encodeAuthMsg(
    creds: Creds,
  ): Promise<ArrayBuffer> {
    const clientVersion = "js-" + npmVersion;
    const enc = new TextEncoder();
    const buf = new ArrayBuffer(140 + clientVersion.length);
    const uint8View = new Uint8Array(buf);
    const dataView = new DataView(buf);
    const now = new Date().toISOString();
    const nonce = uint8View.subarray(35, 43);
    crypto.getRandomValues(nonce);
    const b64nonce = btoa(String.fromCharCode(...nonce));
    const bytesToSign = enc.encode("auth" + creds.accessKey + now + b64nonce);
    const sig = await crypto.subtle.sign("HMAC", creds.privateKey, bytesToSign);

    dataView.setUint8(0, 0x0); // type and stream
    dataView.setUint8(4, 0x0); // version
    const encodeAccessKey = enc.encodeInto(
      creds.accessKey,
      uint8View.subarray(8),
    );
    if (!encodeAccessKey.written || encodeAccessKey.written > 27) {
      throw new Error("Unable to encode access key");
    }
    uint8View.set(new Uint8Array(sig), 43);
    const encodeDeviceId = enc.encodeInto(
      creds.deviceUuid,
      uint8View.subarray(75),
    );
    if (!encodeDeviceId.written || encodeDeviceId.written != 36) {
      throw new Error("Unable to encode device id");
    }
    let pos = 112;
    const encodeIsoDate = enc.encodeInto(now, uint8View.subarray(pos));
    switch (encodeIsoDate.written) {
      case 24:
        pos = 136;
        break;
      case 27:
        dataView.setUint8(111, 0x1);
        pos = 139;
        break;
      default:
        throw new Error("Unexpected ISO date length");
    }
    const encodeClientVersion = enc.encodeInto(
      clientVersion,
      uint8View.subarray(pos + 1),
    );
    if (encodeClientVersion.written && encodeClientVersion.written > 255) {
      throw new Error("Client version too long to encode");
    }
    dataView.setUint8(pos, encodeClientVersion.written || 0);
    pos = pos + 1 + (encodeClientVersion.written || 0);
    return buf.slice(0, pos);
  }

  private encodeGoawayMsg(
    lastStream: number,
    errorCode: number,
    msg: string,
  ): ArrayBuffer {
    if (lastStream >= 2 ** 24) {
      throw new Error("Cannot encode lastStream");
    }
    const buf = new ArrayBuffer(16 + msg.length * 4); // avoid resizing
    const uint8View = new Uint8Array(buf);
    const textEncoder = new TextEncoder();
    const encodeResult = textEncoder.encodeInto(msg, uint8View.subarray(16));
    const dataView = new DataView(buf);
    dataView.setUint8(0, 0x1); // type
    dataView.setUint32(4, lastStream, false);
    dataView.setUint32(8, errorCode, false);
    dataView.setUint32(12, encodeResult.written || 0, false);
    return buf.slice(0, 16 + (encodeResult.written || 0));
  }

  private encodePingMsg(): ArrayBuffer {
    const buf = new ArrayBuffer(4);
    const dataView = new DataView(buf);
    dataView.setUint8(0, 0x5); // type
    return buf;
  }

  private encodePongMsg(): ArrayBuffer {
    const buf = new ArrayBuffer(4);
    const dataView = new DataView(buf);
    dataView.setUint8(0, 0x6); // type
    return buf;
  }

  private encodeStreamDataMsg(stream: number, data: ArrayBuffer): ArrayBuffer {
    if (stream >= 2 ** 24) {
      throw new Error("Cannot encode stream");
    }
    const buf = new ArrayBuffer(4 + data.byteLength);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);
    dataView.setUint32(0, (0x2 << 24) | stream, false); // type and stream id
    uint8View.set(new Uint8Array(data), 4);
    return buf;
  }

  private encodeStreamCloseMsg(stream: number): ArrayBuffer {
    if (stream >= 2 ** 24) {
      throw new Error("Cannot encode stream");
    }
    const buf = new ArrayBuffer(4);
    const dataView = new DataView(buf);
    dataView.setUint32(0, (0x3 << 24) | stream, false); // type and stream id
    return buf;
  }

  private encodeStreamResetMsg(
    stream: number,
    errorCode: number,
    msg: string,
  ): ArrayBuffer {
    if (stream >= 2 ** 24) {
      throw new Error("Cannot encode stream");
    }
    const textEncoder = new TextEncoder();
    const buf = new ArrayBuffer(12 + msg.length * 4); // avoid resizing
    const uint8View = new Uint8Array(buf);
    const dataView = new DataView(buf);

    dataView.setUint32(0, (0x4 << 24) | stream, false); // type and stream id
    dataView.setUint32(4, errorCode, false);
    const encodeResult = textEncoder.encodeInto(msg, uint8View.subarray(12));
    dataView.setUint32(8, encodeResult.written || 0, false);
    return buf.slice(0, 12 + (encodeResult.written || 0));
  }

  private decode(msg: ArrayBuffer): MuxMessage | null {
    const dv = new DataView(msg);
    const typeAndStream = dv.getUint32(0, false);
    const type = typeAndStream >>> 24;
    const stream = typeAndStream & 0xffffff;
    switch (type) {
      case 0: {
        // auth
        return {
          type: "auth",
        };
      }
      case 1: {
        // goaway
        const msgLength = dv.getUint32(12, false);
        const errorMsgBytes = new Uint8Array(msg, 16, msgLength);
        const td = new TextDecoder();
        const errorMsg = td.decode(errorMsgBytes);
        return {
          type: "goaway",
          lastStream: dv.getUint32(4, false),
          errorCode: dv.getUint32(8, false),
          msg: errorMsg,
        };
      }
      case 2: {
        // stream data
        return {
          type: "data",
          stream: stream,
          payload: msg.slice(4),
        };
      }
      case 3: {
        // stream close
        return {
          type: "close",
          stream: stream,
        };
      }
      case 4: {
        // stream reset
        const msgLength = dv.getUint32(8, false);
        const errorMsgBytes = new Uint8Array(msg, 12, msgLength);
        const td = new TextDecoder();
        const errorMsg = td.decode(errorMsgBytes);
        return {
          type: "reset",
          stream: stream,
          errorCode: dv.getUint32(4, false),
          msg: errorMsg,
        };
      }
      case 5: {
        // ping
        if (stream != 0) {
          return null;
        }
        switch (this.state) {
          case MuxedSocketState.AUTH_SENT:
          case MuxedSocketState.CLOSEWAIT:
            this.socket.send(this.encodePongMsg());
            break;
          case MuxedSocketState.IDLE:
          case MuxedSocketState.CLOSING:
          case MuxedSocketState.CLOSED:
            break;
        }
        return null;
      }
      case 6: {
        // pong
        if (stream != 0) {
          return null;
        }
        for (const resolve of this.healthChecks) {
          resolve(true);
        }
        return null;
      }
      default:
        return null;
    }
  }
}

enum StreamState {
  OPEN,
  CLOSING,
  CLOSEWAIT,
  CLOSED,
}

class Stream {
  // constants
  private socket: MuxedSocket;
  private streamId: number;

  // state
  private state: StreamState = StreamState.OPEN;

  // user facing interface ///////////////////////////////////

  onClose?: () => void;
  onError?: (errorCode: number, msg: string) => void;
  onData?: (data: ArrayBuffer) => void;

  close(): void {
    switch (this.state) {
      case StreamState.CLOSING:
      case StreamState.CLOSED:
        break;
      case StreamState.OPEN:
        this.state = StreamState.CLOSING;
        this.socket.streamClose(this.streamId, false);
        break;
      case StreamState.CLOSEWAIT:
        this.state = StreamState.CLOSED;
        this.socket.streamClose(this.streamId, true);
        break;
    }
  }

  error(errorCode: number, msg: string): void {
    switch (this.state) {
      case StreamState.CLOSED:
      case StreamState.CLOSING:
        this.state = StreamState.CLOSED;
        break;
      case StreamState.OPEN:
      case StreamState.CLOSEWAIT:
        this.state = StreamState.CLOSED;
        this.socket.streamError(this.streamId, errorCode, msg);
        break;
    }
  }

  send(data: ArrayBuffer): void {
    switch (this.state) {
      case StreamState.CLOSING:
      case StreamState.CLOSED:
        break;
      case StreamState.OPEN:
      case StreamState.CLOSEWAIT:
        this.socket.streamSend(this.streamId, data);
    }
  }

  // interface used by MuxedSocket ///////////////////////////

  constructor(socket: MuxedSocket, streamId: number) {
    this.socket = socket;
    this.streamId = streamId;
  }

  onStreamClose(): boolean {
    switch (this.state) {
      case StreamState.CLOSED:
        return true;
      case StreamState.CLOSEWAIT:
        return false;
      case StreamState.OPEN:
        this.state = StreamState.CLOSEWAIT;
        if (this.onClose) {
          this.onClose();
        }
        return false;
      case StreamState.CLOSING:
        this.state = StreamState.CLOSED;
        if (this.onClose) {
          this.onClose();
        }
        return true;
    }
  }

  onStreamError(errorCode: number, msg: string): void {
    switch (this.state) {
      case StreamState.CLOSED:
        break;
      case StreamState.CLOSING:
      case StreamState.OPEN:
      case StreamState.CLOSEWAIT:
        this.state = StreamState.CLOSED;
        if (this.onError) {
          this.onError(errorCode, msg);
        }
    }
  }

  onStreamData(data: ArrayBuffer): void {
    switch (this.state) {
      case StreamState.CLOSED:
      case StreamState.CLOSEWAIT:
        break;
      case StreamState.CLOSING:
      case StreamState.OPEN:
        if (this.onData) {
          this.onData(data);
        }
    }
  }
}

/**
 * Connects to a remote SKDB service.
 * @param endpoint Remote SKDB endpoint url.
 * @param db Name of the remote db.
 * @param creds Credentials, including private key and device UUID.
 * @param schemas Requested schemas.
 * @returns SKDB
 */
export async function connect(
  endpoint: string,
  db: string,
  creds: Creds,
  schemas: MirrorDefn[],
): Promise<SKDB> {
  return SKDB.connect(endpoint, db, creds, schemas);
}

/* ***************************************************************************/
/* Server-side database. */
/* ***************************************************************************/

class SKDB {
  private connection: ResilientMuxedSocket;
  private creds: Creds;
  private replicationUid: string = "";
  private mirrorStreams = new Set<ResilientStream>();
  private onRebootFn?: () => void;
  private watermark: bigint = BigInt(0);
  private data = new Map<string, Map<string, [number, Record<string, any>]>>();
  private callbacks = new Map<string, (added: SKDBTable, removed: SKDBTable) => void>();

  private constructor(
    connection: ResilientMuxedSocket,
    creds: Creds,
  ) {
    this.connection = connection;
    this.creds = creds;
  }

  static async connect(
    endpoint: string,
    db: string,
    creds: Creds,
    schemas: MirrorDefn[],
  ): Promise<SKDB> {
    const uri = SKDB.getDbSocketUri(endpoint, db);

    const policy: ResiliencyPolicy = {
      notifyFailedStream() {
        // Do nothing.
      },
      async shouldReconnect(socket: ResilientMuxedSocket): Promise<boolean> {
        // perform an active check
        return !socket.isSocketResponsive();
      },
    };
    const conn = await ResilientMuxedSocket.connect(policy, uri, creds);

    const server = new SKDB(conn, creds);
    // FIXME
    server.replicationUid = creds.deviceUuid;//client.getReplicationUid(creds.deviceUuid);

    await server.mirror(schemas);

    return server;
  }

  async connectedAs(): Promise<string> {
    return this.creds.accessKey;
  }

  async close() {
    this.connection.closeSocket();
  }

  private static getDbSocketUri(endpoint: string, db: string) {
    return `${endpoint}/dbs/${db}/connection`;
  }

  private strictCastData(response: ProtoMsg | null): ProtoData {
    if (response === null) {
      throw new Error(`Unexpected response: ${response}`);
    }
    if (response.type === "data") {
      return response;
    }
    throw new Error(`Unexpected response: ${response}`);
  }

  private deliverDataTransferProtoMsg(
    msg: ProtoMsg | null,
    deliver: (msg: string) => void,
  ) {
    const txtPayload = decodeUTF8(this.strictCastData(msg).payload);
    const rebootSignalled = txtPayload
      .split("\n")
      .find((line) => line.trim() == ":reboot");
    if (rebootSignalled) {
      this.close();
      this.callOnReboot();
      return;
    }
    deliver(txtPayload);
  }

  private callOnReboot(): void {
    if (this.onRebootFn) {
      this.onRebootFn();
    } else {
      throw new Error(
        "Server signalled client should cold start to avoid diverging.",
      );
    }
  }

  private async makeRequest(
    request: ProtoCtrlMsg,
  ): Promise<ProtoResponse | null> {
    const stream = await this.connection.openStream();
    const decoder = new ProtoMsgDecoder();
    return new Promise((resolve, reject) => {
      stream.onData = function (data) {
        decoder.push(data);
      };
      stream.onClose = () => {
        const msg = decoder.tryPop();
        if (
          msg === null ||
          (msg.type !== "credentials" && msg.type !== "data")
        ) {
          resolve(null);
          return;
        }
        resolve(msg);
      };
      stream.onError = (_code, msg) => reject(msg);
      stream.send(encodeProtoMsg(request));
      stream.close();
    });
  }

  private async makeStringRequest(request: ProtoCtrlMsg): Promise<string> {
    return this.makeRequest(request).then((result) =>
      decodeUTF8(this.strictCastData(result).payload),
    );
  }

  private async establishServerTail(mirrorDefs: MirrorDefn[]): Promise<void> {
    const stream = await this.connection.openResilientStream();
    this.mirrorStreams.add(stream);
    const decoder = new ProtoMsgDecoder();

    let resolved = false;

    const buildTailRequest = (tick: bigint) => {
      const reqs: ProtoRequestTail[] = mirrorDefs.map((def) => {
        return {
          type: "tail",
          table: def.table,
          expectedColumns: def.expectedColumns,
          since: tick,
          filterExpr: def.filterExpr ?? "",
          params: def.filterParams ?? new Map(),
        };
      });
      return encodeProtoMsg({
        type: "tailBatch",
        requests: reqs,
      });
    };
    return new Promise((resolve) => {
      stream.onData = (data) => {
        if (decoder.push(data)) {
          const msg = decoder.pop();
          let resolveSignalled = false;
          this.deliverDataTransferProtoMsg(msg, (payload) => {
            if (!resolved) {
              // a non-zero checkpoint indicates that we have received a fully consistent
              // snapshot of the remote table, so should resolve the promise
              for (const line of payload.split("\n")) {
                if (line.trim() === "!rebuild") {
                  resolveSignalled = false;
                  break;
                }
                resolveSignalled ||= (line.match(/^:[1-9]/g) != null);
              }
            }

            this.mergeUpdates(payload);
          });
          if (!resolved && resolveSignalled) {
            resolved = true;
            resolve();
          }
        }
        stream.expectingData();
      };

      stream.onReconnect = () => {
        stream.send(buildTailRequest(this.watermark));
        stream.expectingData();
      };

      stream.send(buildTailRequest(BigInt(0)));
      stream.expectingData();
    });
  }

  private mergeUpdates(payload: string) {
    let curTable = null;
    let curData = null;
    let changedTables = [];
    let added = new Map();
    let removed = new Map();

    for (const line of payload.split("\n")) {
      if (line == "") {
        continue;
      }
      if (line.match(/^:[1-9]/g) != null) {
        this.watermark = BigInt(line.substring(1));
        for (const curTable of changedTables) {
          const cb = this.callbacks.get(curTable);
          if (cb !== undefined) {
            cb(
              SKDBTable.ofRows(added.get(curTable)),
              SKDBTable.ofRows(removed.get(curTable))
            );
          }
        }

        changedTables = [];
        added = new Map();
        removed = new Map();
      } else if (line.startsWith("^")) {
        curTable = line.substring(1);
        curData = this.data.get(curTable);
        changedTables.push(curTable);
        added.set(curTable, []);
        removed.set(curTable, []);
      } else {
        const parts = line.split("\t", 2);
        const row = parts[1];
        const cnt = parseInt(parts[0]);
        const prev_record = curData!.get(row);
        const prev_cnt = (prev_record === undefined) ? 0 : prev_record[0];

        const values = csv2array(row);
        if (prev_cnt < cnt) {
          added.get(curTable).push([cnt - prev_cnt, values]);
        } else {
          removed.get(curTable).push([prev_cnt - cnt, values]);
        }
        if (cnt == 0) {
          curData!.delete(row);
        } else {
          curData!.set(row, [cnt, values]);
        }
      }
    }
  }

  subscribe(
    table: string,
    init_cb: (rows: SKDBTable) => void,
    update_cb: (added: SKDBTable, removed: SKDBTable) => void
  ): (() => void) {
    // TODO: Fail if attempting to subscribe to non-mirrored table.
    // TODO: Allow multiple callbacks?
    this.callbacks.set(table, update_cb);

    init_cb(this.getTable(table));

    return () => { this.callbacks.delete(table); }
  }

  getTable(name: string): SKDBTable {
    return SKDBTable.ofRows(Array.from(this.data.get(name)!.values()));
  }

  // async write(payload: JSON) {
  //  return await fetch(this.endpoint, {
  //     method: "POST",
  //     body: JSON.stringify(payload),
  //     headers: {
  //       "Content-type": "application/json; charset=UTF-8"
  //     },
  //   })
  // }

  async isConnectionHealthy() {
    return this.connection.isSocketConsideredHealthy();
  }

  private async mirror(tables: MirrorDefn[]): Promise<void> {
    if (tables.length < 1) {
      throw new Error("Must specify at least one table to mirror");
    }
    const setupTable = async (mirror_defn: MirrorDefn) => {
      const tableName: string = mirror_defn.table;
      const remoteTable = await this.tableSchema(tableName);

      if (remoteTable == "") {
        throw new Error("Table " + tableName + " does not exist");
      }

      this.data.set(tableName, new Map());
    };

    // mirror has replace semantics. start by tearing down any current
    // mirror session. re-establishing is relatively cheap.
    for (const stream of this.mirrorStreams) {
      stream.close();
    }
    this.mirrorStreams.clear();

    await Promise.all(tables.map(setupTable));

    await this.establishServerTail(tables);
  }

  async exec(stdin: string, params: Params = new Map()): Promise<SKDBTable> {
    if (params instanceof Map) {
      params = Object.fromEntries(params);
    }
    stdin = JSON.stringify(params) + "\n" + stdin;
    return this.makeStringRequest({
      type: "query",
      query: stdin,
      format: "json",
    }).then((result) => {
      const rows = result
        .split("\n")
        .filter((x) => x != "")
        .map((x) => JSON.parse(x));
      return SKDBTable.ofRows(rows);
    });
  }

  async tableSchema(
    tableName: string,
    suffix: string = "",
    legacySchema: boolean = false,
  ): Promise<string> {
    return this.makeStringRequest({
      type: "schema",
      name: tableName,
      scope: legacySchema ? "legacy_schema" : "table",
      suffix: suffix,
    });
  }

  async viewSchema(viewName: string): Promise<string> {
    return this.makeStringRequest({
      type: "schema",
      name: viewName,
      scope: "view",
      suffix: "",
    });
  }

  async schema(): Promise<string> {
    return this.makeStringRequest({
      type: "schema",
      scope: "all",
    });
  }

  async createDatabase(dbName: string): Promise<ProtoResponseCreds> {
    return this.makeRequest({
      type: "createDatabase",
      name: dbName,
    }).then((result) => {
      if (result === null || result.type !== "credentials") {
        throw new Error("Unexpected response.");
      }
      return result;
    });
  }

  async createUser(): Promise<ProtoResponseCreds> {
    return this.makeRequest({
      type: "createUser",
    }).then((result) => {
      if (result === null || result.type !== "credentials") {
        throw new Error("Unexpected response.");
      }
      return result;
    });
  }

  async onReboot(fn: () => void) {
    this.onRebootFn = fn;
  }
}

export default {
  connect,
};
