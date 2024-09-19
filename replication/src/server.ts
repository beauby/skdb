import { WebSocket, WebSocketServer, MessageEvent } from "ws";
import type {
  JSONObject,
  TJSON,
  ProtoPing,
  ProtoPong,
  ProtoAuth,
  ProtoGoAway,
  ProtoRequestTail,
  ProtoAbortTail,
  ProtoRequestTailBatch,
  ProtoData,
  Creds,
} from "./protocol";
import * as Protocol from "./protocol";

class TailingSession {
  private watermarks = new Map<string, bigint>();

  constructor(private pubkey: ArrayBuffer) {}

  subscribe(
    collection: string,
    since: bigint,
    callback: (updates: Array<KeyValue>, isInit: boolean, tick: bigint) => void,
  ): [Array<KeyValue>, bigint] {
    // TODO: return initial data
    this.watermarks.set(collection, since);

    throw new Error("TODO");
  }

  unsubscribe(collection: string) {
    // TODO: unsubscribe from reactive service
    this.watermarks.delete(collection);
  }

  close() {
    for (let collection in this.watermarks) {
      this.unsubscribe(collection);
    }
  }
}

class ServerError {
  constructor(
    public code: number,
    public msg: string,
  ) {}
}

async function handleMessage(
  data: any,
  session: TailingSession,
  ws: WebSocket,
): Promise<void> {
  if (!(data instanceof ArrayBuffer)) {
    throw new ServerError(1001, "Received string WebSocket msg.");
  }

  const msg = Protocol.decodeMsg(data);
  switch (msg.type) {
    case "auth":
      throw new ServerError(1001, "Unexpected auth");
    case "ping": {
      ws.send(
        Protocol.encodeMsg({
          type: "pong",
        }),
      );
      return;
    }
    case "tail": {
      // FIXME: Respond with error 1004 if collection does not exist
      // (for current user).
      const [data, tick] = session.subscribe(
        msg.collection,
        msg.since,
        (updates, isInit, tick) => {
          ws.send(
            Protocol.encodeMsg({
              type: "data",
              collection: msg.collection,
              tick,
              payload: JSON.stringify(updates),
            }),
          );
        },
      );

      // Initial data.
      ws.send(
        Protocol.encodeMsg({
          type: "data",
          collection: msg.collection,
          tick,
          payload: JSON.stringify(data),
        }),
      );
      return;
    }
    case "aborttail": {
      session.unsubscribe(msg.collection);
      return;
    }
    case "tailbatch":
      throw new Error("TODO");
  }
}

async function handleAuthMessage(data: any): Promise<TailingSession> {
  if (!(data instanceof ArrayBuffer)) {
    throw new ServerError(1001, "Received string WebSocket msg.");
  }

  const msg = Protocol.decodeMsg(data);
  if (msg.type != "auth") {
    throw new ServerError(1002, "Authentication failed: Invalid message");
  }
  // FIXME: Check nonce uniqueness to avoid replay attack.
  // Timestamp allowed 60 seconds of drift.
  if (Math.abs(Date.now() - Number(msg.timestamp) * 1000) > 60 * 1000) {
    throw new ServerError(1002, "Authentication failed: Invalid timestamp");
  }

  const pubkey = await Protocol.importKey(msg.pubkey);
  if (await Protocol.verify(pubkey, msg.nonce, msg.timestamp, msg.signature)) {
    return new TailingSession(msg.pubkey);
  } else {
    throw new ServerError(1002, "Authentication failed: Invalid credentials");
  }
}

export class Server {
  private wss: WebSocketServer;
  private sessions = new Map<WebSocket, Promise<TailingSession>>();

  constructor(port: number) {
    this.wss = new WebSocket.Server({ port });
    this.wss.on("connection", this.onconnection);
  }

  close() {
    for (const [ws, session] of this.sessions) {
      session.then((session) => session.close());
      ws.send(
        Protocol.encodeMsg({
          type: "goaway",
          code: 1000,
          msg: "Server shutting down",
        }),
      );
    }
    this.wss.close();
  }

  private onconnection(ws: WebSocket) {
    ws.onmessage = (event) => this.onmessage(ws, event);
    ws.onclose = (_event) => this.onclose(ws);
  }

  private onmessage(ws: WebSocket, event: MessageEvent) {
    // First message must be auth.
    if (!this.sessions.has(ws)) {
      this.sessions.set(ws, handleAuthMessage(event.data));
      this.sessions.get(ws)!.catch((error) => this.errorHandler(ws, error));
    } else {
      void this.sessions
        .get(ws)!
        .then((session) => handleMessage(event.data, session, ws))
        .catch((error) => this.errorHandler(ws, error));
    }
  }

  private onclose(ws: WebSocket) {
    this.sessions.get(ws)?.then((session: TailingSession) => {
      session.close();
    });
  }

  private errorHandler(ws: WebSocket, error: any) {
    if (error instanceof ServerError) {
      ws.send(
        Protocol.encodeMsg({
          type: "goaway",
          code: error.code,
          msg: error.msg,
        }),
      );
      ws.close();
    } else {
      throw error;
    }
  }
}

export function run(port: number) {
  return new Server(port);
}
