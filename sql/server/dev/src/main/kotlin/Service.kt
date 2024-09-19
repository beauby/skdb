package io.skiplabs.skdb.server

import io.skiplabs.skdb.DB_ROOT_USER
import io.skiplabs.skdb.ENV
import io.skiplabs.skdb.MuxedSocket
import io.skiplabs.skdb.MuxedSocketEndpoint
import io.skiplabs.skdb.MuxedSocketFactory
import io.skiplabs.skdb.OutputFormat
import io.skiplabs.skdb.ProtoCreateDb
import io.skiplabs.skdb.ProtoCreateUser
import io.skiplabs.skdb.ProtoCredentials
import io.skiplabs.skdb.ProtoData
import io.skiplabs.skdb.ProtoMessage
import io.skiplabs.skdb.ProtoPushPromise
import io.skiplabs.skdb.ProtoQuery
import io.skiplabs.skdb.ProtoRequestTail
import io.skiplabs.skdb.ProtoRequestTailBatch
import io.skiplabs.skdb.ProtoSchemaQuery
import io.skiplabs.skdb.QueryResponseFormat
import io.skiplabs.skdb.RevealableException
import io.skiplabs.skdb.SchemaScope
import io.skiplabs.skdb.Skdb
import io.skiplabs.skdb.Stream
import io.skiplabs.skdb.TailSpec
import io.skiplabs.skdb.UserConfig
import io.skiplabs.skdb.WebSocket
import io.skiplabs.skdb.createSkdb
import io.skiplabs.skdb.decodeProtoMsg
import io.skiplabs.skdb.encodeProtoMsg
import io.skiplabs.skdb.openSkdb
import io.undertow.Handlers
import io.undertow.Undertow
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.server.handlers.BlockingHandler
import io.undertow.server.handlers.PathTemplateHandler
import io.undertow.util.Headers
import io.undertow.util.HttpString
import io.undertow.util.Methods
import io.undertow.util.PathTemplateMatch
import io.undertow.util.StatusCodes
import io.undertow.websockets.spi.WebSocketHttpExchange
import java.io.BufferedOutputStream
import java.io.File
import java.io.OutputStream
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

fun genCredential(accessKey: String):  {
  val csrng = SecureRandom()

  // generate a 256 bit random key for the root user
  val plaintextRootKey = ByteArray(32)
  csrng.nextBytes(plaintextRootKey)
  val encryptedRootKey = plaintextRootKey
  val creds = Credentials(accessKey, plaintextRootKey, encryptedRootKey)

  return creds
}

fun createDb(dbName: String):  {
  val creds = genCredentials(DB_ROOT_USER)
  createSkdb(dbName, creds.b64encryptedKey())
  return creds
}

sealed interface StreamHandler {

  fun handleMessage(request: ProtoMessage, stream: Stream): StreamHandler

  fun handleMessage(message: ByteBuffer, stream: Stream) =
      handleMessage(decodeProtoMsg(message), stream)

  fun close() {}
}

class ProcessPipe(val proc: Process) : StreamHandler {

  private val stdin: OutputStream

  init {
    val stdin = proc.outputStream
    if (stdin == null) {
      throw RuntimeException("creating a pipe to a process that does not accept input")
    }
    this.stdin = BufferedOutputStream(stdin)
  }

  override fun handleMessage(request: ProtoMessage, stream: Stream): StreamHandler {
    when (request) {
      is ProtoData -> {
        val data = request.data
        stdin.write(data.array(), data.arrayOffset() + data.position(), data.remaining())
        if (request.finFlagSet) {
          stdin.flush()
        }
      }
      else -> {
        close()
        stream.error(2001u, "unexpected request on established connection")
      }
    }

    return this
  }

  override fun close() {
    proc.outputStream?.close()
    proc.destroy()
  }
}

class RequestHandler(
    val skdb: Skdb,
    val pubkey: ByteArray,
    val replicationId: String,
) : StreamHandler {

  override fun handleMessage(request: ProtoMessage, stream: Stream): StreamHandler {
    when (request) {
      is ProtoQuery -> {
        if (accessKey != DB_ROOT_USER) {
          stream.error(2002u, "Authorization error")
          return this
        }
        val format =
            when (request.format) {
              QueryResponseFormat.CSV -> OutputFormat.CSV
              QueryResponseFormat.JSON -> OutputFormat.JSON
              QueryResponseFormat.RAW -> OutputFormat.RAW
            }
        val result = skdb.sql(request.query, format, true)
        if (result.exitSuccessfully()) {
          stream.send(encodeProtoMsg(ProtoData(ByteBuffer.wrap(result.output), finFlagSet = true)))
          stream.close()
        } else {
          stream.error(2000u, result.decode())
        }
      }
      is ProtoSchemaQuery -> {
        val result =
            when (request.scope) {
              SchemaScope.ALL -> skdb.dumpSchema()
              SchemaScope.TABLE -> skdb.dumpTable(request.name!!, request.suffix!!, false)
              SchemaScope.VIEW -> skdb.dumpView(request.name!!)
              SchemaScope.LEGACY_SCHEMA -> skdb.dumpTable(request.name!!, request.suffix!!, true)
            }
        if (result.exitSuccessfully()) {
          stream.send(encodeProtoMsg(ProtoData(ByteBuffer.wrap(result.output), finFlagSet = true)))
          stream.close()
        } else {
          stream.error(2000u, result.decode())
        }
      }
      is ProtoCreateDb -> {
        stream.error(2003u, "DB creation not supported or necessary in the dev server.")
      }
      is ProtoCreateUser -> {
        stream.error(2003u, "user creation not supported or necessary in the dev server.")
      }
      is ProtoRequestTail -> {
        if (!skdb.canMirror(request.table, request.expectedSchema)) {
          stream.error(2003u, "Invalid schema for ${request.table}")
          return this
        }
        val proc =
            skdb.tail(
                accessKey,
                replicationId,
                mapOf(
                    request.table to
                        TailSpec(
                            request.since.toInt(),
                            request.filterExpr,
                            request.filterParams,
                            request.expectedSchema)),
                { data, shouldFlush -> stream.send(encodeProtoMsg(ProtoData(data, shouldFlush))) },
                { stream.error(2000u, "Unexpected EOF") },
            )
        return ProcessPipe(proc)
      }
      is ProtoRequestTailBatch -> {
        val spec = HashMap<String, TailSpec>()
        for (tailreq in request.requests) {
          spec.put(
              tailreq.table,
              TailSpec(
                  tailreq.since.toInt(),
                  tailreq.filterExpr,
                  tailreq.filterParams,
                  tailreq.expectedSchema))
        }
        val proc =
            skdb.tail(
                accessKey,
                replicationId,
                spec,
                { data, shouldFlush -> stream.send(encodeProtoMsg(ProtoData(data, shouldFlush))) },
                { stream.error(2000u, "Unexpected EOF") },
            )
        return ProcessPipe(proc)
      }
      is ProtoPushPromise -> {
        val proc =
            skdb.writeCsv(
                accessKey,
                replicationId,
                request.schemas,
                { data, shouldFlush -> stream.send(encodeProtoMsg(ProtoData(data, shouldFlush))) },
                { stream.error(2000u, "Unexpected EOF") })
        return ProcessPipe(proc)
      }
      is ProtoData -> {
        stream.error(2001u, "unexpected data on non-established connection")
      }
      else -> stream.error(2001u, "unexpected message")
    }
    return this
  }
}

fun connectionHandler(
    workerPool: ExecutorService,
    taskPool: ScheduledExecutorService,
): HttpHandler {
  return BlockingHandler(
      Handlers.websocket(
          MuxedSocketEndpoint(
              object : MuxedSocketFactory {
                override fun onConnect(
                    exchange: WebSocketHttpExchange,
                    channel: WebSocket
                ): MuxedSocket {
                  val pathParams =
                      exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters()
                  val db = pathParams["database"]

                  if (db == null) {
                    throw RuntimeException("database not provided")
                  }

                  var skdb = openSkdb(db)
                  if (skdb == null) {
                    createDb(db)
                    skdb = openSkdb(db)
                  }

                  var replicationId: String? = null
                  var accessKey: String? = null

                  val socket =
                      MuxedSocket(
                          channel = channel,
                          taskPool = taskPool,
                          onStream = { _, stream ->
                            var handler: AtomicReference<StreamHandler> =
                                AtomicReference(
                                    RequestHandler(
                                        skdb!!,
                                        accessKey!!,
                                        replicationId!!,
                                    ))

                            stream.observeLifecycle { state ->
                              when (state) {
                                Stream.State.CLOSED -> handler.get().close()
                                else -> Unit
                              }
                            }

                            stream.onData = { data ->
                              workerPool.execute {
                                try {
                                  handler.set(handler.get().handleMessage(data, stream))
                                } catch (ex: RevealableException) {
                                  ex.printStackTrace(System.err)
                                  stream.error(ex.code, ex.msg)
                                } catch (ex: Exception) {
                                  ex.printStackTrace(System.err)
                                  stream.error(2000u, "Internal error")
                                }
                              }
                            }
                            stream.onClose = {
                              // must happen on the worker to ensure ordering
                              workerPool.execute { stream.close() }
                            }
                            stream.onError = { _, _ -> }
                          },
                          onClose = { socket -> socket.closeSocket() },
                          onError = { _, _, _ -> },
                          log = { _, _, _ -> })

                  if (skdb == null) {
                    socket.errorSocket(1004u, "Could not open database")
                  }

                  return socket
                }
              })))
}

un createHttpServer(
    connectionHandler: HttpHandler,
): Undertow {
  var pathHandler =
      PathTemplateHandler()
          .add("/dbs/{database}/connection", connectionHandler)
  return Undertow.builder().addHttpListener(ENV.port, "0.0.0.0").setHandler(pathHandler).build()
}

fun main(args: Array<String>) {
  val arglist = args.toList()
  val configIdx = arglist.indexOf("--config")
  if (configIdx >= 0 && arglist.size > configIdx + 1) {
    val configFile = arglist.get(configIdx + 1)
    if (File(configFile).exists()) {
      ENV = UserConfig.fromFile(configFile)
    }
  }

  val taskPool = Executors.newSingleThreadScheduledExecutor()
  val workerPool = Executors.newSingleThreadExecutor()
  val connHandler = connectionHandler(workerPool, taskPool)
  val server = createHttpServer(connHandler)
  server.start()

  println("SKDB dev server has started")
  println("------------------------------------------------------")
}
