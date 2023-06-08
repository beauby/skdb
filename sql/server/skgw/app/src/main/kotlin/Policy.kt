package io.skiplabs.skgw

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

// this factors out policy and monitoring from mechanism
interface ServerPolicy {
  fun shouldAcceptConnection(db: String): Boolean

  // you can then hook in to socket events by using socket.observeLifecycle
  fun notifySocketCreated(socket: MuxedSocket, db: String)

  // short-circuits message handling if returns false. may manipulate
  // the stream - e.g. send/close/error it.
  fun shouldHandleMessage(request: ProtoMessage, stream: Stream): Boolean
}

// no policy. useful for subclasses that want to define partial policy
// -- template method pattern -- or as the default in composition
open class NullServerPolicy : ServerPolicy {
  override fun shouldAcceptConnection(db: String): Boolean {
    return true
  }

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {}

  override fun shouldHandleMessage(request: ProtoMessage, stream: Stream): Boolean {
    return true
  }
}

val MAX_CONNECTIONS: UInt = 10_000u

class LimitGlobalConnections(val maxConns: UInt = MAX_CONNECTIONS) : NullServerPolicy() {

  val n: AtomicInteger = AtomicInteger(0)

  override fun shouldAcceptConnection(db: String): Boolean {
    val n = n.get()
    val acceptable = n < maxConns.toInt()
    if (!acceptable) {
      System.err.println("Rejecting conn as ${n} >= ${maxConns}")
    }
    return acceptable
  }

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {
    n.incrementAndGet()
    socket.observeLifecycle { state ->
      when (state) {
        MuxedSocket.State.CLOSED -> n.decrementAndGet()
        MuxedSocket.State.IDLE,
        MuxedSocket.State.AUTH_RECV,
        MuxedSocket.State.CLOSING,
        MuxedSocket.State.CLOSE_WAIT -> Unit
      }
    }
  }
}

class LimitConnectionsPerDb(val maxConnsPerDatabase: UInt) : NullServerPolicy() {

  val openConns: ConcurrentMap<String, Int> = ConcurrentHashMap()

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {
    val n = openConns.merge(db, 1) { oldvalue, _ -> oldvalue + 1 } ?: 1

    socket.observeLifecycle { state ->
      when (state) {
        MuxedSocket.State.CLOSED -> openConns.merge(db, 0) { oldvalue, _ -> oldvalue - 1 }
        MuxedSocket.State.IDLE,
        MuxedSocket.State.AUTH_RECV,
        MuxedSocket.State.CLOSING,
        MuxedSocket.State.CLOSE_WAIT -> Unit
      }
    }

    if (n > maxConnsPerDatabase.toInt()) {
      socket.errorSocket(2000u, "Database connection limit reached")
    }
  }
}

class LimitConnectionsPerUser(val maxConnsPerUser: UInt) : NullServerPolicy() {

  data class DbUser(val user: String, val db: String)

  val openConns: ConcurrentMap<DbUser, Int> = ConcurrentHashMap()

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {
    socket.observeLifecycle { state ->
      val user = socket.authenticatedWith?.msg?.accessKey

      if (user != null) {
        val key = DbUser(user, db)
        when (state) {
          MuxedSocket.State.CLOSED -> {
            openConns.merge(key, 0) { oldvalue, _ ->
              val n = oldvalue - 1
              if (n < 1) null else n
            }
          }
          MuxedSocket.State.AUTH_RECV -> {
            val n = openConns.merge(key, 1) { oldvalue, _ -> oldvalue + 1 }
            if (n != null && n > maxConnsPerUser.toInt()) {
              socket.errorSocket(2000u, "User connection limit reached")
            }
          }
          MuxedSocket.State.IDLE,
          MuxedSocket.State.CLOSING,
          MuxedSocket.State.CLOSE_WAIT -> Unit
        }
      }
    }
  }
}

class TokenBucket(val arrivalRatePerSecond: Double, val size: UInt) {
  private var mutex: Lock = ReentrantLock()
  private var tokens: UInt = size
  private var lastArrival: Instant = Instant.now()

  // null if can immediately service otherwise a non-zero duration
  // estimate of how long to wait before the request should be
  // servicable - be careful, this could easily lead to unfairness
  // without a queue.
  fun requestTokens(n: UInt): Duration? {
    mutex.lock()
    try {
      val now = Instant.now()
      val deltaMillis = Duration.between(lastArrival, now).toMillis()
      val oldTokens = tokens
      tokens = Math.floor(tokens.toLong() + (deltaMillis * arrivalRatePerSecond / 1000)).toUInt()
      if (tokens > oldTokens) {
        // this condition protects the floor from starving updates
        // that arrive fast enough. it ensures progress
        lastArrival = now
      }
      // clip to prevent accumulating enough tokens to allow unacceptable spikes
      tokens = Math.min(tokens.toLong(), size.toLong()).toUInt()

      if (n <= tokens) {
        tokens = tokens - n
        return null
      }

      return Duration.ofMillis(((n - tokens).toLong() / arrivalRatePerSecond * 1000).toLong())
    } finally {
      mutex.unlock()
    }
  }
}

class RateLimitRequestsPerConnection(val maxQpsPerConn: UInt, val maxSpikePerConn: UInt) :
    NullServerPolicy() {

  val openConns: ConcurrentMap<MuxedSocket, TokenBucket> = ConcurrentHashMap()

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {
    openConns.put(socket, TokenBucket(maxQpsPerConn.toDouble(), maxSpikePerConn))
    socket.observeLifecycle { state ->
      when (state) {
        MuxedSocket.State.IDLE,
        MuxedSocket.State.AUTH_RECV,
        MuxedSocket.State.CLOSING,
        MuxedSocket.State.CLOSE_WAIT -> Unit
        MuxedSocket.State.CLOSED -> {
          openConns.remove(socket)
        }
      }
    }
  }

  override fun shouldHandleMessage(request: ProtoMessage, stream: Stream): Boolean {
    val socket = stream.socket
    val tokenBucket = openConns.get(socket)
    if (tokenBucket == null) {
      // something went really wrong
      return true
    }
    val wait = tokenBucket.requestTokens(1u)
    if (wait == null) {
      return true
    }
    stream.error(2000u, "Rate limit exceeded")
    return false
  }
}

class SimpleDebugLogger(val decorated: ServerPolicy) : ServerPolicy {

  override fun shouldAcceptConnection(db: String): Boolean {
    val shouldAccept = decorated.shouldAcceptConnection(db)
    val phrase = if (shouldAccept) "accepted" else "rejected"
    System.err.println("Connection for db ${db} ${phrase}.")
    return shouldAccept
  }

  override fun notifySocketCreated(socket: MuxedSocket, db: String) {
    System.err.println("Socket created: ${socket} for db ${db}.")
    socket.observeLifecycle { state ->
      System.err.println("Socket ${socket} moved to state ${state}")
    }
    decorated.notifySocketCreated(socket, db)
  }

  override fun shouldHandleMessage(request: ProtoMessage, stream: Stream): Boolean {
    val shouldHandle = decorated.shouldHandleMessage(request, stream)
    val phrase = if (shouldHandle) "accepted" else "rejected"
    System.err.println("Request ${request} on stream ${stream} was ${phrase}")
    return shouldHandle
  }
}