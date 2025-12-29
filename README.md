# Veloce-cache

Redis built from scratch in Go, on a hand-rolled epoll/kqueue event loop —
following the phased plan in the parent project. The reference C
implementation lives in `../redis` (github.com/redis/redis).

## Status: Phase 2 complete — the Key-Value Core

- [x] Keyspace: `internal/store` — Go map dict, single DB, loop-goroutine-owned (no locks)
- [x] Object model: `Object{Type, Encoding, lruLfu, Val}` — type/encoding tags,
      no refcount (GC + pointer-identity shared guard instead)
- [x] Shared integer singletons 0..9999 (`SET k 100` allocates nothing)
- [x] Exact `string2ll` port: canonical-form-only integers ("007" stays embstr)
- [x] Encodings: int / embstr (≤44) / raw, verified via OBJECT ENCODING
- [x] Exact `stringmatchlen` port incl. the `skipLongerMatches` anti-blowup flag
- [x] Commands: GET, SET, DEL, EXISTS, KEYS, TYPE, OBJECT ENCODING,
      INCR, DECR, INCRBY, DECRBY (arity + error strings byte-identical to Redis)
- [x] Keyspace hit/miss stats (read-path lookups only, Redis parity)
- [x] **Oracle-verified**: a 53-command script run against real redis-server and
      veloce produces byte-identical output (values, encodings, error strings)
- [x] redis-benchmark: SET 2.1M / GET 3.0M / INCR 2.9M rps at -P 16 (on par
      with real Redis on the same box)

Phase 2 trip-hazard tests: `TestIncrDoesNotCorruptSharedSingleton` (INCR on a
key holding the shared 100-singleton must not change what other keys read)
and `TestMatchAdversarialPattern` (glob `a*a*a*...b` must fail fast, not hang
the event loop).

## Phase 1 — the Reactor & the Wire Protocol

- [x] epoll (Linux) / kqueue (macOS/BSD) behind one `reactor.Poller` interface
- [x] Non-blocking listener, non-blocking client sockets (TCP_NODELAY, backlog 511)
- [x] Per-connection read (parser) and write (`out`) buffers
- [x] Read interest always on; write interest armed **only** when a reply
      doesn't fully drain, disarmed once flushed (the EPOLLOUT protocol)
- [x] Resumable RESP2 parser — parsing progress (`multibulklen`, `bulklen`,
      decoded args) survives arbitrary split points across `read()` calls
- [x] Inline protocol (`PING\r\n` over netcat works)
- [x] RESP2 serializer (simple strings, errors, integers, bulk, arrays)
- [x] Command dispatch table with Redis-style arity checking
- [x] `PING`, `ECHO`, `QUIT`, `COMMAND` (stub so redis-cli stays quiet)
- [x] Stats struct seeded (connections, commands, per-command calls, net bytes)
- [x] Pipelining — falls out of the parser loop, verified by test and
      `redis-benchmark -P 16`

