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

## Layout

| Path | Contents | C counterpart |
|------|----------|---------------|
| `internal/reactor` | `Poller` interface, kqueue + epoll backends | `ae.c`, `ae_kqueue.c`, `ae_epoll.c` |
| `internal/resp` | resumable request parser, reply serializer | `networking.c` (processMultibulkBuffer / addReply*) |
| `internal/store` | object model, keyspace, string2ll, glob matcher | `object.c`, `db.c`, `util.c` |
| `internal/server` | event loop, accept, buffers, dispatch, stats | `server.c` + `networking.c` |
| `internal/server/commands_string.go` | GET/SET/INCR family | `t_string.c` |
| `internal/server/commands_keyspace.go` | DEL/EXISTS/KEYS/TYPE/OBJECT | `db.c`, `object.c` |
| `cmd/veloce` | binary entry point | `main()` in server.c |

## Run

```sh
go run ./cmd/veloce            # listens on 127.0.0.1:6379
go run ./cmd/veloce -port 7000 -bind 0.0.0.0
```

Talk to it with the real tooling (built from ../redis with `make redis-cli redis-benchmark`):

```sh
redis-cli -p 6379 PING                     # PONG
redis-cli -p 6379 ECHO hello               # "hello"
redis-benchmark -p 6379 -t ping -P 16 -q   # pipelined throughput
```

## Test

```sh
go test -race ./...                       # unit + integration (real TCP)
go test -bench=. ./internal/resp/         # parser throughput
GOOS=linux go build ./...                 # verify the epoll backend compiles
```

The two tests that guard the phase-1 trip hazards:

- `TestResumableEveryByteBoundary` / `TestDribbledCommand` — a command must
  parse identically no matter where the byte stream is cut (parser level and
  full server level).
- `TestLargeReplyExercisesWritePath` — a 4MB reply overflows the kernel
  socket buffer, forcing the partial-write → arm-write-interest → resume →
  disarm cycle.

## Design invariants (keep these while adding phases)

- **One goroutine owns everything.** Only the event-loop goroutine touches
  server, client, and (from Phase 2) keyspace state. No mutexes, no atomics.
  Anything read from other goroutines (tests read `Stats()`) must happen
  after the loop has stopped, or move to a message over the wake pipe.
- **Level-triggered polling, one 16KB read per readiness event** — keeps the
  loop fair across clients (Redis PROTO_IOBUF_LEN).
- **Protocol errors are fatal per connection**: reply with the error, flush,
  close — parser state is not recoverable, same as Redis.

## Oracle testing

The strongest verification: run the real thing next to veloce and diff.

```sh
(cd ../redis && make redis-server redis-cli -j8)
../redis/src/redis-server --port 16380 --save '' &
go run ./cmd/veloce -port 16381 &
../redis/src/redis-cli --no-raw -p 16380 < oracle_script.txt > real.txt
../redis/src/redis-cli --no-raw -p 16381 < oracle_script.txt > mine.txt
diff real.txt mine.txt   # phase 2: byte-identical on 53 commands
```

Caveat: avoid multi-result KEYS in oracle scripts (hash-iteration order is
random on both sides); use single-match patterns.

## Next: Phase 3 — serverCron & expiration

Time events on the reactor (the loop currently blocks forever in Wait — it
will need a timeout), lazy + active expiry, EXPIRE/TTL/PERSIST/SET-EX, and
the `TODO(ttl)` marker in `store.DB.Set` (plain SET clears TTL). Reference:
`../redis/src/expire.c` (activeExpireCycle, expireIfNeeded), `server.c`
(serverCron).
