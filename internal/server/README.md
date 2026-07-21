# `internal/server` — the event-loop server

This package is the heart of veloce: a **single-threaded, event-driven** TCP
server, the Go analog of Redis's `server.c` + `networking.c`. One goroutine
owns everything — the listener, the poller, every client, the keyspace, and
the stats — so command execution is serial and atomic **by construction, with
no locks**.

The Redis request path it mirrors:

```
readQueryFromClient → processInputBuffer → processCommand → addReply
                    → writeToClient (with EPOLLOUT re-arm on partial write)
```

---

## 1. The core idea in one picture

Every TCP connection is just an integer **file descriptor (fd)**. Instead of
one thread per connection, we register all fds with the kernel poller
(epoll on Linux, kqueue on macOS — see `internal/reactor`) and let one
goroutine ask "which fds are ready?" then service exactly those.

```
                     ┌──────────────────────────────────────────┐
   TCP clients       │           ONE goroutine: Serve()          │
  ┌────────┐         │                                           │
  │ client │─fd 7──┐ │   for !stopping {                         │
  └────────┘       │ │      n := poller.Wait(events)   ◀──sleeps │
  ┌────────┐       ├─┼─▶    for each ready event:                │
  │ client │─fd 9──┤ │         route by fd → handler             │
  └────────┘       │ │   }                                       │
  listener ─fd 3───┘ │                                           │
                     │   owns (no locks): clients, db, stats     │
                     └──────────────────┬───────────────────────┘
                                        │ "who is ready?"
                          ┌─────────────▼──────────────┐
                          │  kernel poller (epoll/kqueue)│
                          └──────────────────────────────┘
```

**Two rules make this safe:**
1. Only the loop goroutine touches `clients`, `db`, `stats`, and calls
   `unix.Read`/`Write`/`Accept`. (The lone exception: `Stop` writes one byte
   to the wake pipe from another goroutine — a different fd, used precisely to
   poke the loop.)
2. All sockets are **non-blocking**, so acting on a "ready" fd can never stall
   the single goroutine.

---

## 2. Lifecycle: startup → loop → shutdown

```
 New()                    Serve()  (blocks here)                  Stop()
 ─────                    ─────────────────────                   ──────
 reactor.New()            ┌──────────────────────────┐            writes 1 byte
 listenTCP()   ──fd 3──▶  │ poller.Wait ── ready ──┐  │            to wake pipe
 pipe() (wake)            │   ▲                    │  │               │
 poller.Add(listenFd)     │   │   route by fd:     ▼  │   ◀───────────┘
 poller.Add(wakeR)        │   │   ┌─ listenFd → acceptClients
 → returns *Server        │   │   ├─ wakeR    → drainWakePipe → stopping=true
                          │   │   └─ client   → handleReadable / flushOutput
                          │   └────────────────────┘  │
                          └──────────────┬───────────┘
                                         │ loop exits
                                    defer closeAll()  → close every fd, poller
```

---

## 3. Types

### `Server` (server.go)
Owns all server-wide state.

| Field | Purpose |
|-------|---------|
| `poller` | the epoll/kqueue readiness registry (`reactor.Poller`) |
| `listenFd` | the listening socket fd (the "front door") |
| `addr` | the actually-bound address (resolves port 0 → real port) |
| `clients` | `map[fd]*client` — every connected client, keyed by fd |
| `db` | the keyspace (`*store.DB`) — loop-owned |
| `stats` | counters (connections, commands, bytes, protocol errors) |
| `wakeR`, `wakeW` | the self-pipe read/write ends used by `Stop` |
| `stopping` | set true to make the loop exit |
| `Logger` | optional; nil silences logging |

### `client` (server.go)
Per-connection state (a trimmed `client` struct from Redis's `server.h`).

| Field | Purpose |
|-------|---------|
| `fd` | this client's socket fd |
| `addr` | remote address string (for logging) |
| `parser` | the resumable RESP parser holding this client's partial input |
| `out` | pending reply bytes not yet written to the socket |
| `wantWrite` | true while write-interest is armed with the poller (see §5) |
| `closeAfterReply` | true on QUIT / fatal protocol error: flush, then close |

---

## 4. Functions — what each one does

### Setup

**`New(host, port) (*Server, error)`**
Builds everything the loop needs, but does **not** start it:
1. create the poller,
2. `listenTCP` → the listening fd,
3. create the wake self-pipe (`unix.Pipe`),
4. assemble the `Server` (incl. a fresh `store.NewDB()`),
5. register **both** the listener fd and the wake-pipe read end with the
   poller for readable interest.
On any failure it tears down what it built (`closeAll`) and returns the error.

**`listenTCP(host, port) (fd, addr, error)`**
Creates the listening socket — Redis's `anetTcpServer` distilled. Five steps:
`unix.Socket` → `SO_REUSEADDR` → `Bind` → `Listen(511)` → `SetNonblock`, plus
`Getsockname` at the end to read back the real port (matters when `port==0`
means "pick any free port"). Returns the raw fd; **does not accept anything**.
Uses a `closeOnErr` closure as the Go stand-in for C's `goto err:` cleanup so
no fd leaks on a mid-setup failure.

**`sockaddrString(sa) string`**
Formats a `unix.Sockaddr` as `"1.2.3.4:port"`. Used for `addr` and client
logging.

### The loop

**`Serve() error`** — this is Redis's `aeMain`.
Loops until `stopping`:
1. `poller.Wait(events, -1)` — sleeps the goroutine until ≥1 fd is ready
   (or `Stop` pokes the wake pipe),
2. for each ready event, route by `ev.Fd`:
   - **listener fd** → `acceptClients` (new connections waiting),
   - **wake fd** → `drainWakePipe` (Stop was called),
   - **any client fd** → `handleReadable` if readable, then `flushOutput`
     if writable.

Note the two liveness re-checks in the client branch: a client can be closed
earlier in the *same* event batch (e.g. a read error), so we look it up fresh
before touching it, and again between the readable and writable handling. A
`defer closeAll()` guarantees cleanup when the loop exits.

**`Stop()`**
Writes one byte to the wake pipe. That makes the wake-pipe read end readable,
which wakes a blocked `poller.Wait`; the loop then routes to `drainWakePipe`,
which sets `stopping = true`. Safe to call from any goroutine — it's the only
cross-goroutine touch, and it targets a dedicated fd, not shared state.

**`drainWakePipe()`**
Drains the wake pipe (so it doesn't stay readable) and sets `stopping = true`.

### Per-fd handlers

**`acceptClients()`** — Redis's `acceptTcpHandler`.
Loops `unix.Accept` until `EAGAIN` (the queue is drained), capped at
`maxAcceptsPerCall` so a connect storm can't starve the loop. For each new fd:
set non-blocking + close-on-exec + `TCP_NODELAY`, build a `client` with a
fresh parser, register the fd with the poller (readable), and add it to
`clients`. Because the listener is non-blocking, `EAGAIN` cleanly means
"nothing left to accept right now."

**`handleReadable(c)`** — Redis's `readQueryFromClient` + `processInputBuffer`.
1. `unix.Read` one 16KB chunk. `EAGAIN`/`EINTR` → return; error → close;
   `n == 0` (EOF, client hung up) → close.
2. `parser.Feed(bytes)` the raw bytes into this client's parser.
3. **Drain loop:** repeatedly `parser.Next()`:
   - a complete command → `dispatch(c, args)` (appends its reply to `c.out`),
   - `nil` → not enough bytes yet, stop and wait for the next read,
   - a protocol error → append the error, set `closeAfterReply`, stop.
   This drain loop is why **pipelining is free** — many commands in one read
   are all processed before we reply.
4. `flushOutput(c)` to send whatever replies accumulated.

**`flushOutput(c)`** — Redis's `writeToClient` + the EPOLLOUT protocol.
Writes `c.out` to the socket:
- wrote everything → clear `c.out`, and if write-interest was armed, **disarm**
  it (`poller.Modify(fd, read, false)`); then honor `closeAfterReply`;
- `EAGAIN` (kernel send buffer full) → **arm** write-interest
  (`poller.Modify(fd, read, true)`), keep the unsent bytes, and return; the
  loop will call `flushOutput` again when the fd next becomes writable;
- `EINTR` → retry; other error → close.

See §5 for why arming/disarming matters.

### Teardown

**`closeClient(c)`**
Remove from `clients`, decrement the connected count, deregister the fd from
the poller, close the fd. Idempotent (guards on map membership) — safe to call
twice, which happens naturally given the loop's liveness re-checks.

**`closeAll()`**
Close every client, then the listener, the wake pipe, and the poller. Run via
`defer` in `Serve`, so it fires however the loop exits.

**`Stats() Stats`**
Returns a snapshot of the counters, folding in the keyspace hit/miss numbers
owned by `db`. Race-free **only after the loop has stopped** (or when called
from within it) — same single-owner contract as everything else.

---

## 5. The write path & `EPOLLOUT` (the subtle part)

Reading is always driven by "the fd is readable." Writing is trickier: a reply
usually fits in the kernel's send buffer and goes out immediately — but a big
reply can fill that buffer, and a non-blocking `Write` then returns `EAGAIN`
with the reply only partially sent.

```
 flushOutput: while len(c.out) > 0:
      unix.Write(c.out)
        ├─ wrote all ─────▶ c.out = nil; if wantWrite: DISARM write interest
        │                                 honor closeAfterReply
        └─ EAGAIN (full) ─▶ ARM write interest (Modify fd, read+WRITE)
                            keep c.out; return
                                     │
                            ...kernel drains buffer...
                                     ▼
                       poller.Wait reports {fd, writable}
                                     ▼
                       Serve → flushOutput(c) again → resumes ↺
```

Why **disarm** once drained? A socket with free send-buffer space is *almost
always* writable, so if we left write-interest armed, `poller.Wait` would
report it as writable on every single iteration — spinning the loop at 100%
CPU for nothing. So the rule is: **ask to hear about writability only while you
actually have queued bytes to send.** That's exactly what the `c.wantWrite`
flag tracks.

---

## 6. Command dispatch (see also `commands*.go`)

`Serve` handles *transport*; command *semantics* live in the dispatch layer:

- **`commands.go`** — `commandTable` (name → arity + handler) and `dispatch`,
  the analog of `processCommand` + `call`: uppercase the name, look it up,
  enforce arity, run the handler, bump stats. Handlers are
  `func(s *Server, c *client, args [][]byte)` and simply **append their reply
  to `c.out`** — never touching the socket directly. Because the loop is
  serial, a handler that both mutates `db` and writes a reply is atomic.
- **`commands_string.go`** — GET, SET, INCR/DECR family (`t_string.c`).
- **`commands_keyspace.go`** — DEL, EXISTS, KEYS, TYPE, OBJECT (`db.c`).
- **`stats.go`** — the counters snapshotted by `Stats()`.

---

## 7. Mental model to keep

> The kernel poller tells you **which** fds can act without blocking;
> non-blocking I/O guarantees acting on them **never stalls** the one
> goroutine; so a single thread fans across thousands of connections by
> looping **wait → dispatch ready fds → wait** forever.

Everything in this package is an elaboration of that sentence, plus the
bookkeeping to (a) parse commands that arrive split across reads, (b) send
replies that don't fit in one write, and (c) shut down cleanly.
