# `internal/resp` — the wire protocol (RESP2)

This package speaks **RESP2** (REdis Serialization Protocol), the wire format
every Redis client library uses. It has two halves, mirroring `networking.c`
in real Redis:

- **Parser** ([parser.go](parser.go)) — turns raw socket bytes into commands
  (`processMultibulkBuffer` / `processInlineBuffer`).
- **Serializer** ([writer.go](writer.go)) — turns replies into bytes
  (the `addReply*` family).

Because veloce implements the real protocol, the real `redis-cli`,
`redis-benchmark`, and every Redis client library work against it unchanged.

---

## 1. The protocol

RESP2 is a text-framed protocol. Every message starts with **one type byte**,
and every line ends with **CRLF** (`\r\n`):

| First byte | Type | Example on the wire | Meaning |
|------------|------|---------------------|---------|
| `+` | Simple string | `+OK\r\n` | short status reply |
| `-` | Error | `-ERR unknown command\r\n` | error reply |
| `:` | Integer | `:42\r\n` | numeric reply |
| `$` | Bulk string | `$5\r\nhello\r\n` | length-prefixed binary-safe blob |
| `*` | Array | `*2\r\n...\r\n...\r\n` | count-prefixed sequence of the above |

Two special nil forms: `$-1\r\n` (null bulk string — what `GET missing`
returns) and `*-1\r\n` (null array).

**Requests** are always an *array of bulk strings* — the command name and each
argument as one bulk string. **Replies** can be any type.

Why length-prefixed bulk strings instead of quotes or escaping? **Binary
safety.** The payload is read by counting bytes, never by scanning for a
terminator — so a value may contain `\r\n`, `\x00`, anything. That's why
`SET k "a\r\nb"` just works.

---

## 2. A complete round trip, byte by byte

Client runs `SET name gopher`. The client library encodes it as an array of
3 bulk strings:

```
*3\r\n          array of 3 elements
$3\r\n SET\r\n  element 1: 3-byte bulk string "SET"
$4\r\n name\r\n element 2: 4-byte bulk string "name"
$6\r\n gopher\r\n element 3: 6-byte bulk string "gopher"
```

Flat on the wire (33 bytes):

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\ngopher\r\n
```

The journey through veloce:

```
 client socket ──▶ handleReadable (server.go): unix.Read → raw bytes
                        │
                        ▼
                Parser.Feed(bytes)          bytes appended to p.buf
                        │
                        ▼
                Parser.Next()               decodes one complete command
                        │  returns args = [][]byte{"SET","name","gopher"}
                        ▼
                dispatch → setCommand       executes, then serializes:
                        │  c.out = AppendSimpleString(c.out, "OK")
                        ▼                   c.out now holds "+OK\r\n"
                flushOutput: unix.Write ──▶ client reads "+OK\r\n" → prints OK
```

More reply examples, exactly as this package emits them:

| Command | Reply bytes | redis-cli shows |
|---------|-------------|-----------------|
| `GET name` (hit) | `$6\r\ngopher\r\n` | `"gopher"` |
| `GET missing` | `$-1\r\n` | `(nil)` |
| `INCR n` | `:1\r\n` | `(integer) 1` |
| `DEL a b` | `:2\r\n` | `(integer) 2` |
| `KEYS *` (2 keys) | `*2\r\n$1\r\na\r\n$1\r\nb\r\n` | `1) "a"  2) "b"` |
| bad command | `-ERR unknown command 'X'\r\n` | `(error) ERR ...` |

---

## 3. The parser — resumable by design

### The problem it solves

TCP is a **byte stream, not a message stream**. One `read()` can hand you:

- half a command (`*3\r\n$3\r\nSE` … rest arrives later),
- exactly one command,
- three and a half commands (pipelining).

The parser must accept *any* split and produce identical results. That's the
core Phase-1 requirement, and the test suite proves it by feeding a command
**one byte at a time** (`TestResumableEveryByteBoundary`).

### How: saved progress, not restarts

`Parser` keeps its position *inside* a partially-received command across
calls — the same fields Redis keeps on the client struct
(`c->multibulklen` / `c->bulklen`):

```go
type Parser struct {
    buf []byte   // unconsumed bytes fed so far
    pos int      // read offset into buf

    multibulklen int      // array elements still expected; 0 = between commands
    bulklen      int      // -1 = expecting "$<len>" header; else payload bytes pending
    args         [][]byte // arguments decoded so far
}
```

The contract is two calls:

```go
p.Feed(bytes)          // append whatever the socket produced
args, err := p.Next()  // complete command | (nil, nil) = need more | ProtocolError
```

State machine view:

```
                 ┌──────────────────────────────────────────────┐
                 │  multibulklen == 0   (between commands)       │
                 └──────┬───────────────────────┬───────────────┘
        line starts '*' │                       │ any other first byte
                        ▼                       ▼
              read "*N\r\n" header       INLINE: split words on spaces
              multibulklen = N           return them as the command
              args = []                  (lets `PING` over netcat work)
                        │
                        ▼
                 ┌──────────────────────────────────────────────┐
                 │  loop N times:                                │
                 │    bulklen == -1 → read "$len\r\n" header     │
                 │    have len+2 bytes? → copy payload, args++   │
                 │        not yet?  → return (nil,nil), KEEP     │
                 │                    multibulklen/bulklen/args  │◀── resume here
                 └──────┬───────────────────────────────────────┘    on next Feed
                        │ all N collected
                        ▼
                 return args; reset state; compact buffer
```

The magic is the "not yet" arrow: returning `(nil, nil)` **without discarding
progress**. When the next `Feed` arrives, `Next` picks up mid-command —
mid-array, even mid-bulk-payload.

### Worked example: a split command

`*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n` arriving in two reads, cut mid-payload:

```
Read 1: "*2\r\n$4\r\nECHO\r\n$5\r\nhel"
  Feed → Next:
    header "*2"      → multibulklen=2
    header "$4"+ECHO → args=["ECHO"], multibulklen=1
    header "$5"      → bulklen=5, but only "hel" (3) buffered, need 5+2
    → return (nil,nil)     state kept: multibulklen=1, bulklen=5, args=["ECHO"]

Read 2: "lo\r\n"
  Feed → Next:
    buffer now "hello\r\n" → payload complete
    args=["ECHO","hello"], multibulklen=0
    → return [["ECHO"],["hello"]]      ✓ identical to the unsplit case
```

### Pipelining

**What pipelining is:** normally a client works request-by-request — send
`SET`, wait for `+OK`, send `GET`, wait for the value. Every command pays one
network round trip (RTT). A *pipelining* client instead writes **many
commands back-to-back without waiting for any reply**, then reads all the
replies in one go. For N commands that's ~1 round trip instead of N — on a
1ms network link, 100 sequential commands cost ~100ms, pipelined ~1ms.

```
 without pipelining (3 RTTs):          with pipelining (1 RTT):
 client            server              client            server
   │── SET k 1 ──────▶│                  │── SET k 1 ─┐
   │◀───── +OK ───────│                  │── INCR k ──┼─────▶│  one write,
   │── INCR k ────────▶│                 │── GET k ───┘      │  one packet
   │◀───── :2 ────────│                  │                   │
   │── GET k ─────────▶│                 │◀─ +OK  :2  $1 2 ──│  one read,
   │◀───── $1 2 ──────│                  │                   │  all replies
```

**How this package handles it:** one `unix.Read` may therefore deliver several
complete commands in a single buffer, e.g.:

```
*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n
└──────────── SET k 1 ─────────────┘└──────── INCR k ────────┘└─────── GET k ───────┘
```

After one `Feed`, the server calls `Next()` in a loop. Each call consumes
exactly one command from the buffer and returns it; the loop stops when
`Next()` returns `(nil, nil)` — meaning "no complete command left":

```
Feed(the 3-command buffer)
  Next() → ["SET","k","1"]     dispatch → c.out = "+OK\r\n"
  Next() → ["INCR","k"]        dispatch → c.out = "+OK\r\n:2\r\n"
  Next() → ["GET","k"]         dispatch → c.out = "+OK\r\n:2\r\n$1\r\n2\r\n"
  Next() → (nil, nil)          stop; flush c.out with one write
```

Two properties matter and both come free from the design:

- **Order is preserved.** Commands are decoded, executed, and their replies
  appended to `c.out` strictly in arrival order on a single thread — so the
  client can match reply #i to command #i with no IDs or correlation.
- **Replies coalesce.** Because serializers append to `c.out` and the socket
  is flushed once after the drain loop, the three replies above leave in one
  packet — the mirror image of the request batching.

A half command at the end of the batch is fine too: `Next()` returns the two
complete ones, keeps the fragment's progress, and resumes when the rest
arrives — pipelining and resumability are the same mechanism.

No pipelining-specific code exists anywhere; the `Next()`-until-nil loop in
`handleReadable` is all of it. This is why `redis-benchmark -P 16`
(16 commands per packet) hits ~2-3M ops/sec against veloce.

### The inline protocol

A line that doesn't start with `*` is treated as an **inline command**:
space-separated words, e.g. typing `PING` into `nc localhost 6379`. It's a
debugging convenience from real Redis, kept here (minus quote handling, which
is deferred).

### Protocol errors are fatal

Malformed input — wrong type byte where `$` was expected, a bulk length
that's negative or over 512MB, an array over 1M elements, an unterminated
line beyond 64KB — returns a `ProtocolError`. The server sends it as an
`-ERR Protocol error: ...` reply and **closes the connection**, exactly like
Redis: parser state after garbage is unrecoverable, so the connection is too.

```
limits (matching Redis):
  MaxInlineSize  64 KB     unterminated-line / inline cap
  MaxBulkSize    512 MB    single argument cap (proto-max-bulk-len)
  MaxMultibulk   1 M       elements per command
```

These caps exist so a malicious client can't make the server buffer unbounded
garbage while "waiting for the rest."

### Memory behavior

- Returned args are **copies** — safe to hold after the next `Feed` (the
  keyspace stores them directly).
- `compact()` slides unconsumed bytes to the buffer front after each command,
  so the buffer stays bounded by the size of one in-flight request, not by
  total traffic.

---

## 4. The serializer — append, don't send

Reply functions **append encoded bytes to a buffer** and return it; nothing
touches the socket here. The server accumulates replies in `c.out` and
flushes once per event — which is what makes pipelined replies coalesce into
few packets:

```go
c.out = resp.AppendSimpleString(c.out, "OK")     // +OK\r\n
c.out = resp.AppendError(c.out, "ERR nope")      // -ERR nope\r\n
c.out = resp.AppendInt(c.out, 42)                // :42\r\n
c.out = resp.AppendBulk(c.out, []byte("hi"))     // $2\r\nhi\r\n
c.out = resp.AppendNull(c.out)                   // $-1\r\n
c.out = resp.AppendArrayHeader(c.out, 2)         // *2\r\n  (elements follow)
```

One veloce-specific helper: `AppendBulkInt64(b, v)` formats an int-encoded
object (`SET n 100` stores a real int64, not the string "100") straight into
the output buffer via a stack scratch array — the read path allocates no
intermediate object just to print a number.

Arrays are emitted as a header plus N elements — `keysCommand` is the
canonical user:

```go
c.out = resp.AppendArrayHeader(c.out, len(keys))
for _, k := range keys {
    c.out = resp.AppendBulk(c.out, k)
}
```

---

## 5. Debugging the wire yourself

Watch the actual bytes with `nc` (inline protocol):

```sh
$ nc 127.0.0.1 6379
PING                      ← you type (inline command)
+PONG                     ← raw RESP comes back
GET missing
$-1
```

Or see what a client sends by echoing RESP manually:

```sh
$ printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6379
+PONG
```

And the tests are executable documentation:

- `TestResumableEveryByteBoundary` / `TestResumableEverySplitPoint` — the
  split-anywhere guarantee (parser level).
- `TestPipelinedCommandsInOneFeed` — three commands, one buffer.
- `TestProtocolErrors` — every malformed-input case and its exact error text.
- `TestRoundTrip` (writer_test.go) — serializer output fed back through the
  parser comes out identical.

---

## 6. Mental model to keep

> The parser is a **cursor into an incomplete stream**: feed it bytes, ask for
> commands; it either hands you a complete one, or remembers exactly where it
> stopped. The serializer is the reverse: replies are appended to a buffer as
> framed bytes, and the socket sees them only when the event loop flushes.

Everything else — binary safety, pipelining, netcat support, abuse caps — is
a consequence of the length-prefixed framing plus that resumability.
