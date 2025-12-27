package server

import (
	"bufio"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Phase 2 integration tests: the key-value core over real TCP, reusing the
// Phase 1 helpers (startServer, dial, cmd, readReply, mustReply).

func TestSetGetRoundtrip(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("SET", "k", "hello"))
	mustReply(t, br, "OK")
	conn.Write(cmd("GET", "k"))
	mustReply(t, br, "hello")

	// Missing key -> nil.
	conn.Write(cmd("GET", "nosuchkey"))
	mustReply(t, br, "(nil)")

	// Empty value roundtrips.
	conn.Write(cmd("SET", "empty", ""))
	mustReply(t, br, "OK")
	conn.Write(cmd("GET", "empty"))
	mustReply(t, br, "")

	// Overwrite.
	conn.Write(cmd("SET", "k", "second"))
	mustReply(t, br, "OK")
	conn.Write(cmd("GET", "k"))
	mustReply(t, br, "second")

	// Plain SET with stray extra args is a syntax error (options later).
	conn.Write(cmd("SET", "k", "v", "BOGUS"))
	mustReply(t, br, "(error) ERR syntax error")
}

func TestSetGetBinarySafety(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	key := "bin\x00key\r\n"
	val := "val\x00ue\r\nwith\ffunk"
	conn.Write(cmd("SET", key, val))
	mustReply(t, br, "OK")
	conn.Write(cmd("GET", key))
	mustReply(t, br, val)
	// A prefix of the binary key is a different key.
	conn.Write(cmd("GET", "bin\x00key"))
	mustReply(t, br, "(nil)")
}

func TestObjectEncodingTransitions(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	check := func(val, wantEnc string) {
		t.Helper()
		conn.Write(cmd("SET", "k", val))
		mustReply(t, br, "OK")
		conn.Write(cmd("OBJECT", "ENCODING", "k"))
		mustReply(t, br, wantEnc)
		// The value must round-trip byte-identical regardless of encoding.
		conn.Write(cmd("GET", "k"))
		mustReply(t, br, val)
	}

	check("100", "int")
	check("-42", "int")
	check("0", "int")
	check("9223372036854775807", "int")    // MaxInt64
	check("-9223372036854775808", "int")   // MinInt64
	check("007", "embstr")                 // leading zeros: not canonical
	check("+5", "embstr")                  // '+' not supported by string2ll
	check("9223372036854775808", "embstr") // MaxInt64+1 overflows
	check("hello", "embstr")
	check(strings.Repeat("x", 44), "embstr") // at the cutoff
	check(strings.Repeat("x", 45), "raw")    // past the cutoff

	// SET overwrite re-encodes: string -> int.
	conn.Write(cmd("SET", "k", "abc"))
	mustReply(t, br, "OK")
	conn.Write(cmd("SET", "k", "123"))
	mustReply(t, br, "OK")
	conn.Write(cmd("OBJECT", "ENCODING", "k"))
	mustReply(t, br, "int")

	// Missing key: nil, NOT an error (Redis OBJECT quirk).
	conn.Write(cmd("OBJECT", "ENCODING", "missingkey"))
	mustReply(t, br, "(nil)")

	// Unknown subcommand.
	conn.Write(cmd("OBJECT", "FROBNICATE", "k"))
	got, err := readReply(br)
	if err != nil {
		t.Fatal(err)
	}
	if got != "(error) ERR unknown subcommand 'FROBNICATE'. Try OBJECT HELP." {
		t.Fatalf("got %q", got)
	}
}

func TestIncrDecrFamily(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	// INCR on a missing key starts from 0.
	conn.Write(cmd("INCR", "counter"))
	mustReply(t, br, "1")
	conn.Write(cmd("INCR", "counter"))
	mustReply(t, br, "2")
	conn.Write(cmd("DECR", "counter"))
	mustReply(t, br, "1")
	conn.Write(cmd("INCRBY", "counter", "41"))
	mustReply(t, br, "42")
	conn.Write(cmd("DECRBY", "counter", "40"))
	mustReply(t, br, "2")
	conn.Write(cmd("INCRBY", "counter", "-3"))
	mustReply(t, br, "-1")

	// Result is int-encoded.
	conn.Write(cmd("OBJECT", "ENCODING", "counter"))
	mustReply(t, br, "int")

	// INCR on a canonical-integer string value works...
	conn.Write(cmd("SET", "s", "41"))
	mustReply(t, br, "OK")
	conn.Write(cmd("INCR", "s"))
	mustReply(t, br, "42")

	// ...but non-canonical / non-integer values refuse.
	for _, bad := range []string{"007", "abc", "1.5", ""} {
		conn.Write(cmd("SET", "bad", bad))
		mustReply(t, br, "OK")
		conn.Write(cmd("INCR", "bad"))
		mustReply(t, br, "(error) ERR value is not an integer or out of range")
	}

	// Bad INCRBY argument.
	conn.Write(cmd("INCRBY", "counter", "notanumber"))
	mustReply(t, br, "(error) ERR value is not an integer or out of range")
}

// TestIncrDoesNotCorruptSharedSingleton is the critical object-model test:
// INCR on a value served from the shared 0..9999 table must produce a new
// value WITHOUT mutating the singleton other keys point to.
func TestIncrDoesNotCorruptSharedSingleton(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("SET", "a", "100")) // a -> shared singleton for 100
	mustReply(t, br, "OK")
	conn.Write(cmd("SET", "b", "100")) // b -> the same singleton
	mustReply(t, br, "OK")

	conn.Write(cmd("INCR", "a"))
	mustReply(t, br, "101")
	conn.Write(cmd("GET", "a"))
	mustReply(t, br, "101")

	// b must still read 100 — if the singleton were mutated in place, this
	// (and every future SET x 100 in the process) would say 101.
	conn.Write(cmd("GET", "b"))
	mustReply(t, br, "100")
}

func TestIncrInPlaceOnPrivateInt(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	// 20000 is outside the shared range -> private object -> in-place path.
	conn.Write(cmd("SET", "big", "20000"))
	mustReply(t, br, "OK")
	conn.Write(cmd("INCR", "big"))
	mustReply(t, br, "20001")
	conn.Write(cmd("GET", "big"))
	mustReply(t, br, "20001")
	conn.Write(cmd("OBJECT", "ENCODING", "big"))
	mustReply(t, br, "int")
}

func TestIncrDecrOverflow(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	overflow := "(error) ERR increment or decrement would overflow"

	conn.Write(cmd("SET", "max", "9223372036854775807"))
	mustReply(t, br, "OK")
	conn.Write(cmd("INCR", "max"))
	mustReply(t, br, overflow)
	conn.Write(cmd("GET", "max")) // value unchanged after failed INCR
	mustReply(t, br, "9223372036854775807")

	conn.Write(cmd("SET", "min", "-9223372036854775808"))
	mustReply(t, br, "OK")
	conn.Write(cmd("DECR", "min"))
	mustReply(t, br, overflow)
	conn.Write(cmd("INCR", "min")) // stepping back toward zero is fine
	mustReply(t, br, "-9223372036854775807")

	// INCRBY MinInt64 onto 0 is legal (no positive/positive overflow).
	conn.Write(cmd("SET", "z", "0"))
	mustReply(t, br, "OK")
	conn.Write(cmd("INCRBY", "z", "-9223372036854775808"))
	mustReply(t, br, "-9223372036854775808")

	// DECRBY MinInt64: the negation itself overflows, dedicated error.
	conn.Write(cmd("DECRBY", "z", "-9223372036854775808"))
	mustReply(t, br, "(error) ERR decrement would overflow")
}

func TestDelAndExists(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	for _, k := range []string{"a", "b", "c"} {
		conn.Write(cmd("SET", k, "v"))
		mustReply(t, br, "OK")
	}

	// EXISTS counts duplicates per-argument.
	conn.Write(cmd("EXISTS", "a"))
	mustReply(t, br, "1")
	conn.Write(cmd("EXISTS", "a", "a", "a"))
	mustReply(t, br, "3")
	conn.Write(cmd("EXISTS", "a", "missing", "b", "a"))
	mustReply(t, br, "3")
	conn.Write(cmd("EXISTS", "missing"))
	mustReply(t, br, "0")

	// DEL is variadic, counts only real deletions.
	conn.Write(cmd("DEL", "a", "missing", "b"))
	mustReply(t, br, "2")
	conn.Write(cmd("DEL", "a"))
	mustReply(t, br, "0")
	conn.Write(cmd("EXISTS", "a", "b", "c"))
	mustReply(t, br, "1") // only c remains
}

func TestTypeCommand(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("SET", "s", "v"))
	mustReply(t, br, "OK")
	conn.Write(cmd("TYPE", "s"))
	mustReply(t, br, "string")
	conn.Write(cmd("TYPE", "missing"))
	mustReply(t, br, "none")
	// Int-encoded values are still TYPE string.
	conn.Write(cmd("SET", "n", "42"))
	mustReply(t, br, "OK")
	conn.Write(cmd("TYPE", "n"))
	mustReply(t, br, "string")
}

func TestKeysGlob(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	for _, k := range []string{"user:1", "user:2", "user:30", "session:1", "hello", "hallo"} {
		conn.Write(cmd("SET", k, "v"))
		mustReply(t, br, "OK")
	}

	keysSorted := func(pattern string) []string {
		t.Helper()
		conn.Write(cmd("KEYS", pattern))
		got, err := readReply(br)
		if err != nil {
			t.Fatal(err)
		}
		inner := strings.Trim(got, "[]")
		if inner == "" {
			return nil
		}
		keys := strings.Split(inner, " ")
		sort.Strings(keys) // map iteration order is random
		return keys
	}

	if g := keysSorted("*"); len(g) != 6 {
		t.Fatalf("KEYS * = %v", g)
	}
	if g := keysSorted("user:*"); fmt.Sprint(g) != "[user:1 user:2 user:30]" {
		t.Fatalf("KEYS user:* = %v", g)
	}
	if g := keysSorted("user:?"); fmt.Sprint(g) != "[user:1 user:2]" {
		t.Fatalf("KEYS user:? = %v", g)
	}
	if g := keysSorted("h[ae]llo"); fmt.Sprint(g) != "[hallo hello]" {
		t.Fatalf("KEYS h[ae]llo = %v", g)
	}
	if g := keysSorted("h[^e]llo"); fmt.Sprint(g) != "[hallo]" {
		t.Fatalf("KEYS h[^e]llo = %v", g)
	}
	if g := keysSorted("nomatch*"); g != nil {
		t.Fatalf("KEYS nomatch* = %v", g)
	}
}

// TestPipelinedWriteSequence pins ordering of mutating commands in one
// batched write — replies must reflect strictly serial execution.
func TestPipelinedWriteSequence(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	var batch []byte
	batch = append(batch, cmd("SET", "k", "1")...)
	batch = append(batch, cmd("INCR", "k")...)
	batch = append(batch, cmd("GET", "k")...)
	batch = append(batch, cmd("DEL", "k")...)
	batch = append(batch, cmd("GET", "k")...)
	conn.Write(batch)

	for _, want := range []string{"OK", "2", "2", "1", "(nil)"} {
		mustReply(t, br, want)
	}
}

func TestKeyspaceHitMissStats(t *testing.T) {
	srv, stop := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("SET", "k", "v"))
	mustReply(t, br, "OK")
	conn.Write(cmd("GET", "k")) // hit
	mustReply(t, br, "v")
	conn.Write(cmd("GET", "missing")) // miss
	mustReply(t, br, "(nil)")
	conn.Write(cmd("EXISTS", "k", "missing")) // hit + miss
	mustReply(t, br, "1")
	conn.Write(cmd("INCR", "n")) // write path: must NOT count
	mustReply(t, br, "1")

	stop() // stats are loop-owned; halt before reading
	st := srv.Stats()
	if st.KeyspaceHits != 2 || st.KeyspaceMisses != 2 {
		t.Fatalf("hits=%d misses=%d, want 2/2", st.KeyspaceHits, st.KeyspaceMisses)
	}
}
