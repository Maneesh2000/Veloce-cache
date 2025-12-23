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

