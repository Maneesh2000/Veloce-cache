package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// startServer boots a server on a free port and tears it down with the test.
// The returned stop function halts the event loop and waits for it to exit;
// after it returns, reading srv.Stats() is race-free (stats are owned by the
// loop goroutine — the price of the no-locks design).
func startServer(t *testing.T) (*Server, func()) {
	t.Helper()
	srv, err := New("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			srv.Stop()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Serve returned error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("event loop did not stop within 5s")
			}
		})
	}
	t.Cleanup(stop)
	return srv, stop
}

func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial %s: %v", srv.Addr(), err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readReply decodes one RESP reply into a printable form:
//
//	+OK -> "OK" · -ERR x -> "(error) ERR x" · :5 -> "5"
//	$3 abc -> "abc" · $-1 -> "(nil)" · *N -> "[e1 e2 ...]"
func readReply(br *bufio.Reader) (string, error) {
	header, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	header = strings.TrimSuffix(strings.TrimSuffix(header, "\n"), "\r")
	if header == "" {
		return "", fmt.Errorf("empty reply header")
	}
	kind, rest := header[0], header[1:]
	switch kind {
	case '+':
		return rest, nil
	case '-':
		return "(error) " + rest, nil
	case ':':
		return rest, nil
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "(nil)", nil
		}
		payload := make([]byte, n+2) // + CRLF
		if _, err := io.ReadFull(br, payload); err != nil {
			return "", err
		}
		return string(payload[:n]), nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return "", err
		}
		elems := make([]string, n)
		for i := 0; i < n; i++ {
			if elems[i], err = readReply(br); err != nil {
				return "", err
			}
		}
		return "[" + strings.Join(elems, " ") + "]", nil
	default:
		return "", fmt.Errorf("unknown reply type %q in %q", kind, header)
	}
}

func mustReply(t *testing.T, br *bufio.Reader, want string) {
	t.Helper()
	got, err := readReply(br)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func cmd(args ...string) []byte {
	var b []byte
	b = append(b, fmt.Sprintf("*%d\r\n", len(args))...)
	for _, a := range args {
		b = append(b, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
	}
	return b
}

func TestPingAndEcho(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("PING"))
	mustReply(t, br, "PONG")

	conn.Write(cmd("PING", "hello"))
	mustReply(t, br, "hello")

	conn.Write(cmd("ECHO", "hello world"))
	mustReply(t, br, "hello world")

	// Case-insensitive dispatch.
	conn.Write(cmd("ping"))
	mustReply(t, br, "PONG")
}

func TestInlineProtocol(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write([]byte("PING\r\n"))
	mustReply(t, br, "PONG")
	conn.Write([]byte("ECHO inline-works\r\n"))
	mustReply(t, br, "inline-works")
}

func TestPipelining(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	// 500 commands in a single write; expect 500 replies in order.
	const n = 500
	var batch []byte
	for i := 0; i < n; i++ {
		batch = append(batch, cmd("ECHO", fmt.Sprintf("msg-%d", i))...)
	}
	if _, err := conn.Write(batch); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		mustReply(t, br, fmt.Sprintf("msg-%d", i))
	}
}

// TestDribbledCommand writes a command one byte at a time — the wire-level
// version of the parser resumability test, proving the whole server path
// (read event -> feed -> resume) survives arbitrary packet fragmentation.
