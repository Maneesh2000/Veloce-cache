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
func TestDribbledCommand(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	wire := cmd("ECHO", "dribble")
	for _, b := range wire {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	mustReply(t, br, "dribble")
}

// TestLargeReplyExercisesWritePath echoes a 4MB payload. The server tries to
// write the reply before the client reads anything, overflows the kernel
// socket buffer, and must fall back to the write-interest (EPOLLOUT) path.
func TestLargeReplyExercisesWritePath(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReaderSize(conn, 64*1024)

	payload := bytes.Repeat([]byte("x"), 4<<20)
	var wire []byte
	wire = append(wire, fmt.Sprintf("*2\r\n$4\r\nECHO\r\n$%d\r\n", len(payload))...)
	wire = append(wire, payload...)
	wire = append(wire, '\r', '\n')
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}

	got, err := readReply(br)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(payload) || got != string(payload) {
		t.Fatalf("large echo mismatch: got %d bytes", len(got))
	}

	// The connection must still be usable afterwards (write interest was
	// disarmed once drained).
	conn.Write(cmd("PING"))
	mustReply(t, br, "PONG")
}

func TestUnknownCommandAndArity(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("FLYTOTHEMOON"))
	got, err := readReply(br)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "(error) ERR unknown command 'FLYTOTHEMOON'") {
		t.Fatalf("got %q", got)
	}

	conn.Write(cmd("ECHO")) // arity violation
	got, err = readReply(br)
	if err != nil {
		t.Fatal(err)
	}
	if got != "(error) ERR wrong number of arguments for 'echo' command" {
		t.Fatalf("got %q", got)
	}

	// Errors must not kill the connection.
	conn.Write(cmd("PING"))
	mustReply(t, br, "PONG")
}

func TestProtocolErrorRepliesThenCloses(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write([]byte("*1\r\n#4\r\nPING\r\n"))
	got, err := readReply(br)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Protocol error: expected '$', got '#'") {
		t.Fatalf("got %q", got)
	}
	// Server must close after a protocol error.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF after protocol error, got %v", err)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	srv, _ := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("QUIT"))
	mustReply(t, br, "OK")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF after QUIT, got %v", err)
	}
}

// TestConcurrentClients hammers the loop from 50 goroutines to prove client
// state (parser progress, output buffers) never bleeds across connections.
func TestConcurrentClients(t *testing.T) {
	srv, _ := startServer(t)
	const clients, rounds = 50, 100

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", srv.Addr())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			br := bufio.NewReader(conn)
			for r := 0; r < rounds; r++ {
				msg := fmt.Sprintf("client-%d-round-%d", id, r)
				if _, err := conn.Write(cmd("ECHO", msg)); err != nil {
					errs <- fmt.Errorf("client %d: %w", id, err)
					return
				}
				got, err := readReply(br)
				if err != nil {
					errs <- fmt.Errorf("client %d: %w", id, err)
					return
				}
				if got != msg {
					errs <- fmt.Errorf("client %d: got %q want %q", id, got, msg)
					return
				}
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStatsCounters(t *testing.T) {
	srv, stop := startServer(t)
	conn := dial(t, srv)
	br := bufio.NewReader(conn)

	conn.Write(cmd("PING"))
	mustReply(t, br, "PONG")
	conn.Write(cmd("ECHO", "x"))
	mustReply(t, br, "x")
	conn.Write(cmd("NOPE"))
	if _, err := readReply(br); err != nil {
		t.Fatal(err)
	}

	// Stats are owned by the loop goroutine (no-locks design), so halt the
	// loop before reading them; stop() waits for Serve to return.
	stop()
	st := srv.Stats()
	if st.TotalConnectionsReceived != 1 {
		t.Errorf("TotalConnectionsReceived = %d, want 1", st.TotalConnectionsReceived)
	}
	if st.TotalCommandsProcessed != 2 {
		t.Errorf("TotalCommandsProcessed = %d, want 2 (unknown cmd excluded)", st.TotalCommandsProcessed)
	}
	if st.UnknownCommands != 1 {
		t.Errorf("UnknownCommands = %d, want 1", st.UnknownCommands)
	}
	if st.CommandCalls["ping"] != 1 || st.CommandCalls["echo"] != 1 {
		t.Errorf("CommandCalls = %v", st.CommandCalls)
	}
	if st.TotalNetInputBytes == 0 || st.TotalNetOutputBytes == 0 {
		t.Errorf("net byte counters not advancing: in=%d out=%d",
			st.TotalNetInputBytes, st.TotalNetOutputBytes)
	}
}

func TestManySequentialConnections(t *testing.T) {
	srv, _ := startServer(t)
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		br := bufio.NewReader(conn)
		conn.Write(cmd("PING"))
		mustReply(t, br, "PONG")
		conn.Close()
	}
}
