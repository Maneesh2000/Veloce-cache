package resp

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// feedAll feeds the whole input at once and collects every complete command.
func feedAll(t *testing.T, p *Parser, input string) [][][]byte {
	t.Helper()
	p.Feed([]byte(input))
	var cmds [][][]byte
	for {
		args, err := p.Next()
		if err != nil {
			t.Fatalf("unexpected protocol error: %v", err)
		}
		if args == nil {
			return cmds
		}
		cmds = append(cmds, args)
	}
}

func argsEqual(args [][]byte, want ...string) bool {
	if len(args) != len(want) {
		return false
	}
	for i := range want {
		if string(args[i]) != want[i] {
			return false
		}
	}
	return true
}

func TestParseSimpleCommand(t *testing.T) {
	p := NewParser()
	cmds := feedAll(t, p, "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	if len(cmds) != 1 || !argsEqual(cmds[0], "ECHO", "hello") {
		t.Fatalf("got %q", cmds)
	}
	if p.Buffered() != 0 {
		t.Fatalf("parser retained %d bytes", p.Buffered())
	}
}

func TestParseEmptyBulkAndBinary(t *testing.T) {
	p := NewParser()
	// Empty bulk string and a payload containing CR, LF, and NUL bytes.
	payload := "a\r\nb\x00c"
	input := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$0\r\n\r\n$%d\r\n%s\r\n", len(payload), payload)
	cmds := feedAll(t, p, input)
	if len(cmds) != 1 || !argsEqual(cmds[0], "SET", "", payload) {
		t.Fatalf("got %q", cmds)
	}
}

// TestResumableEveryByteBoundary is THE phase-1 test: a command must parse
// identically no matter where the byte stream is cut. Feed the input one
// byte at a time and assert exactly one command comes out, at the last byte.
func TestResumableEveryByteBoundary(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n"
	p := NewParser()
	var got [][]byte
	for i := 0; i < len(input); i++ {
		p.Feed([]byte{input[i]})
		args, err := p.Next()
		if err != nil {
			t.Fatalf("byte %d: protocol error: %v", i, err)
		}
		if args != nil {
			if i != len(input)-1 {
				t.Fatalf("command completed early at byte %d/%d", i, len(input)-1)
			}
			got = args
		}
	}
	if !argsEqual(got, "SET", "key", "hello world") {
		t.Fatalf("got %q", got)
	}
}

// TestResumableEverySplitPoint cuts the input into two feeds at every
// possible position; both halves together must always yield the command.
func TestResumableEverySplitPoint(t *testing.T) {
	input := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	for cut := 0; cut <= len(input); cut++ {
		p := NewParser()
		p.Feed([]byte(input[:cut]))
		var cmds [][][]byte
		for {
			args, err := p.Next()
			if err != nil {
				t.Fatalf("cut %d: %v", cut, err)
			}
			if args == nil {
				break
			}
			cmds = append(cmds, args)
		}
		p.Feed([]byte(input[cut:]))
		for {
			args, err := p.Next()
			if err != nil {
				t.Fatalf("cut %d: %v", cut, err)
			}
			if args == nil {
				break
			}
			cmds = append(cmds, args)
		}
		if len(cmds) != 1 || !argsEqual(cmds[0], "ECHO", "hello") {
			t.Fatalf("cut %d: got %q", cut, cmds)
		}
	}
}

func TestPipelinedCommandsInOneFeed(t *testing.T) {
	p := NewParser()
	cmds := feedAll(t, p,
		"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n*1\r\n$4\r\nPING\r\n")
	if len(cmds) != 3 {
		t.Fatalf("want 3 commands, got %d: %q", len(cmds), cmds)
	}
	if !argsEqual(cmds[0], "PING") || !argsEqual(cmds[1], "ECHO", "hi") || !argsEqual(cmds[2], "PING") {
		t.Fatalf("got %q", cmds)
	}
}

func TestInlineCommand(t *testing.T) {
	p := NewParser()
	cmds := feedAll(t, p, "PING\r\nECHO hello\r\n")
	if len(cmds) != 2 || !argsEqual(cmds[0], "PING") || !argsEqual(cmds[1], "ECHO", "hello") {
		t.Fatalf("got %q", cmds)
	}
}

func TestInlineBareLFAndBlankLines(t *testing.T) {
	p := NewParser()
	// LF-only termination and blank lines (which Redis silently skips).
	cmds := feedAll(t, p, "\r\n\nPING\n")
	if len(cmds) != 1 || !argsEqual(cmds[0], "PING") {
		t.Fatalf("got %q", cmds)
	}
}

func TestZeroAndNegativeMultibulkSkipped(t *testing.T) {
	p := NewParser()
	cmds := feedAll(t, p, "*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n")
	if len(cmds) != 1 || !argsEqual(cmds[0], "PING") {
		t.Fatalf("got %q", cmds)
	}
}

func TestArgsAreCopies(t *testing.T) {
	p := NewParser()
	p.Feed([]byte("*2\r\n$4\r\nECHO\r\n$3\r\nabc\r\n"))
	args, err := p.Next()
	if err != nil || args == nil {
		t.Fatalf("args=%v err=%v", args, err)
	}
	// Feeding more data must not corrupt previously returned args.
	p.Feed(bytes.Repeat([]byte("X"), 4096))
	if !argsEqual(args, "ECHO", "abc") {
		t.Fatalf("args corrupted after Feed: %q", args)
	}
}

