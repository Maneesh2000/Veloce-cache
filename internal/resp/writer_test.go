package resp

import (
	"bytes"
	"testing"
)

func TestSerializers(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"simple string", AppendSimpleString(nil, "PONG"), "+PONG\r\n"},
		{"error", AppendError(nil, "ERR boom"), "-ERR boom\r\n"},
		{"int zero", AppendInt(nil, 0), ":0\r\n"},
		{"int negative", AppendInt(nil, -42), ":-42\r\n"},
		{"bulk", AppendBulk(nil, []byte("hello")), "$5\r\nhello\r\n"},
		{"bulk empty", AppendBulk(nil, nil), "$0\r\n\r\n"},
		{"bulk binary", AppendBulk(nil, []byte("a\r\nb")), "$4\r\na\r\nb\r\n"},
		{"bulk string", AppendBulkString(nil, "hi"), "$2\r\nhi\r\n"},
		{"null", AppendNull(nil), "$-1\r\n"},
		{"array header", AppendArrayHeader(nil, 3), "*3\r\n"},
		{"array empty", AppendArrayHeader(nil, 0), "*0\r\n"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestAppendPreservesPrefix(t *testing.T) {
	// The Append* functions must append, not overwrite — replies accumulate
	// in the client output buffer during pipelining.
	b := AppendSimpleString(nil, "PONG")
	b = AppendBulkString(b, "hi")
	b = AppendInt(b, 7)
	want := "+PONG\r\n$2\r\nhi\r\n:7\r\n"
	if !bytes.Equal(b, []byte(want)) {
		t.Fatalf("got %q, want %q", b, want)
	}
}

// TestRoundTrip: what the serializer emits as a command array, the parser
// must read back verbatim.
func TestRoundTrip(t *testing.T) {
	var wire []byte
	wire = AppendArrayHeader(wire, 3)
	wire = AppendBulkString(wire, "SET")
	wire = AppendBulkString(wire, "k")
	wire = AppendBulk(wire, []byte("v\r\n\x00v"))

	p := NewParser()
	p.Feed(wire)
	args, err := p.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 3 || string(args[0]) != "SET" || string(args[1]) != "k" ||
		!bytes.Equal(args[2], []byte("v\r\n\x00v")) {
		t.Fatalf("round trip mismatch: %q", args)
	}
}
