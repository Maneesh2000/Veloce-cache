// Package resp implements the RESP2 wire protocol: a resumable request
// parser (the analog of Redis's processMultibulkBuffer/processInlineBuffer
// in networking.c) and a reply serializer (the addReply* family).
package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

// Limits mirroring Redis (server.h / networking.c).
const (
	MaxInlineSize = 64 * 1024         // PROTO_INLINE_MAX_SIZE
	MaxBulkSize   = 512 * 1024 * 1024 // proto-max-bulk-len default (512MB)
	MaxMultibulk  = 1024 * 1024       // max elements in a multibulk request
)

// ProtocolError is a fatal protocol violation. The server replies with it
// and closes the connection, exactly like Redis.
type ProtocolError string

func (e ProtocolError) Error() string { return "Protocol error: " + string(e) }

// Parser is a resumable RESP2 request parser.
//
// Feed it whatever bytes arrived on the socket, then call Next repeatedly:
// each call returns one complete command (as an argv of byte slices), or
// (nil, nil) when more bytes are needed. Parsing progress — how many array
// elements remain, the length of a half-received bulk string, the args
// decoded so far — survives across calls, so a command split at any byte
// boundary across many reads is handled correctly. This mirrors the
// c->multibulklen / c->bulklen fields Redis keeps on the client struct.
//
// Returned argument slices are copies; they remain valid after further
// Feed/Next calls.
type Parser struct {
	buf []byte // unconsumed input
	pos int    // read offset into buf (compacted between commands)

	// In-progress multibulk state.
	multibulklen int      // remaining elements of the current command; 0 = between commands
	bulklen      int      // -1 = expecting a "$<len>" header, else bytes of bulk payload pending
	args         [][]byte // arguments decoded so far
}

// NewParser returns a Parser ready to receive bytes.
func NewParser() *Parser {
	return &Parser{bulklen: -1}
}

// Feed appends newly read bytes to the parser's buffer.
func (p *Parser) Feed(data []byte) {
	p.buf = append(p.buf, data...)
}

// Buffered returns how many unconsumed bytes the parser is holding.
func (p *Parser) Buffered() int { return len(p.buf) - p.pos }

// Next returns the next complete command, or (nil, nil) if the buffer does
// not yet hold one. A ProtocolError is fatal: the caller should send it to
// the client and close the connection (parser state is not recoverable).
