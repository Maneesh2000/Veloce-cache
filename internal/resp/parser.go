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
func (p *Parser) Next() ([][]byte, error) {
	for {
		if p.multibulklen == 0 {
			// Between commands: expect a "*<count>" header or an inline command.
			line, ok, err := p.readLine()
			if err != nil {
				return nil, err
			}
			if !ok {
				p.compact()
				return nil, nil
			}
			if len(line) == 0 {
				continue // bare CRLF between commands: ignored, like Redis
			}
			if line[0] != '*' {
				// Inline protocol: space-separated words on one line.
				args := splitInline(line)
				if len(args) == 0 {
					continue
				}
				p.compact()
				return args, nil
			}
			n, perr := parseLen(line[1:])
			if perr != nil || n > MaxMultibulk {
				return nil, ProtocolError("invalid multibulk length")
			}
			if n <= 0 {
				continue // "*0" and "*-1" carry no command; skip
			}
			p.multibulklen = n
			p.bulklen = -1
			p.args = make([][]byte, 0, n)
		}

		// Inside a multibulk: decode the remaining "$<len>\r\n<payload>\r\n" elements.
		for p.multibulklen > 0 {
			if p.bulklen == -1 {
				line, ok, err := p.readLine()
				if err != nil {
					return nil, err
				}
				if !ok {
					p.compact()
					return nil, nil
				}
				if len(line) == 0 || line[0] != '$' {
					c := byte(' ')
					if len(line) > 0 {
						c = line[0]
					}
					return nil, ProtocolError(fmt.Sprintf("expected '$', got '%c'", c))
				}
				n, perr := parseLen(line[1:])
				if perr != nil || n < 0 || n > MaxBulkSize {
					return nil, ProtocolError("invalid bulk length")
				}
				p.bulklen = n
			}
			// Need the full payload plus its trailing CRLF.
			if p.Buffered() < p.bulklen+2 {
				p.compact()
				return nil, nil
			}
			if p.buf[p.pos+p.bulklen] != '\r' || p.buf[p.pos+p.bulklen+1] != '\n' {
				return nil, ProtocolError("invalid bulk data termination")
			}
			arg := make([]byte, p.bulklen)
			copy(arg, p.buf[p.pos:p.pos+p.bulklen])
			p.pos += p.bulklen + 2
			p.bulklen = -1
			p.args = append(p.args, arg)
			p.multibulklen--
		}

		args := p.args
		p.args = nil
		p.compact()
		return args, nil
	}
}

// readLine returns the next CRLF/LF-terminated line (without terminator).
// ok=false means the line is not complete yet. Enforces MaxInlineSize on
// unterminated input so a malicious client can't grow the buffer unboundedly
// while we wait for a header line.
