package server

import (
	"fmt"
	"strings"

	"github.com/Maneesh2000/Veloce-cache/internal/resp"
)

// Keyspace commands — the db.c analog.

// delCommand: DEL key [key ...] -> :numdeleted (delGenericCommand).
func delCommand(s *Server, c *client, args [][]byte) {
	deleted := int64(0)
	for _, key := range args[1:] {
		if s.db.Delete(key) {
			deleted++
		}
	}
	c.out = resp.AppendInt(c.out, deleted)
}

// existsCommand: EXISTS key [key ...] -> count. Duplicate arguments count
// every time they appear (existsCommand in db.c checks each argv slot).
func existsCommand(s *Server, c *client, args [][]byte) {
	count := int64(0)
	for _, key := range args[1:] {
		if s.db.LookupRead(key) != nil {
			count++
		}
	}
	c.out = resp.AppendInt(c.out, count)
}

// keysCommand: KEYS pattern -> multi-bulk of matching keys. Walks the whole
// keyspace on the loop thread — O(N) latency by design, same as Redis.
func keysCommand(s *Server, c *client, args [][]byte) {
	keys := s.db.Keys(args[1])
	c.out = resp.AppendArrayHeader(c.out, len(keys))
	for _, k := range keys {
		c.out = resp.AppendBulk(c.out, k)
	}
}

// typeCommand: TYPE key -> simple string "string" | "none".
func typeCommand(s *Server, c *client, args [][]byte) {
	o := s.db.LookupRead(args[1])
	if o == nil {
		c.out = resp.AppendSimpleString(c.out, "none")
		return
	}
	c.out = resp.AppendSimpleString(c.out, o.TypeName())
}

// objectCommand: OBJECT <subcommand> — container command. Phase 2 supports
// ENCODING (and HELP-adjacent error text). Note Redis replies nil, NOT an
// error, for a missing key here.
func objectCommand(s *Server, c *client, args [][]byte) {
	sub := strings.ToUpper(string(args[1]))
	switch sub {
	case "ENCODING":
		if len(args) != 3 {
			c.out = resp.AppendError(c.out,
				"ERR wrong number of arguments for 'object|encoding' command")
			return
		}
		o := s.db.LookupRead(args[2])
		if o == nil {
			c.out = resp.AppendNull(c.out)
			return
		}
		c.out = resp.AppendBulkString(c.out, o.EncodingName())
	default:
		// commandCheckExistence (server.c) format for container commands.
		c.out = resp.AppendError(c.out, fmt.Sprintf(
			"ERR unknown subcommand '%.128s'. Try OBJECT HELP.", args[1]))
	}
}
