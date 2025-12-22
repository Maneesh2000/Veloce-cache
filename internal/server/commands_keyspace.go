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
