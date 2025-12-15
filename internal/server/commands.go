package server

import (
	"fmt"
	"strings"

	"github.com/Maneesh2000/Veloce-cache/internal/resp"
)

// command mirrors the shape of Redis's redisCommand table entry: a name, an
// arity contract, and a handler. Arity follows the Redis convention:
// positive = exact argc (including the command name), negative = minimum.
type command struct {
	name    string
	arity   int
	handler func(s *Server, c *client, args [][]byte)
}

// commandTable is the Phase 1 dispatch table (analog of redisCommandTable in
// server.c, minus the generated metadata). Keys are uppercase; lookup
// uppercases the wire name, making dispatch case-insensitive like Redis.
var commandTable = map[string]*command{
	"PING":    {name: "ping", arity: -1, handler: pingCommand},
	"ECHO":    {name: "echo", arity: 2, handler: echoCommand},
	"QUIT":    {name: "quit", arity: 1, handler: quitCommand},
	"COMMAND": {name: "command", arity: -1, handler: commandCommand},

}

// dispatch is the analog of processCommand + call in server.c: look up the
// command, enforce arity, execute, and account stats.
func (s *Server) dispatch(c *client, args [][]byte) {
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok {
		s.stats.UnknownCommands++
		// Format matches commandCheckExistence (server.c): the args suffix
		// appears only when arguments are present, each quoted, capped.
		msg := fmt.Sprintf("ERR unknown command '%.128s'", args[0])
		if len(args) >= 2 {
			var b strings.Builder
			for _, a := range args[1:] {
				if b.Len() >= 128 {
					break
				}
				fmt.Fprintf(&b, "'%.128s' ", a)
			}
			msg += ", with args beginning with: " + b.String()
		}
		c.out = resp.AppendError(c.out, msg)
		return
	}
	if (cmd.arity > 0 && len(args) != cmd.arity) || len(args) < -cmd.arity {
		c.out = resp.AppendError(c.out,
			fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.name))
		return
	}
	s.stats.TotalCommandsProcessed++
	s.stats.CommandCalls[cmd.name]++
	cmd.handler(s, c, args)
}

// pingCommand: PING -> +PONG, PING msg -> $msg (t_string-free; lives in
// server.c in Redis).
func pingCommand(s *Server, c *client, args [][]byte) {
	switch len(args) {
	case 1:
		c.out = resp.AppendSimpleString(c.out, "PONG")
	case 2:
		c.out = resp.AppendBulk(c.out, args[1])
	default:
		c.out = resp.AppendError(c.out, "ERR wrong number of arguments for 'ping' command")
	}
}

func echoCommand(s *Server, c *client, args [][]byte) {
	c.out = resp.AppendBulk(c.out, args[1])
}

func quitCommand(s *Server, c *client, args [][]byte) {
	c.out = resp.AppendSimpleString(c.out, "OK")
	c.closeAfterReply = true
}

// commandCommand is a stub (*0) so redis-cli's startup COMMAND DOCS probe
// doesn't spew an error. Real metadata arrives with later phases.
func commandCommand(s *Server, c *client, args [][]byte) {
	c.out = resp.AppendArrayHeader(c.out, 0)
}
