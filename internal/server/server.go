// Package server implements the single-threaded event-loop server: the Go
// analog of Redis's server.c + networking.c request path —
//
//	readQueryFromClient -> processInputBuffer -> processCommand -> addReply
//	                        -> writeToClient (with EPOLLOUT re-arm on partial)
//
// One goroutine owns the loop; nothing else touches server or client state,
// so command execution is serial and atomic by construction.
package server

import (
	"errors"
	"fmt"
	"log"
	"net"

	"golang.org/x/sys/unix"

	"github.com/Maneesh2000/Veloce-cache/internal/reactor"
	"github.com/Maneesh2000/Veloce-cache/internal/resp"
	"github.com/Maneesh2000/Veloce-cache/internal/store"
)

const (
	// readChunk is the per-event read size (Redis PROTO_IOBUF_LEN = 16KB).
	// Level-triggered polling re-fires while more data is pending, so one
	// read per event keeps the loop fair across clients.
	readChunk = 16 * 1024
	// tcpBacklog matches Redis's default.
	tcpBacklog = 511
	// maxAcceptsPerCall bounds accepts per readiness event (Redis
	// MAX_ACCEPTS_PER_CALL) so one connect storm can't starve the loop.
	maxAcceptsPerCall = 1000
)

// client is the per-connection state (analog of the client struct in
// server.h, reduced to Phase 1 needs).
type client struct {
	fd     int
	addr   string
	parser *resp.Parser
	out    []byte // pending reply bytes (Redis: buf + reply list)

	wantWrite       bool // write interest currently registered with the poller
	closeAfterReply bool // QUIT or fatal protocol error: flush, then close
}

// Server owns the listener, the poller, and all clients.
type Server struct {
	poller   reactor.Poller
	listenFd int
	addr     string // actual bound address (resolves port 0)
	clients  map[int]*client
	db       *store.DB // the keyspace; touched only by the loop goroutine
	stats    Stats

	// Self-pipe used by Stop to wake the (possibly blocked) poller from
	// another goroutine. The read end lives in the poller like any fd.
	wakeR, wakeW int
	stopping     bool

	Logger *log.Logger // optional; nil silences
}

// New binds host:port (port 0 picks a free port) and prepares the event
// loop. Call Serve to start it.
func New(host string, port int) (*Server, error) {
	poller, err := reactor.New()
	if err != nil {
		return nil, err
	}

	lfd, addr, err := listenTCP(host, port)
	if err != nil {
		poller.Close()
		return nil, err
	}

	var pipeFds [2]int
	if err := unix.Pipe(pipeFds[:]); err != nil {
		unix.Close(lfd)
		poller.Close()
		return nil, err
	}
	unix.SetNonblock(pipeFds[0], true)
	unix.SetNonblock(pipeFds[1], true)

	s := &Server{
		poller:   poller,
		listenFd: lfd,
		addr:     addr,
		clients:  make(map[int]*client),
		db:       store.NewDB(),
		stats:    newStats(),
		wakeR:    pipeFds[0],
		wakeW:    pipeFds[1],
	}
	if err := poller.Add(lfd, true, false); err != nil {
		s.closeAll()
		return nil, err
	}
	if err := poller.Add(s.wakeR, true, false); err != nil {
		s.closeAll()
		return nil, err
	}
	return s, nil
}

// Addr returns the bound listen address (useful with port 0).
func (s *Server) Addr() string { return s.addr }

// Stats returns a copy of the server counters, folding in the keyspace
// hit/miss counters owned by the DB. Same contract as everything else here:
// only race-free once the loop has stopped (or from within it).
func (s *Server) Stats() Stats {
	st := s.stats.snapshot()
	st.KeyspaceHits = s.db.Hits
	st.KeyspaceMisses = s.db.Misses
	return st
}

// Serve runs the event loop until Stop is called. This is aeMain.
