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
func (s *Server) Serve() error {
	defer s.closeAll()
	events := make([]reactor.Event, 128)
	for !s.stopping {
		n, err := s.poller.Wait(events, -1)
		if err != nil {
			return fmt.Errorf("poller wait: %w", err)
		}
		for i := 0; i < n; i++ {
			ev := events[i]
			switch ev.Fd {
			case s.listenFd:
				s.acceptClients()
			case s.wakeR:
				s.drainWakePipe()
			default:
				// The client may have been closed earlier in this same
				// batch (e.g. read error), so look it up fresh.
				c, ok := s.clients[ev.Fd]
				if !ok {
					continue
				}
				if ev.Readable {
					s.handleReadable(c)
				}
				// Re-check liveness: the read may have closed it.
				if c, ok = s.clients[ev.Fd]; ok && ev.Writable {
					s.flushOutput(c)
				}
			}
		}
	}
	return nil
}

// Stop wakes the event loop and makes Serve return. Safe to call from any
// goroutine; idempotent enough for test teardown.
func (s *Server) Stop() {
	// A single byte on the self-pipe is the wakeup; the loop sets stopping.
	unix.Write(s.wakeW, []byte{1})
}

func (s *Server) drainWakePipe() {
	var buf [64]byte
	for {
		n, err := unix.Read(s.wakeR, buf[:])
		if n <= 0 || err != nil {
			break
		}
	}
	s.stopping = true
}

// acceptClients accepts until EAGAIN (the analog of acceptTcpHandler).
func (s *Server) acceptClients() {
	for i := 0; i < maxAcceptsPerCall; i++ {
		fd, sa, err := unix.Accept(s.listenFd)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK || err == unix.ECONNABORTED {
				return
			}
			// EMFILE etc.: count it, log, and back off until next event.
			s.stats.RejectedConnections++
			s.logf("accept error: %v", err)
			return
		}
		unix.SetNonblock(fd, true)
		unix.CloseOnExec(fd)
		// Redis enables TCP_NODELAY on every client socket.
		unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)

		c := &client{fd: fd, addr: sockaddrString(sa), parser: resp.NewParser()}
		if err := s.poller.Add(fd, true, false); err != nil {
			s.stats.RejectedConnections++
			unix.Close(fd)
			continue
		}
		s.clients[fd] = c
		s.stats.TotalConnectionsReceived++
		s.stats.ConnectedClients++
	}
}

// handleReadable is readQueryFromClient: read one chunk, then drain every
// complete command out of the parser (which is what makes pipelining free),
// then attempt to flush the accumulated replies.
func (s *Server) handleReadable(c *client) {
	var buf [readChunk]byte
	n, err := unix.Read(c.fd, buf[:])
	switch {
	case err == unix.EAGAIN || err == unix.EWOULDBLOCK || err == unix.EINTR:
		return
	case err != nil:
		s.closeClient(c) // connection reset etc.
		return
	case n == 0:
		s.closeClient(c) // orderly EOF
		return
	}
	s.stats.TotalNetInputBytes += uint64(n)
	c.parser.Feed(buf[:n])

	// processInputBuffer: consume every complete pipelined command.
	for {
		args, perr := c.parser.Next()
		if perr != nil {
			s.stats.ProtocolErrors++
			c.out = resp.AppendError(c.out, "ERR "+perr.Error())
			c.closeAfterReply = true
			break
		}
		if args == nil {
			break // need more bytes
		}
		s.dispatch(c, args)
		if c.closeAfterReply {
			break // QUIT: stop consuming further pipelined input
		}
	}
	s.flushOutput(c)
}

// flushOutput is writeToClient + the EPOLLOUT protocol:
//   - write as much as the kernel accepts;
//   - on partial write / EAGAIN, arm write interest and finish later;
//   - once drained, disarm write interest and honor closeAfterReply.
func (s *Server) flushOutput(c *client) {
	for len(c.out) > 0 {
		n, err := unix.Write(c.fd, c.out)
		if n > 0 {
			s.stats.TotalNetOutputBytes += uint64(n)
			c.out = c.out[n:]
		}
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			// Kernel buffer full: register for writability and resume there.
			if !c.wantWrite {
				c.wantWrite = true
				s.poller.Modify(c.fd, true, true)
			}
			return
		}
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			s.closeClient(c) // EPIPE, ECONNRESET, ...
			return
		}
		// err == nil with a partial write: loop; the next Write either
		// makes progress or returns EAGAIN and we arm write interest.
	}
	// Fully drained.
	c.out = nil
	if c.wantWrite {
		c.wantWrite = false
		s.poller.Modify(c.fd, true, false)
	}
	if c.closeAfterReply {
		s.closeClient(c)
	}
}

func (s *Server) closeClient(c *client) {
	if _, ok := s.clients[c.fd]; !ok {
		return
	}
	delete(s.clients, c.fd)
	s.stats.ConnectedClients--
	s.poller.Remove(c.fd)
	unix.Close(c.fd)
}

func (s *Server) closeAll() {
	for _, c := range s.clients {
		s.closeClient(c)
	}
	if s.listenFd >= 0 {
		s.poller.Remove(s.listenFd)
		unix.Close(s.listenFd)
		s.listenFd = -1
	}
	if s.wakeR >= 0 {
		unix.Close(s.wakeR)
		unix.Close(s.wakeW)
		s.wakeR, s.wakeW = -1, -1
	}
	s.poller.Close()
}

func (s *Server) logf(format string, v ...any) {
	if s.Logger != nil {
		s.Logger.Printf(format, v...)
	}
}

// listenTCP creates the non-blocking listener: socket -> SO_REUSEADDR ->
// bind -> listen(511) -> O_NONBLOCK (anet.c's anetTcpServer distilled).
