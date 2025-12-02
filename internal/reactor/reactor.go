// Package reactor provides a minimal readiness-notification abstraction over
// the platform poller: epoll on Linux, kqueue on macOS/BSD.
//
// This is the Go analog of Redis's ae.c / ae_epoll.c / ae_kqueue.c layer.
// Both backends are used in level-triggered mode, mirroring Redis: as long as
// a socket has unread data (or writable space, when write interest is armed),
// Wait keeps reporting it.
package reactor

// Event reports readiness for one file descriptor. With kqueue a single fd
// may surface as two Events in the same Wait batch (one per filter); callers
// must treat Readable/Writable independently.
type Event struct {
	Fd       int
	Readable bool
	Writable bool
}

// Poller is the platform-neutral readiness API.
//
// Interest is expressed as the *desired* state: Add/Modify(fd, readable,
// writable) declare exactly which directions the caller wants notifications
// for, and the backend reconciles.
type Poller interface {
	// Add registers a new fd with the given interest.
	Add(fd int, readable, writable bool) error
	// Modify replaces the interest set of an already-registered fd.
	Modify(fd int, readable, writable bool) error
	// Remove deregisters the fd entirely. Safe to call before closing it.
	Remove(fd int) error
	// Wait blocks until at least one event is ready or timeoutMs elapses.
	// timeoutMs < 0 blocks indefinitely. Fills events and returns the count.
	// A return of (0, nil) means the timeout expired.
	Wait(events []Event, timeoutMs int) (int, error)
	Close() error
}
