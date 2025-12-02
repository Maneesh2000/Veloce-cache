//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package reactor

import "golang.org/x/sys/unix"

// kqueuePoller implements Poller on top of kqueue(2).
//
// kqueue models read and write interest as two independent filters
// (EVFILT_READ / EVFILT_WRITE), unlike epoll's single event mask, so
// Add/Modify reconcile each filter separately.
type kqueuePoller struct {
	kq int
}

// New creates the platform poller (kqueue on this platform).
func New() (Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(kq)
	return &kqueuePoller{kq: kq}, nil
}

// setFilter enables or disables one filter for fd.
func (p *kqueuePoller) setFilter(fd int, filter int, enable bool) error {
	flags := unix.EV_ADD
	if !enable {
		flags = unix.EV_DELETE
	}
	var ev unix.Kevent_t
	unix.SetKevent(&ev, fd, filter, flags)
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	// Deleting a filter that was never added is not an error for us: callers
	// express desired state, and "absent" already matches "disabled".
	if err == unix.ENOENT && !enable {
		return nil
	}
	return err
}

func (p *kqueuePoller) apply(fd int, readable, writable bool) error {
	if err := p.setFilter(fd, unix.EVFILT_READ, readable); err != nil {
		return err
	}
	return p.setFilter(fd, unix.EVFILT_WRITE, writable)
}

