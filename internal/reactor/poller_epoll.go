//go:build linux

package reactor

import "golang.org/x/sys/unix"

// epollPoller implements Poller on top of epoll(7), level-triggered.
type epollPoller struct {
	epfd int
}

// New creates the platform poller (epoll on this platform).
func New() (Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &epollPoller{epfd: epfd}, nil
}

func mask(readable, writable bool) uint32 {
	var m uint32
	if readable {
		m |= unix.EPOLLIN
	}
	if writable {
		m |= unix.EPOLLOUT
	}
	return m
}

func (p *epollPoller) ctl(op int, fd int, readable, writable bool) error {
	ev := unix.EpollEvent{Events: mask(readable, writable), Fd: int32(fd)}
	return unix.EpollCtl(p.epfd, op, fd, &ev)
}

func (p *epollPoller) Add(fd int, readable, writable bool) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, readable, writable)
}

func (p *epollPoller) Modify(fd int, readable, writable bool) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, readable, writable)
}

func (p *epollPoller) Remove(fd int) error {
	err := unix.EpollCtl(p.epfd, unix.EPOLL_CTL_DEL, fd, nil)
	// Already gone (fd closed, or never registered) is fine for teardown.
	if err == unix.ENOENT || err == unix.EBADF {
		return nil
	}
	return err
}

func (p *epollPoller) Wait(events []Event, timeoutMs int) (int, error) {
	epEvents := make([]unix.EpollEvent, len(events))
	for {
		n, err := unix.EpollWait(p.epfd, epEvents, timeoutMs)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			ev := &epEvents[i]
			events[i] = Event{
				Fd: int(ev.Fd),
				// HUP/ERR are folded into Readable so the caller's read()
				// observes EOF/error and closes the client — Redis does the
				// same fold in ae_epoll.c.
				Readable: ev.Events&(unix.EPOLLIN|unix.EPOLLHUP|unix.EPOLLERR) != 0,
				Writable: ev.Events&unix.EPOLLOUT != 0,
			}
		}
		return n, nil
	}
}

func (p *epollPoller) Close() error {
	return unix.Close(p.epfd)
}
