// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Forwarder struct {
	mu        sync.Mutex
	dialer    net.Dialer
	groupCtx  context.Context
	group     *errgroup.Group
	lnCancels map[string]context.CancelFunc
}

func NewForwarder(ctx context.Context) *Forwarder {
	group, groupCtx := errgroup.WithContext(ctx)
	dialer := net.Dialer{
		Timeout: 30 * time.Second,
	}
	fwd := &Forwarder{
		dialer:    dialer,
		groupCtx:  groupCtx,
		group:     group,
		lnCancels: map[string]context.CancelFunc{},
	}
	return fwd
}

func (f *Forwarder) Wait() error {
	<-f.groupCtx.Done()
	return f.group.Wait()
}

func (f *Forwarder) Reconcile(rules []Rule) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	slog.Default().Info("reconciling rules", "count", len(rules))

	oldLnCancels := maps.Clone(f.lnCancels)
	for _, rule := range rules {
		delete(oldLnCancels, rule.String())
		if _, ok := f.lnCancels[rule.String()]; ok {
			continue
		}

		lnCtx, lnCancel := context.WithCancel(f.groupCtx)
		lc := net.ListenConfig{}
		ln, err := lc.Listen(lnCtx, strings.ToLower(string(rule.Protocol)), fmt.Sprintf(":%d", rule.Port))
		if err != nil {
			lnCancel()
			return err
		}
		f.lnCancels[rule.String()] = lnCancel
		f.group.Go(func() error {
			<-lnCtx.Done()
			return ln.Close()
		})
		f.group.Go(func() error {
			wg := &sync.WaitGroup{}
			for {
				conn, err := ln.Accept()
				if err != nil {
					wg.Wait()
					if errors.Is(err, net.ErrClosed) {
						return nil
					}
					return err
				}
				wg.Go(func() {
					err := f.forwardConn(lnCtx, conn, rule)
					if err != nil {
						slog.Default().Error("connection forwarding error", "error", err)
					}
				})
			}
		})
	}

	for key, cancel := range oldLnCancels {
		cancel()
		delete(f.lnCancels, key)
	}

	return nil
}

func (f *Forwarder) forwardConn(ctx context.Context, conn net.Conn, rule Rule) error {
	defer func() { _ = conn.Close() }()
	dst, err := f.dialer.DialContext(ctx, strings.ToLower(string(rule.Protocol)), rule.Dest)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()

	// A failed copy or a forwarder shutdown closes both connections, which unblocks the
	// other copy. A clean end of both copies returns normally and the deferred closes run.
	group, groupCtx := errgroup.WithContext(ctx)
	stop := context.AfterFunc(groupCtx, func() {
		_ = conn.Close()
		_ = dst.Close()
	})
	defer stop()
	group.Go(func() error {
		return copyAndCloseWrite(dst, conn)
	})
	group.Go(func() error {
		return copyAndCloseWrite(conn, dst)
	})
	return group.Wait()
}

// copyAndCloseWrite copies src to dst until EOF, then half-closes dst so that the peer
// on that side sees the end of the stream as well.
func copyAndCloseWrite(dst, src net.Conn) error {
	_, err := io.Copy(dst, src)
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	return err
}
