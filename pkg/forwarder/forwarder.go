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
		f.lnCancels[rule.String()] = lnCancel
		lc := net.ListenConfig{}
		ln, err := lc.Listen(lnCtx, strings.ToLower(string(rule.Protocol)), fmt.Sprintf(":%d", rule.Port))
		if err != nil {
			return err
		}
		f.group.Go(func() error {
			conn, err := ln.Accept()
			if err != nil {
				return err
			}
			f.handleConn(lnCtx, conn, rule)
			return nil
		})
	}

	for _, cancel := range oldLnCancels {
		cancel()
	}

	return nil
}

func (f *Forwarder) handleConn(ctx context.Context, conn net.Conn, rule Rule) {
	f.group.Go(func() error {
		dst, err := f.dialer.DialContext(ctx, strings.ToLower(string(rule.Protocol)), rule.Dest)
		if err != nil {
			return err
		}
		f.group.Go(func() error {
			_, err := io.Copy(dst, conn)
			return errors.Join(err, dst.Close())
		})
		f.group.Go(func() error {
			_, err := io.Copy(conn, dst)
			return errors.Join(err, dst.Close())
		})
		return nil
	})
}
