// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-openapi/testify/v2/require"
	corev1 "k8s.io/api/core/v1"
)

func TestForwarder(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	fwd := NewForwarder(ctx)

	rules := []Rule{
		{
			Protocol: corev1.ProtocolTCP,
			Port:     0,
			Dest:     "example.com",
		},
		{
			Protocol: corev1.ProtocolTCP,
			Port:     0,
			Dest:     "google.com",
		},
	}
	err := fwd.Reconcile(rules)
	require.NoError(t, err)
	require.EqualT(t, len(rules), len(fwd.lnCancels))

	cancel()
	err = fwd.Wait()
	require.NoError(t, err)
}

func freePort(t *testing.T) int32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return int32(port)
}

func TestForwarderRuleReAdd(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fwd := NewForwarder(ctx)

	rule := Rule{Protocol: corev1.ProtocolTCP, Port: freePort(t), Dest: "127.0.0.1:9"}
	addr := fmt.Sprintf("127.0.0.1:%d", rule.Port)

	require.NoError(t, fwd.Reconcile([]Rule{rule}))
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	require.NoError(t, fwd.Reconcile(nil))
	require.Empty(t, fwd.lnCancels)
	require.Eventually(t, func() bool {
		_, err := net.Dial("tcp", addr)
		return err != nil
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, fwd.Reconcile([]Rule{rule}))
	conn, err = net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestForwarderListenErrorRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fwd := NewForwarder(ctx)

	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := int32(ln.Addr().(*net.TCPAddr).Port)
	rule := Rule{Protocol: corev1.ProtocolTCP, Port: port, Dest: "127.0.0.1:9"}

	require.Error(t, fwd.Reconcile([]Rule{rule}))
	require.Empty(t, fwd.lnCancels)

	require.NoError(t, ln.Close())
	require.NoError(t, fwd.Reconcile([]Rule{rule}))
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
