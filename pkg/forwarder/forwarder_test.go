// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
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

// startForwarder starts a forwarder with a single TCP rule to dest and returns the
// address clients should dial.
func startForwarder(t *testing.T, dest string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	fwd := NewForwarder(ctx)
	rule := Rule{Protocol: corev1.ProtocolTCP, Port: freePort(t), Dest: dest}
	require.NoError(t, fwd.Reconcile([]Rule{rule}))
	return fmt.Sprintf("127.0.0.1:%d", rule.Port)
}

// waitForGoroutines waits until the number of goroutines drops back to at most baseline.
// It polls from the test goroutine: require.Eventually would count its own goroutines.
func waitForGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: %d, want at most %d", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestForwarderClientCloseReachesDestination checks that a client closing its connection
// cleanly is propagated to the destination, and that nothing is left behind.
func TestForwarderClientCloseReachesDestination(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	eofs := make(chan error, 32)
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, err := io.Copy(io.Discard, c)
				eofs <- err
			}()
		}
	}()
	addr := startForwarder(t, backend.Addr().String())
	baseline := runtime.NumGoroutine()

	const conns = 20
	for range conns {
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		_, err = conn.Write([]byte("ping"))
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	for i := range conns {
		select {
		case err := <-eofs:
			require.NoError(t, err, "destination did not see a clean EOF")
		case <-time.After(5 * time.Second):
			t.Fatalf("destination saw the client close on %d of %d connections", i, conns)
		}
	}
	waitForGoroutines(t, baseline)
}

// TestForwarderClientHalfCloseKeepsResponse checks that a client closing only its write
// side still receives the response the destination sends after seeing EOF.
func TestForwarderClientHalfCloseKeepsResponse(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				request, err := io.ReadAll(c)
				if err != nil {
					return
				}
				_, _ = c.Write(append([]byte("pong:"), request...))
			}()
		}
	}()
	addr := startForwarder(t, backend.Addr().String())
	baseline := runtime.NumGoroutine()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcpConn.CloseWrite())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	data, err := io.ReadAll(conn)
	require.NoError(t, err, "client never received the response after its half-close")
	require.EqualT(t, "pong:ping", string(data))
	require.NoError(t, conn.Close())
	waitForGoroutines(t, baseline)
}

// TestForwarderDestinationCloseReachesClient checks that a destination closing its
// connection cleanly is propagated to the client.
func TestForwarderDestinationCloseReachesClient(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("pong"))
			_ = c.Close()
		}
	}()
	addr := startForwarder(t, backend.Addr().String())
	baseline := runtime.NumGoroutine()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	data, err := io.ReadAll(conn)
	require.NoError(t, err, "client never saw the destination close")
	require.EqualT(t, "pong", string(data))
	require.NoError(t, conn.Close())
	waitForGoroutines(t, baseline)
}

// TestForwarderClosesClientOnDialError checks that the client connection is closed when
// the destination cannot be reached, instead of being left open.
func TestForwarderClosesClientOnDialError(t *testing.T) {
	addr := startForwarder(t, fmt.Sprintf("127.0.0.1:%d", freePort(t)))
	baseline := runtime.NumGoroutine()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	requireClosed(t, conn, "client connection left open after a dial error")
	require.NoError(t, conn.Close())
	waitForGoroutines(t, baseline)
}

// requireClosed checks that a read on conn ends with EOF or an error instead of a timeout,
// which would mean the peer side was left open.
func requireClosed(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err := io.ReadAll(conn)
	var netErr net.Error
	require.False(t, errors.As(err, &netErr) && netErr.Timeout(), msg)
}

// TestForwarderShutdownClosesActiveConnection checks that stopping the forwarder closes a
// connection whose copies are still blocked in both directions.
func TestForwarderShutdownClosesActiveConnection(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fwd := NewForwarder(ctx)
	rule := Rule{Protocol: corev1.ProtocolTCP, Port: freePort(t), Dest: backend.Addr().String()}
	require.NoError(t, fwd.Reconcile([]Rule{rule}))

	client, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", rule.Port))
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	destination, err := backend.Accept()
	require.NoError(t, err)
	defer func() { _ = destination.Close() }()

	// Exchange data both ways so that both copies are running before the shutdown.
	_, err = client.Write([]byte("ping"))
	require.NoError(t, err)
	require.NoError(t, destination.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadFull(destination, make([]byte, len("ping")))
	require.NoError(t, err)
	_, err = destination.Write([]byte("pong"))
	require.NoError(t, err)
	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadFull(client, make([]byte, len("pong")))
	require.NoError(t, err)

	cancel()
	done := make(chan error, 1)
	go func() { done <- fwd.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("forwarder did not stop with a connection still open")
	}
	requireClosed(t, client, "client connection left open after shutdown")
	requireClosed(t, destination, "destination connection left open after shutdown")
}

// TestForwarderDestinationResetClosesClient checks that a destination resetting an
// established connection closes the client side as well, while the client stays open.
func TestForwarderDestinationResetClosesClient(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			_, _ = io.ReadFull(c, make([]byte, len("ping")))
			// A zero linger turns Close into a reset (RST) instead of a clean FIN.
			_ = c.(*net.TCPConn).SetLinger(0)
			_ = c.Close()
		}
	}()
	addr := startForwarder(t, backend.Addr().String())
	baseline := runtime.NumGoroutine()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	requireClosed(t, conn, "client connection left open after the destination reset")
	// Checked while the client is still open: closing it first would end the
	// client-to-destination copy on its own and hide a leak.
	waitForGoroutines(t, baseline)
}
