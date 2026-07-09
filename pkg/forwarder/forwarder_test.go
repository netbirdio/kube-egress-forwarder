// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"context"
	"testing"

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
