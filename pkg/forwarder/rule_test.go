// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"encoding/json"
	"testing"

	"github.com/go-openapi/testify/v2/require"
	corev1 "k8s.io/api/core/v1"
)

func TestRuleManager(t *testing.T) {
	t.Parallel()

	ruleMgr, err := NewRuleManager(nil)
	require.NoError(t, err)
	require.Empty(t, ruleMgr.initRules)
	require.Empty(t, ruleMgr.initRules)
	require.Empty(t, ruleMgr.portIdx)
	require.Empty(t, ruleMgr.allocRules)

	initial := Rule{
		Protocol: corev1.ProtocolTCP,
		Port:     1234,
		Dest:     "example.com",
	}
	b, err := json.Marshal([]Rule{initial})
	require.NoError(t, err)
	data := map[string]string{
		RuleJsonKey: string(b),
	}
	ruleMgr, err = NewRuleManager(data)
	require.NoError(t, err)
	require.Equal(t, []Rule{initial}, ruleMgr.initRules)
	require.Equal(t, map[string]Rule{"example.com": initial}, ruleMgr.initRuleIdx)
	require.Equal(t, map[int32]any{initial.Port: nil}, ruleMgr.portIdx)
	require.Empty(t, ruleMgr.allocRules)
	require.Equal(t, []Rule{initial}, ruleMgr.AllRules())
	data, err = ruleMgr.Data()
	require.NoError(t, err)
	require.Equal(t, map[string]string{RuleJsonKey: "[]"}, data)

	rule := ruleMgr.Allocate(initial.Protocol, initial.Dest)
	require.EqualT(t, initial, rule)
	rule = ruleMgr.Allocate(corev1.ProtocolTCP, "192.168.1.1:6565")
	require.EqualT(t, corev1.ProtocolTCP, rule.Protocol)
	require.EqualT(t, "192.168.1.1:6565", rule.Dest)
	require.Len(t, ruleMgr.allocRules, 2)
	_, err = ruleMgr.Data()
	require.NoError(t, err)
}
