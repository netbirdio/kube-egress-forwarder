// SPDX-License-Identifier: AGPL-3.0

package forwarder

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"

	corev1 "k8s.io/api/core/v1"
)

const RuleJsonKey = "rules.json"

type Rule struct {
	Protocol corev1.Protocol `json:"protocol"`
	Port     int32           `json:"port"`
	Dest     string          `json:"dest"`
}

func (r Rule) String() string {
	return fmt.Sprintf("%s-%d-%s", r.Protocol, r.Port, r.Dest)
}

type RuleManager struct {
	initRules   []Rule
	initRuleIdx map[string]Rule
	portIdx     map[int32]any
	allocRules  []Rule
}

func NewRuleManager(data map[string]string) (*RuleManager, error) {
	initRules := []Rule{}
	b := []byte(data[RuleJsonKey])
	if len(b) > 0 {
		err := json.Unmarshal(b, &initRules)
		if err != nil {
			return nil, err
		}
	}
	initRuleIdx := map[string]Rule{}
	portIdx := map[int32]any{}
	for _, rule := range initRules {
		if _, ok := portIdx[rule.Port]; ok {
			return nil, fmt.Errorf("duplicate port %d used in rules", rule.Port)
		}
		initRuleIdx[rule.Dest] = rule
		portIdx[rule.Port] = nil
	}

	pm := &RuleManager{
		initRules:   initRules,
		initRuleIdx: initRuleIdx,
		portIdx:     portIdx,
		allocRules:  []Rule{},
	}
	return pm, nil
}

func (p *RuleManager) Allocate(protocol corev1.Protocol, dest string) Rule {
	rule, ok := p.initRuleIdx[dest]
	if !ok {
		for {
			min := int32(1024)
			max := int32(65535)
			randPort := rand.Int32N(max-min+1) + min
			if _, ok := p.portIdx[randPort]; ok {
				continue
			}
			p.portIdx[randPort] = nil
			rule = Rule{
				Protocol: protocol,
				Port:     randPort,
				Dest:     dest,
			}
			break
		}
	}
	p.allocRules = append(p.allocRules, rule)
	return rule
}

func (p *RuleManager) AllRules() []Rule {
	return p.initRules
}

func (p *RuleManager) Data() (map[string]string, error) {
	b, err := json.Marshal(p.allocRules)
	if err != nil {
		return nil, err
	}
	data := map[string]string{
		RuleJsonKey: string(b),
	}
	return data, nil
}
