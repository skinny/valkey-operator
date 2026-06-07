/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package sentinel

import (
	"errors"
	"testing"
)

// isUnknownCoordinatedOption is the gate that decides whether to fall
// back to plain SENTINEL FAILOVER on a pre-9.0 sentinel. False
// positives swallow real failures; false negatives strand the rollout
// on a syntax error. So pin the classifier to a small, real corpus.
func TestIsUnknownCoordinatedOption(t *testing.T) {
	cases := []struct {
		name   string
		err    string
		expect bool
	}{
		// Pre-9.0 sentinel rejecting the extra COORDINATED token.
		{"arity", "ERR wrong number of arguments for 'sentinel|failover' command", true},
		{"syntax", "ERR syntax error", true},
		{"unknown option", "ERR unknown option for SENTINEL FAILOVER", true},

		// 9.0+ sentinel returning a real operational error - MUST NOT fall back.
		{"inprog", "INPROG Failover already in progress", false},
		{"nogoodslave", "NOGOODSLAVE No suitable replica to promote", false},
		{"unknown master", "ERR No such master with that name", false},
		{"network", "dial tcp: i/o timeout", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isUnknownCoordinatedOption(errors.New(tc.err))
			if got != tc.expect {
				t.Fatalf("err %q: got %v, want %v", tc.err, got, tc.expect)
			}
		})
	}
}
