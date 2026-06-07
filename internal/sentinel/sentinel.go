/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package sentinel wraps the SENTINEL command surface needed by the
// ValkeySentinel controller.
package sentinel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	vclient "github.com/valkey-io/valkey-go"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ErrCoordinatedUnsupported is returned from FailoverCoordinated when
// the sentinel rejects the COORDINATED keyword — i.e. it's running a
// version older than Valkey 9.0 where PR #1292 added the option.
// Callers fall back to plain Failover.
var ErrCoordinatedUnsupported = errors.New("SENTINEL FAILOVER COORDINATED not supported")

// Port is the canonical Sentinel TCP port.
const Port = 26379

// Client is a thin SENTINEL-command wrapper over the valkey-go client.
type Client struct {
	addr   string
	client vclient.Client
}

// New dials a single sentinel at host:port.
func New(host string, port int, tlsConfig *tls.Config) (*Client, error) {
	c, err := vclient.NewClient(vclient.ClientOption{
		InitAddress:       []string{fmt.Sprintf("%s:%d", host, port)},
		ForceSingleClient: true,
		TLSConfig:         tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("connect sentinel %s:%d: %w", host, port, err)
	}
	return &Client{addr: fmt.Sprintf("%s:%d", host, port), client: c}, nil
}

// Addr returns the sentinel's host:port.
func (c *Client) Addr() string { return c.addr }

// Close releases the underlying connection.
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// Remove issues `SENTINEL REMOVE <name>`.
func (c *Client) Remove(ctx context.Context, masterName string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "REMOVE", masterName).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL REMOVE %s on %s: %w", masterName, c.addr, err)
	}
	return nil
}

// Failover issues `SENTINEL FAILOVER <name>` so a sentinel is the one
// driving the promotion - sentinel picks the target replica, runs its
// MULTI/EXEC promotion transaction against it, and updates its own
// gossiped view atomically. Used by the operator's planned-rollout
// path when a ValkeySentinel selects this Valkey.
func (c *Client) Failover(ctx context.Context, masterName string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "FAILOVER", masterName).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL FAILOVER %s on %s: %w", masterName, c.addr, err)
	}
	return nil
}

// FailoverCoordinated issues `SENTINEL FAILOVER <name> COORDINATED`
// (Valkey 9.0+, see https://github.com/valkey-io/valkey/pull/1292).
// The chosen replica catches up to the master via Valkey's `FAILOVER`
// command before the role flip, so the other replicas continue with
// `psync` after promotion instead of triggering a full resync — a
// material difference for any non-trivial dataset.
//
// On a sentinel that doesn't recognise the keyword (pre-9.0),
// returns ErrCoordinatedUnsupported so callers can fall back to the
// plain Failover. Any other failure is returned wrapped.
func (c *Client) FailoverCoordinated(ctx context.Context, masterName string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "FAILOVER", masterName, "COORDINATED").Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		if isUnknownCoordinatedOption(err) {
			return ErrCoordinatedUnsupported
		}
		return fmt.Errorf("SENTINEL FAILOVER %s COORDINATED on %s: %w", masterName, c.addr, err)
	}
	return nil
}

// isUnknownCoordinatedOption returns true when the error from a
// SENTINEL FAILOVER ... COORDINATED call is the parser rejecting the
// keyword on a pre-9.0 sentinel. We match conservatively on the
// substrings Valkey/Redis use for ARITY / SYNTAX / unknown-option
// failures so a 9.0+ sentinel returning a real operational error
// (e.g. `INPROG`, `NOGOODSLAVE`) is NOT swallowed.
func isUnknownCoordinatedOption(err error) bool {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "wrong number of arguments"):
		return true
	case strings.Contains(msg, "syntax error"):
		return true
	case strings.Contains(msg, "unknown") && strings.Contains(msg, "option"):
		return true
	}
	return false
}

// Masters returns the names of every master this sentinel knows about.
func (c *Client) Masters(ctx context.Context) ([]string, error) {
	log := logf.FromContext(ctx)
	cmd := c.client.B().Arbitrary("SENTINEL", "MASTERS").Build()
	rows, err := c.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, fmt.Errorf("SENTINEL MASTERS on %s: %w", c.addr, err)
	}
	out := make([]string, 0, len(rows))
	for i, row := range rows {
		// SENTINEL MASTERS returns each entry as a key-value
		// structure. In RESP2 that's a flat array; in RESP3
		// valkey-server returns it as a Map. AsStrMap handles both.
		fields, err := row.AsStrMap()
		if err != nil {
			log.V(1).Info("SENTINEL MASTERS: skipping malformed entry", "addr", c.addr, "index", i, "err", err)
			continue
		}
		if name := fields["name"]; name != "" {
			out = append(out, name)
		}
	}
	return out, nil
}
