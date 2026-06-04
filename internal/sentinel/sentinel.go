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
// Valkey and ValkeySentinel controllers.
package sentinel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"

	vclient "github.com/valkey-io/valkey-go"
)

// Port is the canonical Sentinel TCP port.
const Port = 26379

// ErrNoMaster is returned when a sentinel does not (yet) know about the
// requested master name.
var ErrNoMaster = errors.New("sentinel: no such master")

// MasterAddr is the address Sentinel reports for a monitored master.
type MasterAddr struct {
	IP   string
	Port int
}

func (a MasterAddr) Empty() bool { return a.IP == "" }
func (a MasterAddr) String() string {
	return fmt.Sprintf("%s:%d", a.IP, a.Port)
}

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

// GetMasterAddr asks the sentinel for the current master address for
// the named master. Returns ErrNoMaster when the sentinel does not yet
// know about this master.
func (c *Client) GetMasterAddr(ctx context.Context, masterName string) (MasterAddr, error) {
	cmd := c.client.B().Arbitrary("SENTINEL", "GET-MASTER-ADDR-BY-NAME", masterName).Build()
	pairs, err := c.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		if isNoMasterErr(err) {
			return MasterAddr{}, ErrNoMaster
		}
		return MasterAddr{}, fmt.Errorf("SENTINEL GET-MASTER-ADDR-BY-NAME %s on %s: %w", masterName, c.addr, err)
	}
	if len(pairs) < 2 {
		return MasterAddr{}, nil
	}
	port, err := strconv.Atoi(pairs[1])
	if err != nil {
		return MasterAddr{}, fmt.Errorf("parse port %q from sentinel %s: %w", pairs[1], c.addr, err)
	}
	return MasterAddr{IP: pairs[0], Port: port}, nil
}

// Monitor issues `SENTINEL MONITOR <name> <ip> <port> <quorum>`.
func (c *Client) Monitor(ctx context.Context, masterName, ip string, port, quorum int) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "MONITOR", masterName, ip, strconv.Itoa(port), strconv.Itoa(quorum)).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL MONITOR %s %s:%d quorum=%d on %s: %w", masterName, ip, port, quorum, c.addr, err)
	}
	return nil
}

// Set issues `SENTINEL SET <name> <option> <value>`.
func (c *Client) Set(ctx context.Context, masterName, option, value string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "SET", masterName, option, value).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL SET %s %s=%s on %s: %w", masterName, option, value, c.addr, err)
	}
	return nil
}

// Failover issues `SENTINEL FAILOVER <name>`.
func (c *Client) Failover(ctx context.Context, masterName string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "FAILOVER", masterName).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL FAILOVER %s on %s: %w", masterName, c.addr, err)
	}
	return nil
}

// Remove issues `SENTINEL REMOVE <name>`.
func (c *Client) Remove(ctx context.Context, masterName string) error {
	cmd := c.client.B().Arbitrary("SENTINEL", "REMOVE", masterName).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("SENTINEL REMOVE %s on %s: %w", masterName, c.addr, err)
	}
	return nil
}

// Masters returns the names of every master this sentinel knows about.
func (c *Client) Masters(ctx context.Context) ([]string, error) {
	cmd := c.client.B().Arbitrary("SENTINEL", "MASTERS").Build()
	rows, err := c.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, fmt.Errorf("SENTINEL MASTERS on %s: %w", c.addr, err)
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		pairs, err := row.AsStrSlice()
		if err != nil {
			continue
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i] == "name" {
				out = append(out, pairs[i+1])
				break
			}
		}
	}
	return out, nil
}

// Ping is a liveness check.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Do(ctx, c.client.B().Ping().Build()).Error()
}

func isNoMasterErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such master") ||
		strings.Contains(msg, "no master with that name")
}
