// Copyright 2014 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdmods

import (
	"encoding/json"
	"path"
	"time"

	"github.com/ecnahc515/etcd_modules/etcd"
)

const (
	leasePrefix = "lease"
)

type leaseMetadata struct {
	MachineID string
	Version   int
}

// lease implements the Lease interface
type lease struct {
	key    string
	meta   leaseMetadata
	idx    uint64
	ttl    time.Duration
	client etcd.Client
}

func (l *lease) Release() error {
	req := etcd.Delete{
		Key:           l.key,
		PreviousIndex: l.idx,
	}
	_, err := l.client.Do(&req)
	return err
}

func (l *lease) Renew(period time.Duration) error {
	val, err := serializeLeaseMetadata(l.meta.MachineID, l.meta.Version)
	req := etcd.Set{
		Key:           l.key,
		Value:         val,
		PreviousIndex: l.idx,
		TTL:           period,
	}

	resp, err := l.client.Do(&req)
	if err != nil {
		return err
	}

	renewed := leaseFromResult(resp, l.client)
	*l = *renewed

	return nil
}

func (l *lease) MachineID() string {
	return l.meta.MachineID
}

func (l *lease) Version() int {
	return l.meta.Version
}

func (l *lease) Index() uint64 {
	return l.idx
}

func (l *lease) TimeRemaining() time.Duration {
	return l.ttl
}

func serializeLeaseMetadata(machID string, ver int) (string, error) {
	meta := leaseMetadata{
		MachineID: machID,
		Version:   ver,
	}

	b, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

type LeaseManager struct {
	client    etcd.Client
	keyPrefix string
}

func NewLeaseManager(client etcd.Client, keyPrefix string) *LeaseManager {
	return &LeaseManager{client: client, keyPrefix: keyPrefix}
}

func (r *LeaseManager) leasePath(name string) string {
	return path.Join(r.keyPrefix, leasePrefix, name)
}

func (r *LeaseManager) GetLease(name string) (Lease, error) {
	key := r.leasePath(name)
	req := etcd.Get{
		Key: key,
	}

	resp, err := r.client.Do(&req)
	if err != nil {
		if etcd.IsKeyNotFound(err) {
			err = nil
		}
		return nil, err
	}

	l := leaseFromResult(resp, r.client)
	return l, nil
}

func (r *LeaseManager) StealLease(name, machID string, ver int, period time.Duration, idx uint64) (Lease, error) {
	val, err := serializeLeaseMetadata(machID, ver)
	if err != nil {
		return nil, err
	}

	req := etcd.Set{
		Key:           r.leasePath(name),
		Value:         val,
		PreviousIndex: idx,
		TTL:           period,
	}

	resp, err := r.client.Do(&req)
	if err != nil {
		if etcd.IsNodeExist(err) {
			err = nil
		}
		return nil, err
	}

	l := leaseFromResult(resp, r.client)
	return l, nil
}

func (r *LeaseManager) AcquireLease(name string, machID string, ver int, period time.Duration) (Lease, error) {
	val, err := serializeLeaseMetadata(machID, ver)
	if err != nil {
		return nil, err
	}

	req := etcd.Create{
		Key:   r.leasePath(name),
		Value: val,
		TTL:   period,
	}

	resp, err := r.client.Do(&req)
	if err != nil {
		if etcd.IsNodeExist(err) {
			err = nil
		}
		return nil, err
	}

	l := leaseFromResult(resp, r.client)
	return l, nil
}

func leaseFromResult(res *etcd.Result, client etcd.Client) *lease {
	l := &lease{
		key:    res.Node.Key,
		idx:    res.Node.ModifiedIndex,
		ttl:    res.Node.TTLDuration(),
		client: client,
	}

	err := json.Unmarshal([]byte(res.Node.Value), &l.meta)

	// fall back to using the entire value as the MachineID for
	// backwards-compatibility with engines that are not aware
	// of this versioning mechanism
	if err != nil {
		l.meta = leaseMetadata{
			MachineID: res.Node.Value,
			Version:   0,
		}
	}

	return l
}
