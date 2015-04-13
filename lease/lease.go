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

package lease

import (
	"encoding/json"
	"path"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
)

const (
	leasePrefix = "lease"
)

type leaseMetadata struct {
	Owner   string
	Version int
}

// lease implements the Lease interface
type lease struct {
	key  string
	meta leaseMetadata
	idx  uint64
	ttl  int64
	kAPI etcd.KeysAPI
}

func (l *lease) Release() error {
	opts := &etcd.DeleteOptions{PrevIndex: l.idx}
	_, err := l.kAPI.Delete(context.Background(), l.key, opts)
	return err
}

func (l *lease) Renew(period time.Duration) error {
	val, err := serializeLeaseMetadata(l.meta.Owner, l.meta.Version)
	opts := &etcd.SetOptions{
		PrevIndex: l.idx,
		TTL:       period,
	}
	resp, err := l.kAPI.Set(context.Background(), l.key, val, opts)
	if err != nil {
		return err
	}
	renewed, err := leaseFromResult(resp, l.kAPI)
	if err != nil {
		return err
	}
	*l = *renewed

	return nil
}

func (l *lease) Owner() string {
	return l.meta.Owner
}

func (l *lease) Version() int {
	return l.meta.Version
}

func (l *lease) Index() uint64 {
	return l.idx
}

func (l *lease) TimeRemaining() int64 {
	return l.ttl
}

func serializeLeaseMetadata(owner string, ver int) (string, error) {
	meta := leaseMetadata{
		Owner:   owner,
		Version: ver,
	}

	b, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

type LeaseManager struct {
	kAPI      etcd.KeysAPI
	keyPrefix string
}

func NewLeaseManager(kAPI etcd.KeysAPI, keyPrefix string) *LeaseManager {
	return &LeaseManager{kAPI: kAPI, keyPrefix: keyPrefix}
}

func (r *LeaseManager) leasePath(name string) string {
	return path.Join(r.keyPrefix, leasePrefix, name)
}

func (r *LeaseManager) GetLease(name string) (Lease, error) {
	resp, err := r.kAPI.Get(context.Background(), r.leasePath(name), nil)
	if err != nil {
		if eerr, ok := err.(etcd.Error); ok && eerr.Code == etcd.ErrorCodeKeyNotFound {
			err = nil
		}
		return nil, err
	}
	l, err := leaseFromResult(resp, r.kAPI)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (r *LeaseManager) StealLease(name, owner string, ver int, period time.Duration, idx uint64) (Lease, error) {
	val, err := serializeLeaseMetadata(owner, ver)
	if err != nil {
		return nil, err
	}
	opts := &etcd.SetOptions{
		PrevIndex: idx,
		TTL:       period,
	}
	resp, err := r.kAPI.Set(context.Background(), r.leasePath(name), val, opts)
	if err != nil {
		if eerr, ok := err.(etcd.Error); ok && eerr.Code == etcd.ErrorCodeNodeExist {
			err = nil
		}
		return nil, err
	}
	l, err := leaseFromResult(resp, r.kAPI)
	if err != nil {
		return nil, err
	}
	return l, err
}

func (r *LeaseManager) AcquireLease(name string, owner string, ver int, period time.Duration) (Lease, error) {
	val, err := serializeLeaseMetadata(owner, ver)
	if err != nil {
		return nil, err
	}

	opts := &etcd.SetOptions{
		PrevExist: etcd.PrevNoExist,
		TTL:       period,
	}
	resp, err := r.kAPI.Set(context.Background(), r.leasePath(name), val, opts)
	if err != nil {
		if eerr, ok := err.(etcd.Error); ok && eerr.Code == etcd.ErrorCodeNodeExist {
			err = nil
		}
		return nil, err
	}

	l, err := leaseFromResult(resp, r.kAPI)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func leaseFromResult(res *etcd.Response, kAPI etcd.KeysAPI) (*lease, error) {
	l := &lease{
		key:  res.Node.Key,
		idx:  res.Node.ModifiedIndex,
		ttl:  res.Node.TTL,
		kAPI: kAPI,
	}

	err := json.Unmarshal([]byte(res.Node.Value), &l.meta)
	if err != nil {
		return nil, err
	}
	return l, nil
}
