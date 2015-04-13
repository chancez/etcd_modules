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
	"reflect"
	"testing"

	etcd "github.com/coreos/etcd/client"
)

func TestSerializeLeaseMetadata(t *testing.T) {
	tests := []struct {
		owner string
		ver   int
		want  string
	}{
		{
			owner: "XXX",
			ver:   9,
			want:  `{"Owner":"XXX","Version":9}`,
		},
		{
			owner: "XXX",
			ver:   0,
			want:  `{"Owner":"XXX","Version":0}`,
		},
	}

	for i, tt := range tests {
		got, err := serializeLeaseMetadata(tt.owner, tt.ver)
		if err != nil {
			t.Errorf("case %d: unexpected err=%v", i, err)
			continue
		}
		if tt.want != got {
			t.Errorf("case %d: incorrect output from serializeLeaseMetadata\nwant=%s\ngot=%s", i, tt.want, got)
		}
	}
}

func TestLeaseFromResult(t *testing.T) {
	tests := []struct {
		res       etcd.Response
		want      *lease
		shouldErr bool
	}{
		// typical case
		{
			res: etcd.Response{
				Node: &etcd.Node{
					Key:           "/foo/bar",
					ModifiedIndex: 12,
					TTL:           9,
					Value:         `{"Owner":"XXX","Version":19}`,
				},
			},
			want: &lease{
				key: "/foo/bar",
				idx: 12,
				ttl: 9,
				meta: leaseMetadata{
					Owner:   "XXX",
					Version: 19,
				},
			},
			shouldErr: false,
		},
		// Invalid json should just bubble up the error
		{
			res: etcd.Response{
				Node: &etcd.Node{
					Key:           "/foo/bar",
					ModifiedIndex: 12,
					TTL:           9,
					Value:         `{"Owner":"XXX","Ver}`,
				},
			},
			want:      nil,
			shouldErr: true,
		},
	}

	for i, tt := range tests {
		got, err := leaseFromResult(&tt.res, nil)
		if tt.shouldErr && err == nil {
			t.Errorf("Expected err != nil got err=nil")
		} else if !tt.shouldErr && err != nil {
			t.Errorf("Expected err == nil, got err=%v", err)
		}
		if !reflect.DeepEqual(tt.want, got) {
			t.Errorf("case %d: incorrect output from leaseFromResult\nwant=%#v\ngot=%#vs", i, tt.want, got)
		}
	}
}
