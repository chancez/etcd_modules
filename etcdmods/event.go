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
	"time"

	"github.com/coreos/pkg/log"
	"github.com/ecnahc515/etcd_modules/etcd"
	"github.com/jonboulle/clockwork"
)

type Event string

type eventStream struct {
	etcd    etcd.Client
	prefix  string
	handler ResultHandler
}

func NewEventStream(client etcd.Client, prefix string, handler ResultHandler) EventStream {
	return &eventStream{etcd: client, prefix: prefix, handler: handler}
}

// Next returns a channel which will emit an Event as soon as one of interest occurs
func (es *eventStream) Next(stop chan struct{}) chan Event {
	evchan := make(chan Event)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			res := watch(es.etcd, es.prefix, stop)
			if res == nil || res.Node == nil {
				return
			}
			if ev, ok := es.handler.Handle(res, es.prefix); ok {
				evchan <- ev
				return
			}
		}

	}()

	return evchan
}

func watch(client etcd.Client, key string, stop chan struct{}) (res *etcd.Result) {
	for res == nil {
		select {
		case <-stop:
			log.Debugf("Gracefully closing etcd watch loop: key=%s", key)
			return
		default:
			req := &etcd.Watch{
				Key:       key,
				WaitIndex: 0,
				Recursive: true,
			}

			log.Debugf("Creating etcd watcher: %v", req)

			var err error
			res, err = client.Wait(req, stop)
			if err != nil {
				log.Errorf("etcd watcher %v returned error: %v", req, err)
			}
		}

		// Let's not slam the etcd server in the event that we know
		// an unexpected error occurred.
		time.Sleep(time.Second)
	}

	return
}

// ResultHandler generates an Event from a result returned by etcd.
type ResultHandler interface {
	Handle(*etcd.Result, string) (Event, bool)
}

type ResultHandlerFunc func(*etcd.Result, string) (Event, bool)

func (f ResultHandlerFunc) Handle(res *etcd.Result, prefix string) (Event, bool) {
	return f(res, prefix)
}

type PeriodicReconciler interface {
	Run(stop chan bool)
}

// NewPeriodicReconciler creates a PeriodicReconciler that will run recFunc at least every
// ival, or in response to anything emitted from EventStream.Next()
func NewPeriodicReconciler(interval time.Duration, recFunc func(), eStream EventStream) PeriodicReconciler {
	return &reconciler{
		ival:    interval,
		rFunc:   recFunc,
		eStream: eStream,
		clock:   clockwork.NewRealClock(),
	}
}

type reconciler struct {
	ival    time.Duration
	rFunc   func()
	eStream EventStream
	clock   clockwork.Clock
}

func (r *reconciler) Run(stop chan bool) {
	trigger := make(chan struct{})
	go func() {
		abort := make(chan struct{})
		for {
			select {
			case <-stop:
				close(abort)
				return
			case <-r.eStream.Next(abort):
				trigger <- struct{}{}
			}
		}
	}()

	ticker := r.clock.After(r.ival)

	// When starting up, reconcile once immediately
	log.Debug("Initial reconciliation commencing")
	r.rFunc()

	for {
		select {
		case <-stop:
			log.Debug("Reconciler exiting due to stop signal")
			return
		case <-ticker:
			ticker = r.clock.After(r.ival)
			log.Debug("Reconciler tick")
			r.rFunc()
		case <-trigger:
			ticker = r.clock.After(r.ival)
			log.Debug("Reconciler triggered")
			r.rFunc()
		}
	}

}
