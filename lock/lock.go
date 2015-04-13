package lock

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
)

const defaultLockKeyPrefix = "/_etcd2/mod/lock"

type LockAPI interface {
	Acquire(key, value string, ttl time.Duration) error
}

func NewLockAPI(kAPI etcd.KeysAPI) LockAPI {
	return NewLockAPIWithPrefix(kAPI, defaultLockKeyPrefix)
}

func NewLockAPIWithPrefix(kAPI etcd.KeysAPI, prefix string) LockAPI {
	return &etcdLockAPI{
		kAPI:     kAPI,
		prefix:   prefix,
		context:  context.Background(),
		stopChan: make(chan bool),
	}
}

type etcdLockAPI struct {
	kAPI     etcd.KeysAPI
	context  context.Context
	prefix   string
	stopChan chan bool
}

func (l *etcdLockAPI) Acquire(key, value string, ttl time.Duration) (err error) {
	node, index := l.findExistingNode(key, value)
	if index == 0 {
		// Node does not exist, let's create it
		index, err = l.createNode(key, value, ttl)
		if err != nil {
			return err
		}
	}
	indexPath := path.Join(key, strconv.FormatUint(index, 10))

	// If node == nil, we do not already have the lock
	if node == nil {
		// Keep updating the ttl while we wait
		go l.ttlKeepAlive(key, value, ttl, l.stopChan)
		// wait for lock
		err = l.watch(key, index, l.stopChan)
	}

	// Return on error, deleting our lock request on the way
	if err != nil {
		if index > 0 {
			l.kAPI.Delete(l.context, indexPath, nil)
		}
		return err
	}

	// TODO(chance): why is this needed?
	// Check for connection disconnect before we write the lock index.
	select {
	case <-l.stopChan:
		err = errors.New("user interrupted")
	default:
	}

	if err == nil {
		_, err = l.kAPI.Set(l.context, indexPath, value, &etcd.SetOptions{TTL: ttl})
	} else {
		_, err = l.kAPI.Delete(l.context, indexPath, nil)
	}

	return err
}

func (l *etcdLockAPI) createNode(dir, value string, ttl time.Duration) (uint64, error) {
	resp, err := l.kAPI.CreateInOrder(l.context, dir, value, &etcd.CreateInOrderOptions{TTL: ttl})
	if err != nil {
		return 0, nil
	}
	index := NodeIndex(resp.Node)
	return index, err
}

func (l *etcdLockAPI) findExistingNode(key, value string) (*etcd.Node, uint64) {
	if len(value) > 0 {
		resp, err := l.kAPI.Get(l.context, key, &etcd.GetOptions{Sort: true})
		if err == nil {
			nodes := resp.Node.Nodes
			if node := FindNodeByValue(nodes, value); node != nil {
				index := NodeIndex(node)
				return node, index
			}
		}
	}
	return nil, 0
}

func (l *etcdLockAPI) getLockIndex(key string, index uint64) (uint64, uint64, error) {
	// Read all nodes for the lock.
	resp, err := l.kAPI.Get(l.context, key, &etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		return 0, 0, fmt.Errorf("lock watch lookup error: %s", err.Error())
	}
	nodes := resp.Node.Nodes
	prevIndex, modifiedIndex := PrevNodeIndex(nodes, index)
	return prevIndex, modifiedIndex, nil
}

// ttlKeepAlive continues to update a key's TTL until the stop channel is closed.
func (l *etcdLockAPI) ttlKeepAlive(key, value string, ttl time.Duration, stopChan chan bool) {
	for {
		select {
		case <-time.After(ttl / 2):
			fmt.Println("Keep alive!")
			l.kAPI.Set(l.context, key, value, &etcd.SetOptions{TTL: ttl, PrevExist: etcd.PrevExist})
		case <-stopChan:
			return
		}
	}
}

// watch continuously waits for a given lock index to be acquired or until lock fails.
// Returns a boolean indicating success.
func (l *etcdLockAPI) watch(key string, index uint64, stopChan chan bool) error {
	prevIndex, modifiedIndex, err := l.getLockIndex(key, index)
	// If there is no previous index then we have the lock.
	if prevIndex == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(l.context)

	// If the caller closes the stopChan cancel the request
	go func() {
		<-stopChan
		cancel()
	}()

	// Wait from the last modification of the node.
	waitIndex := modifiedIndex + 1
	opts := &etcd.WatcherOptions{
		AfterIndex: waitIndex,
	}
	watcher := l.kAPI.Watcher(path.Join(key, strconv.FormatUint(prevIndex, 10)), opts)
	_, err = watcher.Next(ctx)
	return err
}
