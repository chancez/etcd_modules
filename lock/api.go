package lock

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
)

const defaultLockKeyPrefix = "/_etcd2/mod/lock"

type Config struct {
	Context context.Context
	Logger  *log.Logger
	Prefix  string
}

func NewEtcdLockAPI(kAPI etcd.KeysAPI, cfg *Config) *etcdLockAPI {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.Prefix == "" {
		cfg.Prefix = defaultLockKeyPrefix
	}
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "lock: ", log.Lshortfile)
	}
	return &etcdLockAPI{
		Config: cfg,
		kAPI:   kAPI,
	}
}

type etcdLockAPI struct {
	*Config
	kAPI     etcd.KeysAPI
	stopChan chan bool
}

// func (l *etcdLockAPI) Stop() {
// }

func (l *etcdLockAPI) Acquire(key, value string, ttl time.Duration) (lock Lock, err error) {
	keyPath := path.Join(l.Prefix, key)
	// Create the lock
	index, err := l.createNode(keyPath, value, ttl)
	if err != nil {
		return nil, err
	}

	// Index path is /keyPath/XXXXX value where XXXXX is an integer generated
	// by the server and is atomically increasing
	indexPath := path.Join(keyPath, strconv.FormatUint(index, 10))
	lock = &lockNode{key: keyPath, value: value, index: index, lAPI: l}

	// Query the list of lock nodes
	resp, err := l.kAPI.Get(l.Context, keyPath, &etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		lock = nil
		_, err = l.kAPI.Delete(l.Context, indexPath, nil)
		return
	}

	nodes := resp.Node.Nodes
	if len(nodes) > 0 {
		smallestNode := nodes[0]
		// if our node is the smallest, we have the lock, and can return
		if NodeIndex(smallestNode) == index {
			// We have the lock, return
			return
		}
	}

	// heartbeat the ttl of our lock so it doesn't expire while we wait
	go l.ttlKeepAlive(keyPath, value, ttl)
	// wait on our lock
	err = l.watch(keyPath, index)

	// Return on error, deleting our lock request on the way
	if err != nil {
		if index > 0 {
			l.kAPI.Delete(l.Context, indexPath, nil)
		}
		return nil, err
	}

	// at this point we should have the lock, as all other locks ahead of us
	// have either explicitly been released (deleted), or they've expired via TTL

	// Check for connection disconnect before we write the lock index.
	select {
	case <-l.Context.Done():
		err = errors.New("user interrupted")
	default:
	}

	// Update our TTL one last time if lock was aquired, otherwise delete
	if err != nil {
		l.kAPI.Delete(l.Context, indexPath, nil)
		return nil, err
	}

	l.kAPI.Set(l.Context, indexPath, value, &etcd.SetOptions{TTL: ttl})
	l.Logger.Printf("Acquired lock %s", lock)

	return
}

func (l *etcdLockAPI) Get(key string) (Lock, error) {
	return nil, nil
}

func (l *etcdLockAPI) createNode(dir, value string, ttl time.Duration) (uint64, error) {
	if len(value) == 0 {
		value = "-"
	}
	resp, err := l.kAPI.CreateInOrder(l.Context, dir, value, &etcd.CreateInOrderOptions{TTL: ttl})
	if err != nil {
		return 0, nil
	}
	index := NodeIndex(resp.Node)
	return index, err
}

func (l *etcdLockAPI) getLockIndex(key string, index uint64) (uint64, uint64, error) {
	// Read all nodes for the lock.
	resp, err := l.kAPI.Get(l.Context, key, &etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		return 0, 0, fmt.Errorf("error getting list of lock nodes: %s", err.Error())
	}
	nodes := resp.Node.Nodes
	prevIndex, modifiedIndex := PrevNodeIndex(nodes, index)
	return prevIndex, modifiedIndex, nil
}

// ttlKeepAlive continues to update a key's TTL until the stop channel is closed.
func (l *etcdLockAPI) ttlKeepAlive(key, value string, ttl time.Duration) {
	for {
		select {
		case <-time.After(ttl / 2):
			l.kAPI.Set(l.Context, key, value, &etcd.SetOptions{TTL: ttl, PrevExist: etcd.PrevExist})
		case <-l.Context.Done():
			l.Logger.Println("No longer keeping lock alive")
			return
		}
	}
}

// watch continuously waits for a given lock index to be acquired or until lock fails.
// Returns a boolean indicating success.
func (l *etcdLockAPI) watch(key string, index uint64) error {
	prevIndex, modifiedIndex, err := l.getLockIndex(key, index)
	if err != nil {
		return err
	}
	// If there is no previous index then we have the lock.
	if prevIndex == 0 {
		return nil
	}

	// Wait from the last modification of the node.
	waitIndex := modifiedIndex + 1
	opts := &etcd.WatcherOptions{
		AfterIndex: waitIndex,
	}
	// AfterIndex gets updated every time a new event comes in by the kAPI
	watcher := l.kAPI.Watcher(path.Join(key, strconv.FormatUint(prevIndex, 10)), opts)

	for {
		prevIndex, modifiedIndex, err := l.getLockIndex(key, index)
		if err != nil {
			return err
		}
		// If there is no previous index then we have the lock.
		if prevIndex == 0 {
			return nil
		}
		l.Logger.Printf("prevIndex %d, index: %d, modifiedIndex: %d\n", prevIndex, index, modifiedIndex)
		// Wait for the next event
		_, err = watcher.Next(l.Context)
		if err != nil {
			return err
		}
	}
	return nil
}
