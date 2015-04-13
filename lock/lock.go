package lock

import (
	"fmt"
	"time"
)

type lockNode struct {
	key   string
	value string
	index uint64
	lAPI  *etcdLockAPI
}

func (l *lockNode) Renew(time.Duration) error {
	return nil
}

func (l *lockNode) Release() error {
	return nil
}

func (l *lockNode) Value() string {
	return ""
}

func (l *lockNode) Index() uint64 {
	return 0
}

func (l *lockNode) String() string {
	return fmt.Sprintf("Key: %s, Value: %s, Index: %d", l.key, l.value, l.index)
}
