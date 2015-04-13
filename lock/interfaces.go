package lock

import "time"

// type LockAPI interface {
// 	// Should this return Locks? Or should it be purely for interacting with the API?
// 	// Acquire(key, value string, ttl time.Duration) (Lock, error)
// 	// Renew(key string, ttl time.Duration) (Lock, error)
// 	// Release(key string) error
// 	// Get(key string) (Lock, error)
// 	Acquire(key, value string, ttl time.Duration) error
// 	Renew(key string, ttl time.Duration) error
// 	Release(key string) error
// 	Get(key string) (uint64, error) // Return index or value? Or lock?
// }

type Lock interface {
	Renew(time.Duration) error
	Release() error
	Value() string
	Index() uint64
}

type LockAPI interface {
	Acquire(key, value string, ttl time.Duration) (Lock, error)
	Get(key string) (Lock, error)
}
