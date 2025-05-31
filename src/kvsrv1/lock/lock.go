package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	// Name of lock file
	lockKey string
	// Owner of the lock
	lockIdentifier string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	lk.lockIdentifier = kvtest.RandValue(8)

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// Everytime we acquire lock, we will make sure nobody has the lock or the lock is release by the current owner
	// We call put to acquire the lock and release the lock so that odd value for lockKey means lock is qcquired and
	// even number means that lock is released
	for {
		value, version, err := lk.ck.Get(lk.lockKey)

		// With the same lock identifier as the value. This means the the lock has just been acquired by the same call
		if err != rpc.ErrNoKey && value != lk.lockIdentifier && value != "" {
			// Someone has acquired the lock or the lock is acquired by current owner already
			// fmt.Println("Here")
			continue
		}

		putErr := lk.ck.Put(lk.lockKey, lk.lockIdentifier, version)
		if putErr == rpc.OK {
			break
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		// fmt.Println(value, version, err, lk.lockIdentifier)
		if err != rpc.OK {
			continue
		}

		if value != lk.lockIdentifier {
			break
		}

		putErr := lk.ck.Put(lk.lockKey, "", version)
		if putErr == rpc.OK {
			break
		}
	}
}
