package btree

import "sync"

type MultiLock struct {
	owned sync.Map
	mu    sync.Mutex
}

func (l *MultiLock) Lock(offset int) {
	lock := l.acquireLock(offset)
	lock.Lock()
}

func (l *MultiLock) Unlock(offset int) {
	lock := l.releaseLock(offset)
	lock.Unlock()
}

func (l *MultiLock) RLock(offset int) {
	lock := l.acquireLock(offset)
	lock.RLock()
}

func (l *MultiLock) RUnlock(offset int) {
	lock := l.releaseLock(offset)
	lock.RUnlock()
}

func (l *MultiLock) acquireLock(offset int) *sync.RWMutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	kl := l.getLock(offset)
	kl.count++

	return kl.lock
}

func (l *MultiLock) releaseLock(offset int) *sync.RWMutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	kl := l.getLock(offset)
	kl.count--

	if kl.count <= 0 {
		l.owned.Delete(offset)
	}

	return kl.lock
}

func (l *MultiLock) getLock(offset int) *keyLock {
	lock, _ := l.owned.LoadOrStore(offset, &sync.RWMutex{})

	return &keyLock{
		lock: lock.(*sync.RWMutex),
	}
}

type keyLock struct {
	count int
	lock  *sync.RWMutex
}
