package balance

import (
	"fmt"
	"sync"
)

type flowLookup struct {
	lock   sync.RWMutex
	lookup map[string]*worker
}

func newFlowLookup() *flowLookup {
	return &flowLookup{
		lookup: make(map[string]*worker),
	}
}

func (f *flowLookup) store(hash string, w *worker) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.lookup[hash] = w
}

func (f *flowLookup) load(hash string) (w *worker, found bool) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	w, found = f.lookup[hash]
	return
}

func (f *flowLookup) delete(hash string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	delete(f.lookup, hash)
}

func (f *flowLookup) print() {
	f.lock.RLock()
	defer f.lock.RUnlock()
	fmt.Println("\n\n==============================================================")
	for hash, worker := range f.lookup {
		fmt.Printf("[%s]\t%s (%v)\n", hash, worker.uuid, worker.idx)
	}
	fmt.Printf("==============================================================\n\n")
}
