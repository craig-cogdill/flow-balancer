package balance

import (
	"fmt"
	"sync"
)

type flowTable struct {
	lock   sync.RWMutex
	lookup map[string]*worker
}

func NewFlowTable() *flowTable {
	return &flowTable{
		lookup: make(map[string]*worker),
	}
}

func (f *flowTable) Set(hash string, w *worker) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.lookup[hash] = w
}

func (f *flowTable) Get(hash string) (w *worker, found bool) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	w, found = f.lookup[hash]
	return
}

func (f *flowTable) Clear(hash string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	delete(f.lookup, hash)
}

func (f *flowTable) Print() {
	f.lock.RLock()
	defer f.lock.RUnlock()
	fmt.Println("\n\n==============================================================")
	for hash, worker := range f.lookup {
		fmt.Printf("[%s]\t%s (%v)\n", hash, worker.uuid, worker.idx)
	}
	fmt.Println("==============================================================\n\n")
}
