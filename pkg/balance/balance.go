package balance

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/gopacket/gopacket"
)

type Settings struct {
	NumWorkers      int
	Handler         Handler
	WorkerQueueSize int
	// TODO: retry settings
	// TODO: custom heap implementation
}

type BalancerStats struct {
	processed  atomic.Int64
	failedBusy atomic.Int64
	failedType atomic.Int64
	failedHash atomic.Int64
}

func (b *BalancerStats) Processed() int64 {
	return b.processed.Load()
}

func (b *BalancerStats) FailedBusy() int64 {
	return b.failedBusy.Load()
}

func (b *BalancerStats) FailedType() int64 {
	return b.failedType.Load()
}

func (b *BalancerStats) FailedHash() int64 {
	return b.failedHash.Load()
}

type myPool struct {
	p    Pool
	lock sync.Mutex
}

type Balancer struct {
	Stats BalancerStats

	done          chan *worker
	expBackoff    backoff.BackOff
	flowTable     map[string]*worker
	workersByUuid map[string]*worker
	mp            myPool
}

func newExponentialBackOff() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Microsecond
	expBackoff.RandomizationFactor = 0.3
	expBackoff.Multiplier = 2
	return expBackoff
}

func New(settings Settings) *Balancer {
	done := make(chan *worker, settings.NumWorkers)
	pool := make(Pool, 0, settings.NumWorkers)
	workersByUuid := make(map[string]*worker, 0)
	for i := 0; i < settings.NumWorkers; i++ {
		w := newWorker(i, settings.Handler, done, settings.WorkerQueueSize)
		workersByUuid[w.uuid] = w
		heap.Push(&pool, w)
		w.Start()
	}
	heap.Init(&pool)
	return &Balancer{
		mp: myPool{
			p: pool,
		},
		done: done,
		expBackoff: backoff.WithMaxRetries(
			newExponentialBackOff(),
			5,
		),
		workersByUuid: workersByUuid,
		flowTable:     make(map[string]*worker, 0),
	}
}

func (b *Balancer) Start(in <-chan gopacket.Packet) {
	go func() {
		for {
			select {
			case worker := <-b.done:
				b.updateHeap(worker)
				b.Stats.processed.Add(1)
				// b.print()
			}
		}
	}()
	go func() {
		for {
			select {
			case req := <-in:
				pkt, ok := req.(gopacket.Packet)
				if !ok {
					fmt.Println("received a non-packet")
					b.Stats.failedType.Add(1)
					continue
				}
				b.dispatch(pkt)
				// b.printFlowTable()
			}
		}
	}()
}

func (b *Balancer) printFlowTable() {
	fmt.Println("\n\n==============================================================")
	for hash, worker := range b.flowTable {
		fmt.Printf("[%s]\t%s (%v)\n", hash, worker.uuid, worker.idx)
	}
	fmt.Println("==============================================================\n\n")
}

func (b *Balancer) dispatch(pkt gopacket.Packet) {
	hash, err := hash(pkt)
	if err != nil {
		b.Stats.failedHash.Add(1)
		fmt.Println("unable to calculate hash")
		return
	}

	waitFn := b.nextWorkerIsBusy
	w, found := b.flowTable[hash]
	if found {
		waitFn = b.getIsBusy(w)
	}

	b.expBackoff.Reset()
	err = backoff.Retry(
		waitFn,
		b.expBackoff,
	)
	if err != nil {
		fmt.Println("ERROR: everything is busy and I tried 5 times, dropping data...")
		b.Stats.failedBusy.Add(1)
		return
	}
	b.mp.lock.Lock()
	defer b.mp.lock.Unlock()
	if found {
		w.requests <- pkt
		w.pending++
		heap.Fix(&b.mp.p, w.idx)
	} else {
		w := heap.Pop(&b.mp.p).(*worker)
		w.requests <- pkt
		heap.Push(&b.mp.p, w)
		w.pending++
		b.flowTable[hash] = w
	}
}

func (b *Balancer) print() {
	sum := 0
	sumsq := 0
	for _, w := range b.mp.p {
		fmt.Printf("%d ", w.pending)
		sum += w.pending
		sumsq += w.pending * w.pending
	}
	avg := float64(sum) / float64(len(b.mp.p))
	variance := float64(sumsq)/float64(len(b.mp.p)) - avg*avg
	fmt.Printf(" %.2f %.2f\n", avg, variance)
}

func (b *Balancer) nextWorkerIsBusy() error {
	if cap(b.mp.p[0].requests) == len(b.mp.p[0].requests) {
		return errors.New("")
	}
	return nil
}

func (b *Balancer) getIsBusy(w *worker) backoff.Operation {
	return func() error {
		if cap(w.requests) == len(w.requests) {
			return errors.New("")
		}
		return nil
	}
}

func (b *Balancer) updateHeap(w *worker) {
	b.mp.lock.Lock()
	defer b.mp.lock.Unlock()
	w.pending--
	mruWorker := heap.Remove(&b.mp.p, w.idx)
	heap.Push(&b.mp.p, mruWorker)
}
