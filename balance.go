package balance

import (
	"container/heap"
	"context"
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
	WorkerQueueSize int
	ShutdownTimeout time.Duration
	Backoff         backoff.BackOff
}

var Default = Settings{
	NumWorkers:      8,
	WorkerQueueSize: 10000,
	ShutdownTimeout: 30 * time.Second,
	Backoff: backoff.WithMaxRetries(
		newExponentialBackOff(),
		10,
	),
}

type Stats struct {
	processed          atomic.Uint64
	failedBusy         atomic.Uint64
	failedType         atomic.Uint64
	failedHash         atomic.Uint64
	processedPerWorker map[string]uint64
}

func (b *Stats) Processed() uint64 {
	return b.processed.Load()
}

func (b *Stats) Failed() uint64 {
	return b.FailedBusy() + b.FailedHash() + b.FailedType()
}

func (b *Stats) FailedBusy() uint64 {
	return b.failedBusy.Load()
}

func (b *Stats) FailedType() uint64 {
	return b.failedType.Load()
}

func (b *Stats) FailedHash() uint64 {
	return b.failedHash.Load()
}

func (b *Stats) ProcessedPerWorker() map[string]uint64 {
	return b.processedPerWorker
}

type pool struct {
	lock sync.RWMutex
	heap Heap
	wg   sync.WaitGroup
}

func newPool(numWorkers int) *pool {
	p := &pool{
		heap: make(Heap, 0, numWorkers),
	}
	p.wg.Add(numWorkers)
	return p
}

func newExponentialBackOff() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Microsecond
	expBackoff.RandomizationFactor = 0.1
	expBackoff.Multiplier = 1.5
	return expBackoff
}

type Balancer struct {
	Stats Stats

	ctx            context.Context
	cancelBalancer context.CancelFunc
	wg             sync.WaitGroup
	cancelWorkers  context.CancelFunc
	completed      chan *worker
	blocker        *blocker
	flowTable      *flowLookup
	p              *pool
}

func New(processFn Handler, settings Settings) *Balancer {
	ctx, cancelBalancer := context.WithCancel(context.Background())
	completed := make(chan *worker, settings.WorkerQueueSize*2)

	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	p := newPool(settings.NumWorkers)
	for i := 0; i < settings.NumWorkers; i++ {
		w, err := newWorker(
			workerCtx,
			workerSettings{
				initialIndex:    i,
				handler:         processFn,
				completed:       completed,
				queueSize:       settings.WorkerQueueSize,
				postStop:        p.wg.Done,
				shutdownTimeout: settings.ShutdownTimeout,
			},
		)
		if err != nil {
			cancelBalancer()
			cancelWorkers()
			return nil
		}
		heap.Push(&p.heap, w)
		w.start()
	}
	heap.Init(&p.heap)

	return &Balancer{
		Stats: Stats{
			processedPerWorker: make(map[string]uint64),
		},
		ctx:            ctx,
		cancelBalancer: cancelBalancer,
		cancelWorkers:  cancelWorkers,
		p:              p,
		completed:      completed,
		blocker: NewBlocker(
			backoff.WithMaxRetries(
				newExponentialBackOff(),
				10,
			),
		),
		flowTable: newFlowLookup(),
	}
}

func (b *Balancer) Start(in <-chan gopacket.Packet) {
	b.wg.Add(2)
	go b.handleCompleted()
	go b.handleDispatch(in)
}

func (b *Balancer) Stop() {
	defer close(b.completed)

	// It is important that the dispatch and completed goroutines exit before
	// the worker goroutines. This ensures that no data is lost due to race
	// conditions between the final packets being dispatched and the worker
	// goroutines draining their request channels.
	b.cancelBalancer()
	b.wg.Wait()

	b.cancelWorkers()
	b.p.wg.Wait()

	withTimeout(func() {
		numCompleted := len(b.completed)
		if numCompleted > 0 {
			for i := 0; i < numCompleted; i++ {
				worker, ok := <-b.completed
				if !ok {
					return
				}
				b.Stats.processed.Add(1)
				b.Stats.processedPerWorker[worker.uuid]++
			}
		}
	}, 5*time.Second)
}

func (b *Balancer) Delete(hash string) {
	b.flowTable.delete(hash)
}

func (b *Balancer) handleCompleted() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case worker, ok := <-b.completed:
			if !ok {
				return
			}
			b.updateHeap(worker)
			b.Stats.processed.Add(1)
			// this is a race condition
			b.Stats.processedPerWorker[worker.uuid]++
			// b.print()
		}
	}
}

func (b *Balancer) handleDispatch(in <-chan gopacket.Packet) {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case pkt, ok := <-in:
			if !ok {
				return
			}
			b.dispatch(pkt)
		}
	}
}

func (b *Balancer) dispatch(pkt gopacket.Packet) {
	hash, err := hash(pkt)
	if err != nil {
		b.Stats.failedHash.Add(1)
		return
	}

	b.p.lock.RLock()
	selectedWorker := b.p.heap[0]
	b.p.lock.RUnlock()

	existingWorker, found := b.flowTable.load(hash)
	if found {
		selectedWorker = existingWorker
	}

	err = b.blocker.Wait(b.queueIsNotFull(selectedWorker))
	if err != nil {
		b.Stats.failedBusy.Add(1)
		return
	}

	b.p.lock.Lock()
	defer b.p.lock.Unlock()
	if found {
		selectedWorker.requests <- pkt
		selectedWorker.pending++
		heap.Fix(&b.p.heap, selectedWorker.idx)
	} else {
		w := heap.Pop(&b.p.heap).(*worker)
		w.requests <- pkt
		heap.Push(&b.p.heap, w)
		w.pending++
		b.flowTable.store(hash, w)
	}
}

func (b *Balancer) updateHeap(w *worker) {
	b.p.lock.Lock()
	defer b.p.lock.Unlock()
	w.pending--
	mruWorker := heap.Remove(&b.p.heap, w.idx)
	heap.Push(&b.p.heap, mruWorker)
}

func (b *Balancer) queueIsNotFull(w *worker) func() error {
	return func() error {
		if cap(w.requests) == len(w.requests) {
			return errors.New("queue is full")
		}
		return nil
	}
}

func (b *Balancer) print() {
	sum := 0
	sumsq := 0
	for _, w := range b.p.heap {
		fmt.Printf("%d ", w.pending)
		sum += w.pending
		sumsq += w.pending * w.pending
	}
	avg := float64(sum) / float64(len(b.p.heap))
	variance := float64(sumsq)/float64(len(b.p.heap)) - avg*avg
	fmt.Printf(" %.2f %.2f\n", avg, variance)
}
