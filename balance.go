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

	ctx       context.Context
	cancel    context.CancelFunc
	completed chan *worker
	blocker   *blocker
	flowTable *flowLookup
	p         *pool
}

func New(processFn Handler, settings Settings) *Balancer {
	ctx, cancel := context.WithCancel(context.Background())
	completed := make(chan *worker, settings.WorkerQueueSize*2)

	p := newPool(settings.NumWorkers)
	for i := 0; i < settings.NumWorkers; i++ {
		w, err := newWorker(
			ctx,
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
			cancel()
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
		ctx:       ctx,
		cancel:    cancel,
		p:         p,
		completed: completed,
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
	go b.handleCompleted()
	go b.handleDispatch(in)
}

func (b *Balancer) Stop() {
	defer close(b.completed)
	b.cancel()
	b.p.wg.Wait()
	b.blocker.Wait(func() error {
		if len(b.completed) > 0 {
			return errors.New("completed channel not empty")
		}
		return nil
	})
}

func (b *Balancer) Delete(hash string) {
	b.flowTable.delete(hash)
}

func (b *Balancer) handleCompleted() {
	for {
		select {
		// The goroutine should continue to process completed
		// packets until the completed channel is closed by
		// a call to Balancer.Stop()
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

	selectedWorker := b.p.heap[0]
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
