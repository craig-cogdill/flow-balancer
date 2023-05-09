package balance

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
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
		NewExponentialBackOff(),
		10,
	),
}

type BalancerStats struct {
	processed          int64
	failedBusy         int64
	failedType         int64
	failedHash         int64
	processedPerWorker map[string]int64
}

func NewStats() BalancerStats {
	return BalancerStats{
		processed:          0,
		failedBusy:         0,
		failedType:         0,
		failedHash:         0,
		processedPerWorker: make(map[string]int64),
	}
}

func (b *BalancerStats) Processed() int64 {
	return b.processed
}

func (b *BalancerStats) FailedBusy() int64 {
	return b.failedBusy
}

func (b *BalancerStats) FailedType() int64 {
	return b.failedType
}

func (b *BalancerStats) FailedHash() int64 {
	return b.failedHash
}

func (b *BalancerStats) ProcessedPerWorker() map[string]int64 {
	return b.processedPerWorker
}

type pool struct {
	lock sync.Mutex
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

type Balancer struct {
	Stats BalancerStats

	ctx       context.Context
	cancel    context.CancelFunc
	completed chan *worker
	blocker   *blocker
	flowTable *flowLookup
	p         *pool
}

func NewExponentialBackOff() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Microsecond
	expBackoff.RandomizationFactor = 0.3
	expBackoff.Multiplier = 2
	return expBackoff
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
		w.Start()
	}
	heap.Init(&p.heap)

	return &Balancer{
		Stats:     NewStats(),
		ctx:       ctx,
		cancel:    cancel,
		p:         p,
		completed: completed,
		blocker: NewBlocker(
			backoff.WithMaxRetries(
				NewExponentialBackOff(),
				10,
			),
		),
		flowTable: NewFlowLookup(),
	}
}

func (b *Balancer) Start(in <-chan gopacket.Packet) {
	go b.handleCompleted()
	go b.handleDispatch(in)
}

func (b *Balancer) Stop() {
	b.cancel()
	b.p.wg.Wait()
	b.blocker.Wait(func() error {
		if len(b.completed) > 0 {
			return errors.New("")
		}
		return nil
	})
	close(b.completed)
}

func (b *Balancer) Delete(hash string) {
	b.flowTable.Delete(hash)
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
			b.Stats.processed++
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
		b.Stats.failedHash++
		return
	}

	waitFn := b.nextWorkerIsBusy
	w, found := b.flowTable.Load(hash)
	if found {
		waitFn = b.getIsQueueFull(w)
	}

	err = b.blocker.Wait(waitFn)
	if err != nil {
		b.Stats.failedBusy++
		return
	}

	b.p.lock.Lock()
	defer b.p.lock.Unlock()
	if found {
		w.requests <- pkt
		w.pending++
		heap.Fix(&b.p.heap, w.idx)
	} else {
		w := heap.Pop(&b.p.heap).(*worker)
		w.requests <- pkt
		heap.Push(&b.p.heap, w)
		w.pending++
		b.flowTable.Store(hash, w)
	}
}

func (b *Balancer) nextWorkerIsBusy() error {
	return b.getIsQueueFull(b.p.heap[0])()
}

func (b *Balancer) updateHeap(w *worker) {
	b.p.lock.Lock()
	defer b.p.lock.Unlock()
	w.pending--
	mruWorker := heap.Remove(&b.p.heap, w.idx)
	heap.Push(&b.p.heap, mruWorker)
}

func (b *Balancer) getIsQueueFull(w *worker) backoff.Operation {
	return func() error {
		if cap(w.requests) == len(w.requests) {
			return errors.New("worker queue is full")
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
