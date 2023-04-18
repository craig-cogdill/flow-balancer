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
	Handler         Handler
	WorkerQueueSize int
	// TODO: retry settings
	// TODO: custom heap implementation
}

type BalancerStats struct {
	processed  int64
	failedBusy int64
	failedType int64
	failedHash int64
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

type myPool struct {
	lock sync.Mutex
	p    Pool
	wg   sync.WaitGroup
}

func NewMyPool(numWorkers int) *myPool {
	mp := &myPool{
		p: make(Pool, 0, numWorkers),
	}
	mp.wg.Add(numWorkers)
	return mp
}

type Balancer struct {
	Stats BalancerStats

	ctx       context.Context
	cancel    context.CancelFunc
	done      chan *worker
	blocker   *blocker
	flowTable *flowTable
	mp        *myPool
}

func NewExponentialBackOff() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Microsecond
	expBackoff.RandomizationFactor = 0.3
	expBackoff.Multiplier = 2
	return expBackoff
}

func New(settings Settings) *Balancer {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *worker, settings.WorkerQueueSize)
	mp := NewMyPool(settings.NumWorkers)
	for i := 0; i < settings.NumWorkers; i++ {
		w := newWorker(
			ctx,
			i,
			settings.Handler,
			done,
			settings.WorkerQueueSize,
			func() { mp.wg.Done() },
		)
		heap.Push(&mp.p, w)
		w.Start()
	}
	heap.Init(&mp.p)
	return &Balancer{
		ctx:    ctx,
		cancel: cancel,
		mp:     mp,
		done:   done,
		blocker: NewBlocker(
			backoff.WithMaxRetries(
				NewExponentialBackOff(),
				5,
			),
		),
		flowTable: NewFlowTable(),
	}
}

func (b *Balancer) Start(in <-chan gopacket.Packet) {
	go b.handleDone()

	go func() {
		for {
			select {
			case req := <-in:
				pkt, ok := req.(gopacket.Packet)
				if !ok {
					b.Stats.failedType++
					continue
				}
				b.dispatch(pkt)
				// b.flowTable.Print()
			}
		}
	}()
}

func (b *Balancer) Stop() {
	b.cancel()
	b.mp.wg.Wait()
}

func (b *Balancer) Clear(hash string) {
	b.flowTable.Clear(hash)
}

func (b *Balancer) handleDone() {
	for {
		select {
		case worker := <-b.done:
			b.updateHeap(worker)
			b.Stats.processed++
			// b.print()
		}
	}
}

func (b *Balancer) dispatch(pkt gopacket.Packet) {
	hash, err := hash(pkt)
	if err != nil {
		b.Stats.failedHash++
		fmt.Println("unable to calculate hash")
		return
	}

	waitFn := b.nextWorkerIsBusy
	w, found := b.flowTable.Get(hash)
	if found {
		waitFn = b.getIsBusy(w)
	}

	err = b.blocker.Wait(waitFn)
	if err != nil {
		fmt.Println("ERROR: everything is busy and I tried 5 times, dropping data...")
		b.Stats.failedBusy++
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
		b.flowTable.Set(hash, w)
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
