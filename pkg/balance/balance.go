package balance

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
)

type Settings struct {
	NumWorkers int
	Handler    Handler
}

type Balancer struct {
	pool       Pool
	poolLock   sync.Mutex
	done       chan *worker
	i          int
	expBackoff backoff.BackOff
}

func newExponentialBackOff() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 100 * time.Microsecond
	expBackoff.RandomizationFactor = 0.01
	expBackoff.Multiplier = 1
	return expBackoff
}

func New(settings Settings) *Balancer {
	done := make(chan *worker, settings.NumWorkers)
	pool := make(Pool, 0, settings.NumWorkers)
	for i := 0; i < settings.NumWorkers; i++ {
		w := newWorker(i, settings.Handler, done)
		heap.Push(&pool, w)
		w.Start()
	}
	heap.Init(&pool)
	return &Balancer{
		pool: pool,
		done: done,
		expBackoff: backoff.WithMaxRetries(
			newExponentialBackOff(),
			5,
		),
	}
}

func (b *Balancer) Start(in <-chan any) {
	go func() {
		for {
			select {
			case worker := <-b.done:
				b.updateHeap(worker)
				// b.print()
			}
		}
	}()
	go func() {
		for {
			select {
			case req := <-in:
				b.safeDispatch(req)
			}
		}
	}()
}

func (b *Balancer) print() {
	sum := 0
	sumsq := 0
	for _, w := range b.pool {
		fmt.Printf("%d ", w.pending)
		sum += w.pending
		sumsq += w.pending * w.pending
	}
	avg := float64(sum) / float64(len(b.pool))
	variance := float64(sumsq)/float64(len(b.pool)) - avg*avg
	fmt.Printf(" %.2f %.2f\n", avg, variance)
}

func (b *Balancer) nextWorkerIsBusy() error {
	if cap(b.pool[0].requests) == len(b.pool[0].requests) {
		return errors.New("")
	}
	return nil
}

func (b *Balancer) safeDispatch(data any) {
	// TODO: backoff.WithContext
	b.expBackoff.Reset()
	err := backoff.Retry(
		b.nextWorkerIsBusy,
		b.expBackoff,
	)
	if err != nil {
		fmt.Println("ERROR: everything is busy and I tried 5 times, dropping data...")
		return
	}
	b.dispatch(data)
}

func (b *Balancer) dispatch(data any) {
	b.poolLock.Lock()
	defer b.poolLock.Unlock()
	w := heap.Pop(&b.pool).(*worker)
	w.requests <- data
	w.pending++
	heap.Push(&b.pool, w)
}

func (b *Balancer) updateHeap(w *worker) {
	b.poolLock.Lock()
	defer b.poolLock.Unlock()
	w.pending--
	mruWorker := heap.Remove(&b.pool, w.i)
	heap.Push(&b.pool, mruWorker)
}
