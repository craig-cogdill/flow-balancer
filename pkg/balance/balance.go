package balance

import (
	"container/heap"
	"fmt"
)

type Settings struct {
	NumWorkers int
	Handler    Handler
}

type Balancer struct {
	pool Pool
	done chan *worker
	i    int
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
	}
}

func (b *Balancer) Start(in <-chan any) {
	go func() {
		for {
			select {
			case req := <-in:
				b.dispatch(req)
			case w := <-b.done:
				b.updateHeap(w)
				b.print()
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

func (b *Balancer) dispatch(data any) {
	w := heap.Pop(&b.pool).(*worker)
	w.requests <- data
	w.pending++
	heap.Push(&b.pool, w)
}

func (b *Balancer) updateHeap(w *worker) {
	w.pending--
	mruWorker := heap.Remove(&b.pool, w.i)
	heap.Push(&b.pool, mruWorker)
}
