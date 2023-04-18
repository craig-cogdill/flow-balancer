package balance

import (
	"context"

	"github.com/google/uuid"
)

type Handler func(any)

type worker struct {
	ctx      context.Context
	idx      int
	uuid     string
	requests chan any
	pending  int
	handler  Handler
	finished func()
	doneChan chan<- *worker
}

type WorkerSettings struct {
	InitialIndex int
	Handler      Handler
	QueueSize    int
}

func newWorker(
	ctx context.Context,
	initialHeapIdx int,
	fn Handler,
	doneChan chan<- *worker,
	queueSize int,
	finished func(),
) *worker {
	return &worker{
		ctx:      ctx,
		idx:      initialHeapIdx,
		uuid:     uuid.NewString(),
		requests: make(chan any, queueSize),
		pending:  0,
		handler:  fn,
		doneChan: doneChan,
		finished: finished,
	}
}

func (w *worker) Start() {
	if w.handler == nil {
		return
	}
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.finished()
				return
			case x := <-w.requests:
				w.handler(x)
				w.doneChan <- w
			}
		}
	}()
}
