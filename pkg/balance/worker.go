package balance

import "github.com/google/uuid"

type Handler func(any)

type worker struct {
	idx      int
	uuid     string
	requests chan any
	pending  int
	handler  Handler
	doneChan chan<- *worker
}

func newWorker(initialHeapIdx int, fn Handler, doneChan chan<- *worker, queueSize int) *worker {
	return &worker{
		idx:      initialHeapIdx,
		uuid:     uuid.NewString(),
		requests: make(chan any, queueSize),
		pending:  0,
		handler:  fn,
		doneChan: doneChan,
	}
}

func (w *worker) Start() {
	go func() {
		for {
			select {
			case x := <-w.requests:
				w.handler(x)
				w.doneChan <- w
			}
		}
	}()
}
