package balance

type Handler func(any)

type worker struct {
	i        int
	requests chan any
	pending  int
	handler  Handler
	doneChan chan<- *worker
}

func newWorker(initialHeapIdx int, fn Handler, doneChan chan<- *worker) *worker {
	return &worker{
		i:        initialHeapIdx,
		requests: make(chan any, 100000),
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
