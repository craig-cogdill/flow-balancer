package balance

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Handler func(any)

type worker struct {
	ctx      context.Context
	uuid     string
	idx      int
	requests chan any
	pending  int
	settings workerSettings
}

type workerSettings struct {
	initialIndex    int
	handler         Handler
	completed       chan<- *worker
	queueSize       int
	finished        func()
	shutdownTimeout time.Duration
}

func newWorker(ctx context.Context, settings workerSettings) (*worker, error) {
	if settings.handler == nil {
		return nil, errors.New("Failed to initialize worker: empty handler")
	}
	if settings.shutdownTimeout == 0 {
		settings.shutdownTimeout = 30 * time.Second
	}
	return &worker{
		ctx:      ctx,
		uuid:     uuid.NewString(),
		idx:      settings.initialIndex,
		requests: make(chan any, settings.queueSize),
		pending:  0,
		settings: settings,
	}, nil
}

func (w *worker) Start() error {
	if w.settings.handler == nil {
		return errors.New("Failed to start worker: empty handler")
	}
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.drainRequests()
				if w.settings.finished != nil {
					w.settings.finished()
				}
				return
			case x := <-w.requests:
				w.settings.handler(x)
				w.settings.completed <- w
			}
		}
	}()
	return nil
}

func (w *worker) drainRequests() {
	finished := make(chan struct{})
	go func() {
		for {
			select {
			case req, ok := <-w.requests:
				if !ok {
					finished <- struct{}{}
					return
				}
				w.settings.handler(req)
				w.settings.completed <- w
			default:
				finished <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-finished:
		return
	case <-time.After(w.settings.shutdownTimeout):
		return
	}
}
