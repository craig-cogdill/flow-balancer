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
	postStop        func()
	shutdownTimeout time.Duration
}

func newWorker(ctx context.Context, settings workerSettings) (*worker, error) {
	if settings.handler == nil {
		return nil, errors.New("failed to initialize worker: empty handler")
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

func (w *worker) start() error {
	if w.settings.handler == nil {
		return errors.New("failed to start worker: empty handler")
	}
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.drainRequests()
				if w.settings.postStop != nil {
					w.settings.postStop()
				}
				return
			case x, ok := <-w.requests:
				if !ok {
					return
				}
				w.settings.handler(x)
				w.settings.completed <- w
			}
		}
	}()
	return nil
}

func (w *worker) drainRequests() {
	withTimeout(func() {
		for {
			select {
			case req, ok := <-w.requests:
				if !ok {
					return
				}
				w.settings.handler(req)
				w.settings.completed <- w
			default:
				return
			}
		}
	}, w.settings.shutdownTimeout)
}
