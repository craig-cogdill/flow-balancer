package balance

import (
	"fmt"
)

type worker struct {
	id           int
	pendingTasks int
}

func newWorker(id int) *worker {
	return &worker{
		id:           id,
		pendingTasks: 0,
	}
}

func (w *worker) Start() {
	fmt.Println("Worker: started")
}
