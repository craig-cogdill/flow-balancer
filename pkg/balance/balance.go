package balance

import "fmt"

type balancer struct {
	settings Settings
	pool     Pool
}

type Settings struct {
	NumThreads int
}

var Default = Settings{
	NumThreads: 1,
}

func New(settings Settings) *balancer {
	return &balancer{
		settings: settings,
		pool:     NewPool(settings.NumThreads),
	}
}

func (b *balancer) Start() {
	defer fmt.Println("started")
	for _, worker := range b.pool {
		worker.Start()
	}
}
