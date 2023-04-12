package balance

type Pool []*worker

func NewPool(numWorkers int) Pool {
	pool := []*worker{}
	for i := 0; i < numWorkers; i++ {
		pool = append(pool, newWorker(i))
	}
	return pool
}

func (p Pool) Len() int { return len(p) }

func (p Pool) Less(i, j int) bool {
	return p[i].pendingTasks < p[j].pendingTasks
}

func (p *Pool) Swap(i, j int) {
	a := *p
	a[i], a[j] = a[j], a[i]
	a[i].id = i
	a[j].id = j
}

func (p *Pool) Push(x interface{}) {
	a := *p
	n := len(a)
	a = a[0 : n+1]
	w := x.(*worker)
	a[n] = w
	w.id = n
	*p = a
}

func (p *Pool) Pop() interface{} {
	a := *p
	*p = a[0 : len(a)-1]
	w := a[len(a)-1]
	w.id = -1 // for safety
	return w
}
