package balance

type Heap []*worker

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	return h[i].pending < h[j].pending
}

func (h *Heap) Swap(i, j int) {
	a := *h
	a[i], a[j] = a[j], a[i]
	a[i].idx = i
	a[j].idx = j
}

func (h *Heap) Push(x interface{}) {
	a := *h
	n := len(a)
	a = a[0 : n+1]
	w := x.(*worker)
	a[n] = w
	w.idx = n
	*h = a
}

func (h *Heap) Pop() interface{} {
	a := *h
	*h = a[0 : len(a)-1]
	w := a[len(a)-1]
	w.idx = -1 // for safety
	return w
}
