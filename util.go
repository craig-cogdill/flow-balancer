package balance

import "github.com/cenkalti/backoff"

type blocker struct {
	b backoff.BackOff
}

func NewBlocker(b backoff.BackOff) *blocker {
	return &blocker{
		b: b,
	}
}

func (b *blocker) Wait(fn func() error) error {
	b.b.Reset()
	return backoff.Retry(
		fn,
		b.b,
	)
}
