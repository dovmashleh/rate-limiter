package rate_limiter

import (
	"context"
	"sync/atomic"
	"time"
)

type limit[TKey comparable] struct {
	amount atomic.Int32
	next   atomic.Int64
}

func newLimit[TKey comparable](ctx context.Context, limiter *Limiter[TKey], key TKey) *limit[TKey] {
	l := &limit[TKey]{}
	ticker := time.NewTicker(time.Duration(limiter.period) * time.Millisecond)
	l.next.Store(time.Now().UnixMilli())
	l.run(ctx, ticker, limiter, key)
	return l
}

func (l *limit[TKey]) run(ctx context.Context, ticker *time.Ticker, limiter *Limiter[TKey], key TKey) {
	for {
		select {
		case <-ticker.C:
			l.tick(ticker, limiter, key)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (l *limit[TKey]) tick(ticker *time.Ticker, limiter *Limiter[TKey], key TKey) {
	if l.amount.Add(-1) < 1 {
		ticker.Stop()
		limiter.remove(key)
		return
	}
	l.next.Add(limiter.period)
}

func (l *limit[TKey]) call(maxBurst int32, period int64) (bool, time.Time) {
	// Add and subtract can be overhead, but it ensures absence of gaps between check and addition without swap loops
	incAmount := l.amount.Add(1)
	if incAmount > maxBurst {
		l.amount.Add(-1)
		return false, time.UnixMilli(l.next.Load())
	}
	return true, time.Time{}
}
