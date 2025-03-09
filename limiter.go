package rate_limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

type Limiter[TKey comparable] struct {
	mu       *sync.RWMutex
	limits   map[TKey]*limit[TKey]
	maxBurst int32
	period   int64
}

func New[TKey comparable](maxBurst int, periodMillis int) *Limiter[TKey] {
	if maxBurst < 1 {
		maxBurst = 1
	}
	if maxBurst > math.MaxInt32 {
		panic("to large max burst")
	}
	if periodMillis < 1 {
		periodMillis = 1
	}
	return &Limiter[TKey]{
		mu:       &sync.RWMutex{},
		limits:   make(map[TKey]*limit[TKey]),
		maxBurst: int32(maxBurst),
		period:   int64(periodMillis),
	}
}

func (l *Limiter[TKey]) Try(ctx context.Context, key TKey) (can bool, waitUntil time.Time) {
	l.mu.RLock()
	lim, ok := l.limits[key]
	l.mu.RUnlock()
	if !ok {
		lim = newLimit(ctx, l, key)
		l.mu.Lock()
		// have to check one more time since we have new critical section here.
		// crating separate critical section to avoid excessive write locks
		if checkLim, ok := l.limits[key]; ok {
			lim = checkLim
		} else {
			l.limits[key] = lim
		}
		l.mu.Unlock()
	}
	return lim.call(l.maxBurst, l.period)
}

func (l *Limiter[TKey]) remove(key TKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.limits, key)
}
