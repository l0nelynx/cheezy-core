package connectionlimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Limiter bounds the number of physical connections that share an upstream.
// A Limiter is intentionally reusable across adapter reloads through Registry.
type Limiter struct {
	limit   int
	tokens  chan struct{}
	active  atomic.Int64
	waiting atomic.Int64
}

type Snapshot struct {
	Active  int64
	Waiting int64
	Limit   int
}

func New(limit int) (*Limiter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("connection limit must be positive: %d", limit)
	}
	return &Limiter{limit: limit, tokens: make(chan struct{}, limit)}, nil
}

func (l *Limiter) Acquire(ctx context.Context) (func(), error) {
	l.waiting.Add(1)
	select {
	case l.tokens <- struct{}{}:
		l.waiting.Add(-1)
		l.active.Add(1)
		var once sync.Once
		return func() {
			once.Do(func() {
				<-l.tokens
				l.active.Add(-1)
			})
		}, nil
	case <-ctx.Done():
		l.waiting.Add(-1)
		return nil, ctx.Err()
	}
}

func (l *Limiter) Snapshot() Snapshot {
	return Snapshot{Active: l.active.Load(), Waiting: l.waiting.Load(), Limit: l.limit}
}

type Registry struct {
	mu       sync.Mutex
	limiters map[string]*Limiter
}

func NewRegistry() *Registry {
	return &Registry{limiters: make(map[string]*Limiter)}
}

func (r *Registry) Get(key string, limit int) (*Limiter, error) {
	if limit <= 0 {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.limiters[key]; current != nil {
		if current.limit != limit {
			return nil, fmt.Errorf("connection limit for %q changed from %d to %d", key, current.limit, limit)
		}
		return current, nil
	}
	limiter, err := New(limit)
	if err != nil {
		return nil, err
	}
	r.limiters[key] = limiter
	return limiter, nil
}
