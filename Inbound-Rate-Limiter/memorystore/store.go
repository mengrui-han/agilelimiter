// Package memorystore implements per-key, fixed-window rate limiting in memory.
package memorystore

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	limiter "github.com/Yiming1997/agilelimiter/Inbound-Rate-Limiter"
)

var _ limiter.Store = (*store)(nil)

// store holds default configuration, per-key buckets, and shutdown state.
type store struct {
	tokens   uint64
	interval time.Duration

	sweepInterval time.Duration
	sweepMinTTL   uint64

	// data holds the per-key buckets. It is a sync.Map because the access
	// pattern is create-once, read-many: a bucket is created on a key's first
	// Take and then read on every subsequent Take. sync.Map serves those reads
	// from a read-only snapshot without mutating shared state, avoiding the
	// per-call atomic writes an RWMutex makes to a single counter (which bounces
	// that cache line across cores under load).
	data sync.Map // map[string]*bucket

	stopped atomic.Bool
	stopCh  chan struct{}
}

// Config supplies user options to New. Zero values use the documented defaults.
type Config struct {
	// Tokens is the number of tokens to allow per interval. The default value is
	// 1.
	Tokens uint64

	// Interval is the time interval upon which to enforce rate limiting. The default value is 1 second.
	Interval time.Duration

	// SweepInterval is the rate at which to run the garbage collection on stale
	// entries. Setting this to a low value will optimize memory consumption, but
	// will likely reduce performance and increase lock contention. Setting this
	// to a high value will maximum throughput, but will increase the memory
	// footprint. This can be tuned in combination with SweepMinTTL to control how
	// long stale entries are kept. The default value is 6 hours.
	SweepInterval time.Duration

	// SweepMinTTL is measured from the last recorded window start, not the exact
	// last access. It defaults to 12 hours. Keep it at least as long as every
	// bucket's window to avoid discarding active limits; this is not validated.
	SweepMinTTL time.Duration

	// InitialAlloc previously pre-sized the in-memory map. It is retained for
	// backward compatibility but no longer has any effect: the store now uses a
	// sync.Map, which does not support pre-sizing.
	InitialAlloc int

	// DisablePurge disables the purge operation. WARNING: this will cause
	// unbounded memory growth. Do not enable unless you have a fixed number of
	// buckets.
	DisablePurge bool
}

// New creates an in-memory rate limiter that uses a bucketing model to limit
// the number of permitted events over an interval. It's optimized for runtime
// and memory efficiency.
func New(c *Config) limiter.Store {
	if c == nil {
		c = new(Config)
	}

	tokens := uint64(1)
	if c.Tokens > 0 {
		tokens = c.Tokens
	}

	interval := 1 * time.Second
	if c.Interval > 0 {
		interval = c.Interval
	}
	// Default to a 6-hour cleanup interval unless a positive value is supplied.
	sweepInterval := 6 * time.Hour
	if c.SweepInterval > 0 {
		sweepInterval = c.SweepInterval
	}

	sweepMinTTL := 12 * time.Hour
	if c.SweepMinTTL > 0 {
		sweepMinTTL = c.SweepMinTTL
	}

	s := &store{
		tokens:   tokens,
		interval: interval,

		sweepInterval: sweepInterval,
		sweepMinTTL:   uint64(sweepMinTTL),

		stopCh: make(chan struct{}),
	}

	if !c.DisablePurge {
		go s.purge()
	}

	return s
}

// Take finds or creates the key's bucket and attempts to consume one token.
// It returns the limit, remaining tokens, next reset time in Unix nanoseconds,
// whether a token was granted, and any error. Exhaustion is not an error.
func (s *store) Take(ctx context.Context, key string) (uint64, uint64, uint64, bool, error) {
	// Reject access once the atomic stopped flag is set.
	if s.stopped.Load() {
		return 0, 0, 0, false, limiter.ErrStopped
	}

	// Reuse the existing bucket when the key is present.
	if v, ok := s.data.Load(key); ok {
		return v.(*bucket).take()
	}

	// Create a candidate bucket using the store's default allowance and interval.
	b := newBucket(s.tokens, s.interval)
	// Store the candidate if absent, or use the bucket another goroutine stored
	// first. LoadOrStore returns the stored bucket pointer without copying it.
	actual, _ := s.data.LoadOrStore(key, b)
	// Consume from the bucket actually stored for this key.
	return actual.(*bucket).take()
}

// Get reads the stored limit and remaining tokens without consuming tokens
// or refreshing the window. An absent key returns zero values and no error.
func (s *store) Get(ctx context.Context, key string) (uint64, uint64, error) {
	// Reject access once the atomic stopped flag is set.
	if s.stopped.Load() {
		return 0, 0, limiter.ErrStopped
	}

	// Find the bucket; its get method acquires a read lock to read the balance.
	if v, ok := s.data.Load(key); ok {
		return v.(*bucket).get()
	}

	return 0, 0, nil
}

// Set replaces the key's bucket with a full bucket using the supplied limit
// and duration. This restarts its window; a zero token limit is allowed.
func (s *store) Set(ctx context.Context, key string, tokens uint64, interval time.Duration) error {
	// Fall back to the store's default duration for a non-positive interval,
	// avoiding an invalid divisor in tick.
	if interval <= 0 {
		interval = s.interval
	}
	// Create a new bucket and overwrite any existing record for this key.
	s.data.Store(key, newBucket(tokens, interval))
	return nil
}

// Burst adds temporary tokens without changing the normal window allowance.
// It does not refresh the window; the next window reset discards the bonus.
func (s *store) Burst(ctx context.Context, key string, tokens uint64) error {
	// Add the bonus directly to the existing bucket.
	if v, ok := s.data.Load(key); ok {
		v.(*bucket).burst(tokens)
		return nil
	}

	// Create a candidate bucket with the default allowance and window duration.
	b := newBucket(s.tokens, s.interval)
	// Add the bonus before attempting to store the candidate.
	b.burst(tokens)
	// If another goroutine stored a bucket first, add the bonus to that bucket.
	// Otherwise the candidate was stored with the bonus already applied.
	if actual, loaded := s.data.LoadOrStore(key, b); loaded {
		actual.(*bucket).burst(tokens)
	}
	return nil
}

// Close marks the store as stopped, signals the purge loop, and deletes
// entries. Repeated calls return nil. It does not wait for the purge loop
// or in-flight operations to finish.
func (s *store) Close(ctx context.Context) error {
	// Only the caller that atomically changes false to true performs shutdown,
	// preventing a second close of stopCh.
	if !s.stopped.CompareAndSwap(false, true) {
		return nil
	}
	// Close the channel to notify the purge loop to exit.
	close(s.stopCh)

	// Delete visited entries. Returning true tells Range to continue.
	s.data.Range(func(k, _ any) bool {
		s.data.Delete(k)
		return true
	})
	return nil
}

// purge periodically scans and removes stale buckets using one shared
// cleanup loop rather than a separate expiration goroutine per key.
func (s *store) purge() {
	// Use the configured cleanup interval, which defaults to 6 hours.
	ticker := time.NewTicker(s.sweepInterval)
	defer ticker.Stop()

	// Wait for shutdown or the next cleanup tick. If neither channel is ready,
	// block here; closing stopCh exits the method, while a tick starts a scan.
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
		// Capture the current wall-clock time in Unix nanoseconds.
		now := uint64(time.Now().UnixNano())
		// Visit each key and its stored bucket pointer.
		s.data.Range(func(k, v any) bool {
			// Assert the stored value's type without copying the bucket.
			b := v.(*bucket)
			// Acquire a read lock before reading the bucket's window state.
			b.lock.RLock()
			// Calculate the last recorded window start from the origin and window index.
			// This is not the exact last-access time.
			lastTime := b.startTime + (b.lastTick * uint64(b.interval))
			// Release the read lock after reading the window state.
			b.lock.RUnlock()

			// Clamp to this pass's timestamp to prevent unsigned subtraction underflow
			// if a concurrent update or clock adjustment makes lastTime later.
			if lastTime > now {
				lastTime = now
			}
			// Remove entries beyond the retention threshold. This deletion is not atomic
			// with bucket updates because the read lock has already been released.
			if now-lastTime > s.sweepMinTTL {
				s.data.Delete(k)
			}
			return true
		})
	}
}

// bucket tracks the allowance and fixed-window state for one key.
type bucket struct {
	// startTime is the number of nanoseconds from unix epoch when this bucket was
	// initially created.
	startTime uint64

	// maxTokens is the normal allowance restored at each new window.
	// Burst may temporarily raise the available balance above this value.
	maxTokens uint64

	// interval is the time at which ticking should occur.
	interval time.Duration

	// availableTokens is the current point-in-time number of tokens remaining.
	availableTokens uint64

	// lastTick is the last recorded window index, not a timestamp.
	lastTick uint64

	// lock guards the mutable fields.
	lock sync.RWMutex
}

// newBucket creates a new bucket from the given tokens and interval.
func newBucket(tokens uint64, interval time.Duration) *bucket {
	b := &bucket{
		startTime:       uint64(time.Now().UnixNano()),
		maxTokens:       tokens,
		availableTokens: tokens,
		interval:        interval,
	}
	return b
}

// get reads the stored allowance and balance without refreshing the window.
func (b *bucket) get() (tokens uint64, remaining uint64, retErr error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	tokens = b.maxTokens
	remaining = b.availableTokens
	return
}

// take refreshes the balance on a new window and attempts to consume one token.
// reset is an absolute Unix nanosecond timestamp, not a duration. Named results
// start at their zero values; retErr remains nil.
func (b *bucket) take() (tokens uint64, remaining uint64, reset uint64, ok bool, retErr error) {
	// Capture the current wall-clock time in Unix nanoseconds.
	now := uint64(time.Now().UnixNano())
	// Serialize changes to this bucket; release the lock when the method exits.
	b.lock.Lock()
	defer b.lock.Unlock()

	// If the clock precedes the window origin, rebase the origin and reset the
	// recorded window index to zero. This does not itself refill the bucket.
	if now < b.startTime {
		b.startTime = now
		b.lastTick = 0
	}
	// Compute the zero-based window index: (now - startTime) / interval.
	currTick := tick(b.startTime, now, b.interval)
	// Return the normal per-window allowance.
	tokens = b.maxTokens
	// Return the next window's start as an absolute Unix nanosecond timestamp.
	reset = b.startTime + ((currTick + 1) * uint64(b.interval))

	// On a new window, restore the normal allowance and update the recorded
	// window index. Unused allowances do not accumulate across windows.
	if b.lastTick < currTick {
		b.availableTokens = b.maxTokens
		b.lastTick = currTick
	}
	// Consume one token, mark the request as allowed, and report the new balance.
	// Taking the last token still succeeds. With no tokens, ok and remaining
	// retain their zero values.
	if b.availableTokens > 0 {
		b.availableTokens--
		ok = true
		remaining = b.availableTokens
	}

	return
}

// burst adds the specified number of tokens to the bucket's available tokens in
// a thread-safe manner. The addition saturates at math.MaxUint64 so a large
// burst cannot overflow and wrap to a smaller value.
func (b *bucket) burst(tokens uint64) {
	b.lock.Lock()
	if b.availableTokens > math.MaxUint64-tokens {
		b.availableTokens = math.MaxUint64
	} else {
		b.availableTokens += tokens
	}
	b.lock.Unlock()
}

// tick is the total number of times the current interval has occurred between
// when the time started (start) and the current time (curr). For example, if
// the start time was 12:30pm and it's currently 1:00pm, and the interval was 5
// minutes, tick would return 6 because 1:00pm is the 6th 5-minute tick. Note
// that tick would return 5 at 12:59pm, because it hasn't reached the 6th tick
// yet.
func tick(start, curr uint64, interval time.Duration) uint64 {
	return (curr - start) / uint64(interval.Nanoseconds())
}
