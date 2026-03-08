package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type latencyMetrics struct {
	enabled atomic.Bool

	ingestedRecords atomic.Uint64
	lastEventTimeNs atomic.Int64

	mu             sync.Mutex
	rateCounter    int64
	rateLastBucket int64
	rateBuckets    *int64Ring
}

func newLatencyMetrics(window int) *latencyMetrics {
	if window < 16 {
		window = 16
	}
	return &latencyMetrics{
		rateBuckets: newInt64Ring(window),
	}
}

func (m *latencyMetrics) setEnabled(v bool) { m.enabled.Store(v) }

func (m *latencyMetrics) observeIngest(now time.Time) {
	if !m.enabled.Load() {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.ingestedRecords.Add(1)

	bucket := now.Unix()
	m.mu.Lock()
	if bucket == m.rateLastBucket {
		m.rateCounter++
	} else {
		if m.rateLastBucket > 0 {
			m.rateBuckets.add(m.rateCounter)
			// fill zeros for any idle seconds between last activity and now
			idle := bucket - m.rateLastBucket - 1
			if idle > int64(len(m.rateBuckets.buf)) {
				idle = int64(len(m.rateBuckets.buf))
			}
			for i := int64(0); i < idle; i++ {
				m.rateBuckets.add(0)
			}
		}
		m.rateLastBucket = bucket
		m.rateCounter = 1
	}
	m.mu.Unlock()
}

func (m *latencyMetrics) observeEventTime(t time.Time) {
	if !t.IsZero() {
		m.lastEventTimeNs.Store(t.UnixNano())
	}
}

type snapshot struct {
	records       uint64
	ingestRps     int64
	lastEventTime time.Time
}

func (m *latencyMetrics) snapshot() snapshot {
	if !m.enabled.Load() {
		return snapshot{}
	}

	records := m.ingestedRecords.Load()
	lastEventNs := m.lastEventTimeNs.Load()

	m.mu.Lock()
	rps := m.currentRate()
	m.mu.Unlock()

	var lastEventTime time.Time
	if lastEventNs > 0 {
		lastEventTime = time.Unix(0, lastEventNs)
	}

	return snapshot{
		records:       records,
		ingestRps:     rps,
		lastEventTime: lastEventTime,
	}
}

// currentRate returns the median rate including the current in-progress second
// and any idle seconds since the last activity. Called with m.mu held.
func (m *latencyMetrics) currentRate() int64 {
	if m.rateLastBucket == 0 {
		return 0
	}
	// Copy ring so we don't mutate the real one.
	r := &int64Ring{
		buf:   append([]int64(nil), m.rateBuckets.buf...),
		idx:   m.rateBuckets.idx,
		count: m.rateBuckets.count,
	}
	r.add(m.rateCounter)
	idle := time.Now().Unix() - m.rateLastBucket - 1
	if idle > int64(len(r.buf)) {
		idle = int64(len(r.buf))
	}
	for i := int64(0); i < idle; i++ {
		r.add(0)
	}
	return medianRate(r)
}

// medianRate returns the median of recent per-second record counts.
func medianRate(r *int64Ring) int64 {
	if r == nil || r.count == 0 {
		return 0
	}
	vals := ringValues(r)
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

func ringValues(r *int64Ring) []int64 {
	vals := make([]int64, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := r.idx - r.count + i
		for idx < 0 {
			idx += len(r.buf)
		}
		vals = append(vals, r.buf[idx%len(r.buf)])
	}
	return vals
}

// int64Ring is a fixed-size circular buffer of int64 values.
type int64Ring struct {
	buf   []int64
	idx   int
	count int
}

func newInt64Ring(n int) *int64Ring {
	if n < 1 {
		n = 1
	}
	return &int64Ring{buf: make([]int64, n)}
}

func (r *int64Ring) add(v int64) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.idx] = v
	r.idx++
	if r.idx >= len(r.buf) {
		r.idx = 0
	}
	if r.count < len(r.buf) {
		r.count++
	}
}
