// Package backoff provides backoff mechanisms for retries.
package backoff

import (
	"math"
	"math/rand"
	"time"
)

// JitterExp implements the jitter exponential backoff algorithm described in
// https://www.awsarchitectureblog.com/2015/03/backoff.html
type JitterExp struct {
	attempt int
	base    float64
	unit    time.Duration

	waitTime         time.Duration
	maxTotalWaitTime time.Duration
}

// NewJitterExp creates a new JitterExp.
func NewJitterExp(base float64, unit, maxTotalWaitTime time.Duration) *JitterExp {
	je := &JitterExp{
		base:             base,
		unit:             unit,
		maxTotalWaitTime: maxTotalWaitTime,
	}
	je.Reset()
	return je
}

// Reset resets the retry attempt counters so that the backoff restarts.
func (je *JitterExp) Reset() {
	je.attempt = -1
	je.waitTime = 0
}

// Wait pauses the calling goroutine according to the jitter exponential backoff algorithm.
// The returned value indicates whether the caller should continue retrying.
func (je *JitterExp) Wait() bool {
	available := je.maxTotalWaitTime - je.waitTime
	if available < time.Microsecond {
		return false
	}

	je.attempt++
	max := math.Pow(je.base, float64(je.attempt))
	jitter := rand.Float64() * max
	d := je.unit * time.Duration(jitter)
	if d > available {
		d = available
	}

	je.waitTime += d
	<-time.After(d)

	return true
}
