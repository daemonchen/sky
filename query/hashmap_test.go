package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that integer values can be set and retrieved.
func TestHashmapInt(t *testing.T) {
	h := NewHashmap()
	defer h.Free()

	max := 10000
	for i := 0; i < max; i++ {
		h.Set(int64(i), int64(max-i))
	}
	for i := 0; i < max; i++ {
		assert.Equal(t, h.Get(int64(i)), int64(max-i))
	}
}

// Ensure that float values can be set and retrieved.
func TestHashmapFloat64(t *testing.T) {
	h := NewHashmap()
	defer h.Free()

	max := 10000
	for i := 0; i < max; i++ {
		h.SetDouble(int64(i), float64(max-i)/100)
	}
	for i := 0; i < max; i++ {
		assert.Equal(t, h.GetDouble(int64(i)), float64(max-i)/100)
	}
}

// Ensure that hashmap values can be retrieved.
func TestHashmapSubmap(t *testing.T) {
	h := NewHashmap()
	defer h.Free()
	s0 := h.Submap(10)
	s0.Set(100, 200)
	s1 := h.Submap(11)
	s1.Set(100, 300)
	assert.Equal(t, h.Submap(10).Get(100), 200)
	assert.Equal(t, h.Submap(11).Get(100), 300)
}

// Ensure that retrieving a submap will overwrite an int value.
func TestHashmapOverrideWithSubmap(t *testing.T) {
	h := NewHashmap()
	defer h.Free()
	h.Set(10, 100)
	s0 := h.Submap(10)
	s0.Set(20, 200)
	assert.Equal(t, h.Submap(10).Get(20), 200)
}

// Ensure that retrieving an int will overwrite a submap value.
func TestHashmapOverrideWithInt(t *testing.T) {
	h := NewHashmap()
	defer h.Free()
	h.Submap(10)
	h.Set(10, 100)
	assert.Equal(t, h.Get(10), 100)
}

// Ensure that iterator goes over each value once.
func TestHashmapIterator(t *testing.T) {
	count := 10000
	h := NewHashmap()
	defer h.Free()
	for i := 0; i < count; i++ {
		h.Set(int64(i), 10)
	}

	iterator := NewHashmapIterator(h)
	results := make(map[int64]bool)
	for {
		key, _, ok := iterator.Next()
		if !ok {
			break
		}
		results[key] = true
	}

	for i := 0; i < count; i++ {
		assert.Equal(t, results[int64(i)], true)
	}
}

// Runs the benchmark 10,000 times per iteration within the C context.
func BenchmarkHashmapGet10k(b *testing.B) {
	h := NewHashmap()
	for i := 0; i < BucketCount(); i++ {
		h.Set(int64(i), 100)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchmarkGet(h, 10000)
	}
}

// Runs the benchmark 10,000 times per iteration within the C context.
func BenchmarkHashmapSet10k(b *testing.B) {
	h := NewHashmap()
	for i := 0; i < b.N; i++ {
		benchmarkSet(h, 10000)
	}
}
