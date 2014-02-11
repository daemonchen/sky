package db

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we open a factorizer.
func TestFactorizerOpen(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	f := NewFactorizer()
	assert.NoError(t, f.Open(path))
	assert.Equal(t, path, f.Path())
	f.Close()
}

// Ensure that we can factorize and defactorize values.
func TestFactorizer(t *testing.T) {
	withFactorizer(func(f *Factorizer) {
		num, err := f.Factorize("bar", "/index.html", true)
		if err != nil || num != 1 {
			t.Fatalf("Wrong factorization: exp: %v, got: %v (%v)", 1, num, err)
		}
		num, err = f.Factorize("bar", "/about.html", true)
		if err != nil || num != 2 {
			t.Fatalf("Wrong factorization: exp: %v, got: %v (%v)", 2, num, err)
		}

		str, err := f.Defactorize("bar", 1)
		if err != nil || str != "/index.html" {
			t.Fatalf("Wrong defactorization: exp: %v, got: %v (%v)", "/index.html", str, err)
		}
		str, err = f.Defactorize("bar", 2)
		if err != nil || str != "/about.html" {
			t.Fatalf("Wrong defactorization: exp: %v, got: %v (%v)", "/about.html", str, err)
		}
	})
}

// Ensure that very large factorized values get truncated.
func TestFactorizerTruncate(t *testing.T) {
	withFactorizer(func(f *Factorizer) {
		value := "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
		shortValue := f.truncate(value)
		num, err := f.Factorize("bar", value, true)
		assert.Equal(t, num, uint64(1))
		assert.NoError(t, err)
		str, err := f.Defactorize("bar", 1)
		assert.Equal(t, len(str), 500)
		assert.NoError(t, err)
		num2, err := f.Factorize("bar", shortValue, true)
		assert.Equal(t, num2, uint64(1))
		assert.NoError(t, err)
	})
}

func BenchmarkFactorizer(b *testing.B) {
	withFactorizer(func(f *Factorizer) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Factorize("bar", strconv.Itoa(i), true)
		}
	})
}

func BenchmarkFactorizerCache(b *testing.B) {
	withFactorizer(func(f *Factorizer) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Factorize("bar", strconv.Itoa(i%2), true)
		}
	})
}

func withFactorizer(fn func(f *Factorizer)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	f := NewFactorizer()
	if err := f.Open(path); err != nil {
		panic(err.Error())
	}
	defer f.Close()

	fn(f)
}
