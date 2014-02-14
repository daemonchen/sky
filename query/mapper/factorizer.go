package mapper

// Factorizer manages factorization of values.
type Factorizer interface {
	Factorize(id string, value string, createIfMissing bool) (uint64, error)
	Defactorize(id string, value uint64) (string, error)
}
