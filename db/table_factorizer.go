package db

// TableFactorizer manages factorization for a single table.
type TableFactorizer interface {
	Factorize(id string, value string, createIfMissing bool) (uint64, error)
	Defactorize(id string, value uint64) (string, error)
}

// NewTableFactorizer creates a new TableFactorizer instance.
func NewTableFactorizer(f *Factorizer, tablespace string) TableFactorizer {
	return &tableFactorizer{
		f:          f,
		tablespace: tablespace,
	}
}

type tableFactorizer struct {
	f          *Factorizer
	tablespace string
}

func (f *tableFactorizer) Factorize(id string, value string, createIfMissing bool) (uint64, error) {
	return f.f.Factorize(f.tablespace, id, value, createIfMissing)
}

func (f *tableFactorizer) Defactorize(id string, value uint64) (string, error) {
	return f.f.Defactorize(f.tablespace, id, value)
}
