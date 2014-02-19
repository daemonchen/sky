package db

import (
	"github.com/szferi/gomdb"
)

// transaction wraps an LMDB transaction into something more tolerable.
type transaction struct {
	*mdb.Txn
}

// dbi creates a db in the transaction with the given name and flags.
func (t *transaction) dbi(name string, flags uint) error {
	_, err := t.DBIOpen(&name, mdb.CREATE|flags)
	if err != nil {
		return &Error{"dbi error", err}
	}
	return nil
}

// get returns the value for a given key in a given named db.
func (t *transaction) get(name string, key []byte) ([]byte, error) {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return nil, &Error{"dbi error", err}
	}

	value, err := t.Get(dbi, []byte(key))
	if err != nil && err != mdb.NotFound {
		return nil, &Error{"get error", err}
	}
	return value, nil
}

// put sets the value for a given key in a given named db.
func (t *transaction) put(name string, key []byte, value []byte) error {
	dbi, err := t.DBIOpen(&name, 0)
	if err != nil {
		return &Error{"dbi error", err}
	}

	if err := t.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return &Error{"put error", err}
	}
	return nil
}
