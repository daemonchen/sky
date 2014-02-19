package db

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/szferi/gomdb"
)

// Table represents a collection of objects.
type Table struct {
	sync.RWMutex

	db         *DB
	name       string
	path       string
	properties map[string]*Property
	env        *mdb.Env

	shardCount     int
	maxPermanentID int
	maxTransientID int

	noSync     bool
	maxDBs     uint
	maxReaders uint
}

// Name returns the name of the table.
func (t *Table) Name() string {
	return t.name
}

// Path returns the location of the table on disk.
func (t *Table) Path() string {
	return t.path
}

// Exists returns whether the table exists.
func (t *Table) Exists() bool {
	_, err := os.Stat(t.path)
	return !os.IsNotExist(err)
}

func (t *Table) create() error {
	t.Lock()
	defer t.Unlock()

	// Create directory.
	if err := os.MkdirAll(t.path, 0700); err != nil {
		return err
	}

	// Set initial shard count.
	if t.shardCount == 0 {
		t.shardCount = runtime.NumCPU()
	}

	// Open the table.
	if err := t._open(); err != nil {
		return err
	}

	// Save initial table state.
	if err := t.save(); err != nil {
		return err
	}

	return nil
}

// open opens and initializes the table.
func (t *Table) open() error {
	t.Lock()
	defer t.Unlock()
	return t._open()
}

func (t *Table) _open() error {
	if t.env != nil {
		return nil
	} else if !t.Exists() {
		return ErrTableNotFound
	}

	// Initialize directory.
	if err := os.MkdirAll(t.path, 0700); err != nil {
		return err
	}

	// Create LMDB environment.
	env, err := mdb.NewEnv()
	assert(err == nil, "table env error: %v", err)

	// LMDB environment settings.
	err = env.SetMaxDBs(mdb.DBI(t.maxDBs))
	assert(err == nil, "max dbs (%d) error: %s", t.maxDBs, err)
	err = env.SetMaxReaders(t.maxReaders)
	assert(err == nil, "max readers (%d) error: %s", t.maxReaders, err)
	err = env.SetMapSize(1 << 34)
	assert(err == nil, "map size error: %s", err)

	// Set LMDB flags.
	options := uint(mdb.NOTLS)
	if t.noSync {
		options |= mdb.NOSYNC
	}

	// Open the LMDB environment.
	if err := env.Open(t.path, options, 0600); err != nil {
		env.Close()
		return &Error{"table open error", err}
	}
	t.env = env

	// Create standard tables.
	err = t.txn(0, func(txn *transaction) error {
		err := txn.dbi("meta", 0)
		assert(err == nil, "meta dbi error: %v", err)

		// Initialize shards.
		for i := 0; i < t.shardCount; i++ {
			err := txn.dbi(fmt.Sprintf("shard.%d", i), mdb.DUPSORT)
			assert(err == nil, "shard dbi error: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Load the metadata.
	if err := t.load(); err != nil {
		t._close()
		return err
	}

	return nil
}

// drop closes and removes the table.
func (t *Table) drop() error {
	t.Lock()
	defer t.Unlock()

	// Close table and delete everything.
	t._close()
	if err := os.RemoveAll(t.path); err != nil {
		return err
	}

	return nil
}

// opened returned whether the table is currently open.
func (t *Table) opened() bool {
	return t.env != nil
}

func (t *Table) close() {
	t.Lock()
	defer t.Unlock()
	t._close()
}

func (t *Table) _close() {
	if t.env != nil {
		t.env.Close()
		t.env = nil
	}
}

func (t *Table) load() error {
	return t.txn(mdb.RDONLY, func(txn *transaction) error {
		value, err := txn.get("meta", []byte("meta"))
		if err != nil {
			return &Error{"table meta error", err}
		} else if len(value) == 0 {
			return nil
		}
		warnf("unmarshal: %s", string(value))
		if err := t.unmarshal(value); err != nil {
			return err
		}
		return nil
	})
}

func (t *Table) save() error {
	return t.txn(0, func(txn *transaction) error {
		value, err := t.marshal()
		warnf("marshal: %s", string(value))
		assert(err == nil, "table marshal error: %v", err)
		return txn.put("meta", []byte("meta"), value)
	})
}

// Properties retrieves a map of properties by property name.
func (t *Table) Properties() (map[string]*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}
	return t.properties, nil
}

// Property returns a single property from the table with the given name.
func (t *Table) Property(name string) (*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}
	return t.properties[name], nil
}

// PropertyByID returns a single property from the table by id.
func (t *Table) PropertyByID(id int) (*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}
	for _, p := range t.properties {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, nil
}

// CreateProperty creates a new property on the table.
func (t *Table) CreateProperty(name string, dataType string, transient bool) (*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}

	// Don't allow duplicate names.
	if t.properties[name] != nil {
		return nil, ErrPropertyExists
	}

	// Create and validate property.
	p := &Property{
		Name:      name,
		Transient: transient,
		DataType:  dataType,
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}

	// Retrieve the next property id.
	if transient {
		t.maxTransientID--
		p.ID = t.maxTransientID
	} else {
		t.maxPermanentID++
		p.ID = t.maxPermanentID
	}

	// Add it to the collection.
	t.copyProperties()
	t.properties[name] = p

	if err := t.save(); err != nil {
		return nil, err
	}

	return p, nil
}

// RenameProperty updates the name of a property.
func (t *Table) RenameProperty(oldName, newName string) (*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	} else if t.properties[oldName] == nil {
		return nil, ErrPropertyNotFound
	} else if t.properties[newName] != nil {
		return nil, ErrPropertyExists
	}

	t.copyProperties()
	p := t.properties[oldName].Clone()
	p.Name = newName
	delete(t.properties, oldName)
	t.properties[newName] = p

	if err := t.save(); err != nil {
		return nil, err
	}
	return p, nil
}

// DeleteProperty removes a single property from the table.
func (t *Table) DeleteProperty(name string) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return ErrTableNotOpen
	} else if t.properties[name] == nil {
		return ErrPropertyNotFound
	}
	delete(t.properties, name)
	return t.save()
}

// copyProperties creates a new map and copies all existing properties.
func (t *Table) copyProperties() {
	properties := make(map[string]*Property)
	for k, v := range t.properties {
		properties[k] = v
	}
	t.properties = properties
}

// marshal encodes the table into a byte slice.
func (t *Table) marshal() ([]byte, error) {
	var msg = tableRawMessage{Name: t.name, ShardCount: t.shardCount, MaxPermanentID: t.maxPermanentID, MaxTransientID: t.maxTransientID}
	for _, p := range t.properties {
		msg.Properties = append(msg.Properties, p)
	}
	return json.Marshal(msg)
}

// unmarshal decodes a byte slice into a table.
func (t *Table) unmarshal(data []byte) error {
	var msg tableRawMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	t.name = msg.Name
	t.maxPermanentID = msg.MaxPermanentID
	t.maxTransientID = msg.MaxTransientID
	t.shardCount = msg.ShardCount

	t.properties = make(map[string]*Property)
	for _, p := range msg.Properties {
		t.properties[p.Name] = p
	}

	return nil
}

// txn executes a function within the context of a LMDB transaction.
func (t *Table) txn(flags uint, fn func(*transaction) error) error {
	txn, err := t.env.BeginTxn(nil, flags)
	if err != nil {
		return &Error{"txn error", err}
	}
	if err := fn(&transaction{txn}); err != nil {
		txn.Abort()
		return err
	}
	if err := txn.Commit(); err != nil {
		return &Error{"txn commit error", err}
	}
	return nil
}

type tableRawMessage struct {
	Name           string      `json:"name"`
	ShardCount     int         `json:"shardCount"`
	MaxPermanentID int         `json:"maxPermanentID"`
	MaxTransientID int         `json:"maxTransientID"`
	Properties     []*Property `json:"properties"`
}

// Event represents the state for an object at a given point in time.
type Event struct {
	Timestamp time.Time
	Data      map[string]interface{}
}

// rawEvent represents an internal event.
type rawEvent struct {
	Timestamp int64
	Data      map[int64]interface{}
}
