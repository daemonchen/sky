package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/skydb/sky/hash"
	"github.com/szferi/gomdb"
	"github.com/ugorji/go/codec"
)

// maxKeySize is the size, in bytes, of the largest key that can be inserted.
// This is a limitation of LMDB.
const maxKeySize = 500

// FactorCacheSize is the number of factors that are stored in the LRU cache.
// This cache size is per-property.
const FactorCacheSize = 1000

// Table represents a collection of objects.
type Table struct {
	sync.RWMutex

	db             *DB
	name           string
	path           string
	properties     map[string]*Property
	propertiesByID map[int]*Property
	env            *mdb.Env
	caches         map[int]*cache

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
			err := txn.dbi(shardDBName(i), mdb.DUPSORT)
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

	// Initialize factor databases.
	err = t.txn(0, func(txn *transaction) error {
		for _, p := range t.properties {
			if p.DataType != Factor {
				continue
			}
			err := txn.dbi(factorDBName(p.ID), 0)
			assert(err == nil, "factor dbi error: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Initialize the factor caches.
	t.caches = make(map[int]*cache)
	for _, p := range t.properties {
		if p.DataType == Factor {
			t.caches[p.ID] = newCache(FactorCacheSize)
		}
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
		// warnf("unmarshal: %s", string(value))
		if err := t.unmarshal(value); err != nil {
			return err
		}
		return nil
	})
}

func (t *Table) save() error {
	return t.txn(0, func(txn *transaction) error {
		value, err := t.marshal()
		// warnf("marshal: %s", string(value))
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

// Properties retrieves a map of properties by property identifier.
func (t *Table) PropertiesByID() (map[int]*Property, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}
	return t.propertiesByID, nil
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
	return t.propertiesByID[id], nil
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

	// Initialize factor database.
	if p.DataType == Factor {
		err := t.txn(0, func(txn *transaction) error {
			err := txn.dbi(factorDBName(p.ID), 0)
			assert(err == nil, "factor dbi error: %v", err)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Add it to the collection.
	properties, propertiesByID := t.properties, t.propertiesByID
	t.copyProperties()
	t.properties[name] = p
	t.propertiesByID[p.ID] = p

	if err := t.save(); err != nil {
		t.properties = properties
		t.propertiesByID = propertiesByID
		return nil, err
	}

	// Initialize the cache.
	if p.DataType == Factor {
		t.caches[p.ID] = newCache(FactorCacheSize)
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

	properties := t.properties
	t.copyProperties()
	p := t.properties[oldName].Clone()
	p.Name = newName
	delete(t.properties, oldName)
	t.properties[newName] = p

	if err := t.save(); err != nil {
		t.properties = properties
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
	}

	p := t.properties[name]
	if p == nil {
		return ErrPropertyNotFound
	}

	properties, propertiesByID := t.properties, t.propertiesByID
	t.copyProperties()
	delete(t.properties, name)
	delete(t.propertiesByID, p.ID)

	if err := t.save(); err != nil {
		t.properties = properties
		t.propertiesByID = propertiesByID
		return err
	}
	return nil
}

// copyProperties creates a new map and copies all existing properties.
func (t *Table) copyProperties() {
	properties := make(map[string]*Property)
	for k, v := range t.properties {
		properties[k] = v
	}
	t.properties = properties

	propertiesByID := make(map[int]*Property)
	for k, v := range t.propertiesByID {
		propertiesByID[k] = v
	}
	t.propertiesByID = propertiesByID
}

// GetEvent returns a single event for an object at a given timestamp.
func (t *Table) GetEvent(id string, timestamp time.Time) (*Event, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}
	rawEvent, err := t.getRawEvent(id, shiftTime(timestamp))
	if err != nil {
		return nil, err
	} else if rawEvent == nil {
		return nil, nil
	}
	return t.toEvent(rawEvent)
}

// GetEvents returns all events for an object in chronological order.
func (t *Table) GetEvents(id string) ([]*Event, error) {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return nil, ErrTableNotOpen
	}

	// Retrieve raw events.
	rawEvents, err := t.getRawEvents(id)
	if err != nil {
		return nil, err
	}

	// Convert to regular events and return.
	var events []*Event
	for _, rawEvent := range rawEvents {
		event, err := t.toEvent(rawEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *Table) getRawEvent(id string, timestamp int64) (*rawEvent, error) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, timestamp)
	prefix := buf.Bytes()

	// Retrieve event bytes from the database.
	var b []byte
	err := t.txn(mdb.RDONLY, func(txn *transaction) error {
		var err error
		b, err = txn.getAt(shardDBName(t.shardIndex(id)), []byte(id), prefix)
		return err
	})
	if err != nil {
		return nil, err
	} else if b == nil {
		return nil, nil
	}

	// Unmarshal bytes into a raw event.
	e := &rawEvent{}
	if err := e.unmarshal(b); err != nil {
		return nil, err
	}

	return e, nil
}

func (t *Table) getRawEvents(id string) ([]*rawEvent, error) {
	// Retrieve all bytes from the database.
	var slices [][]byte
	err := t.txn(mdb.RDONLY, func(txn *transaction) error {
		var err error
		slices, err = txn.getAll(shardDBName(t.shardIndex(id)), []byte(id))
		return err
	})
	if err != nil {
		return nil, err
	} else if slices == nil {
		return nil, nil
	}

	// Unmarshal each slice into a raw event.
	var events []*rawEvent
	for _, b := range slices {
		e := &rawEvent{}
		if err := e.unmarshal(b); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

// InsertEvent inserts an event for an object.
func (t *Table) InsertEvent(id string, event *Event) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return ErrTableNotOpen
	}
	return t.insertEvent(id, event)
}

// InsertEvents inserts multiple events for an object.
func (t *Table) InsertEvents(id string, events []*Event) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return ErrTableNotOpen
	}
	for _, event := range events {
		if err := t.insertEvent(id, event); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) insertEvent(id string, e *Event) error {
	// Convert to raw event.
	rawEvent, err := t.toRawEvent(e)
	if err != nil {
		return err
	}

	// Retrieve existing event for object at the same moment and merge.
	current, err := t.getRawEvent(id, rawEvent.timestamp)
	if current != nil {
		data := current.data
		for k, v := range rawEvent.data {
			data[k] = v
		}
		rawEvent.data = data
	}

	// Marshal raw event into byte slice.
	b, err := rawEvent.marshal()
	if err != nil {
		return err
	}

	// Create the timestamp prefix.
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rawEvent.timestamp)
	prefix := buf.Bytes()

	// Insert event into appropriate shard.
	return t.txn(0, func(txn *transaction) error {
		return txn.putAt(shardDBName(t.shardIndex(id)), []byte(id), prefix, b)
	})
}

// DeleteEvent removes a single event for an object at a given timestamp.
func (t *Table) DeleteEvent(id string, timestamp time.Time) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return ErrTableNotOpen
	}

	// Create the timestamp prefix.
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, shiftTime(timestamp))
	prefix := buf.Bytes()

	// Delete the event.
	return t.txn(0, func(txn *transaction) error {
		return txn.delAt(shardDBName(t.shardIndex(id)), []byte(id), prefix)
	})
}

// DeleteEvents removes all events for an object.
func (t *Table) DeleteEvents(id string) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return ErrTableNotOpen
	}

	// Delete all events.
	return t.txn(0, func(txn *transaction) error {
		return txn.del(shardDBName(t.shardIndex(id)), []byte(id))
	})
}

// factorize converts a factor property value to its integer index representation.
// Returns an error if the factor could not be found and createIfNotExists is false.
func (t *Table) factorize(propertyID int, value string, createIfNotExists bool) (int, error) {
	// Blank is always zero.
	if value == "" {
		return 0, nil
	}

	// Check the LRU first.
	if sequence, ok := t.caches[propertyID].getValue(value); ok {
		return sequence, nil
	}

	// Find an existing factor for the value.
	var val int
	err := t.txn(mdb.RDONLY, func(txn *transaction) error {
		data, err := txn.get(factorDBName(propertyID), factorKey(value))
		if err != nil {
			return err
		} else if data != nil {
			val = int(binary.BigEndian.Uint64(data))
		}
		return nil
	})
	if err != nil {
		return 0, err
	} else if val != 0 {
		return val, nil
	}

	// Create a new factor if requested.
	if createIfNotExists {
		return t.addFactor(propertyID, value)
	}

	return 0, ErrFactorNotFound
}

func (t *Table) addFactor(propertyID int, value string) (int, error) {
	var index int
	err := t.txn(0, func(txn *transaction) error {
		// Look up next sequence index.
		data, err := txn.get(factorDBName(propertyID), []byte("+"))
		if err != nil {
			return err
		} else if data == nil {
			data = make([]byte, 8)
		}

		// Read identifier and increment.
		index = int(binary.BigEndian.Uint64(data))
		index += 1

		// Save incremented index.
		binary.BigEndian.PutUint64(data, uint64(index))
		if err = txn.put(factorDBName(propertyID), []byte("+"), data); err != nil {
			return err
		}

		// Truncate the value so it fits in our max key size.
		value = truncateFactor(value)

		// Store the value-to-index lookup.
		binary.BigEndian.PutUint64(data[:], uint64(index))
		if err := txn.put(factorDBName(propertyID), factorKey(value), data); err != nil {
			return err
		}

		// Save the index-to-value lookup.
		if err := txn.put(factorDBName(propertyID), reverseFactorKey(index), []byte(value)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Add to cache.
	t.caches[propertyID].add(value, index)

	return index, nil
}

// defactorize converts a factor index to its string value.
func (t *Table) defactorize(propertyID int, index int) (string, error) {
	// Blank is always zero.
	if index == 0 {
		return "", nil
	}

	// Check the cache first.
	if key, ok := t.caches[propertyID].getKey(index); ok {
		return key, nil
	}

	var data []byte
	err := t.txn(mdb.RDONLY, func(txn *transaction) error {
		var err error
		data, err = txn.get(factorDBName(propertyID), reverseFactorKey(index))
		return err
	})
	if err != nil {
		return "", err
	} else if data == nil {
		return "", ErrFactorNotFound
	}

	// Add to cache.
	t.caches[propertyID].add(string(data), index)

	return string(data), nil
}

// shardIndex returns the appropriate shard for a given object id.
func (t *Table) shardIndex(id string) int {
	return int(hash.Local(id)) % t.shardCount
}

// shardDBName returns the name of the shard table.
func shardDBName(index int) string {
	return fmt.Sprintf("shards/%d", index)
}

// factorDBName returns the name of the factor table for a property.
func factorDBName(propertyID int) string {
	return fmt.Sprintf("factors/%d", propertyID)
}

// factorKey returns the value-to-index key.
func factorKey(value string) []byte {
	return []byte(fmt.Sprintf(">%s", truncateFactor(value)))
}

// reverseFactorKey returns the index-to-value key.
func reverseFactorKey(index int) []byte {
	return []byte(fmt.Sprintf("<%d", index))
}

// truncateFactor returns the value that can be saved to the factorizer because
// of LMDB key size restrictions.
func truncateFactor(value string) string {
	if len(value) > maxKeySize {
		return value[0:maxKeySize]
	}
	return value
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
	t.propertiesByID = make(map[int]*Property)
	for _, p := range msg.Properties {
		t.properties[p.Name] = p
		t.propertiesByID[p.ID] = p
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

// toRawEvent returns a raw event representation of this event.
func (t *Table) toRawEvent(e *Event) (*rawEvent, error) {
	rawEvent := &rawEvent{
		timestamp: shiftTime(e.Timestamp),
		data:      make(map[int]interface{}),
	}

	// Map data by property id instead of name.
	for k, v := range e.Data {
		p := t.properties[k]
		if p == nil {
			return nil, ErrPropertyNotFound
		}

		// Cast the value to the appropriate type.
		v = p.Cast(v)

		// Factorize value, if needed.
		if p.DataType == Factor {
			var err error
			v, err = t.factorize(p.ID, v.(string), true)
			if err != nil {
				return nil, err
			}
		}

		rawEvent.data[p.ID] = v
	}

	return rawEvent, nil
}

// toEvent returns an normal event representation of this raw event.
func (t *Table) toEvent(e *rawEvent) (*Event, error) {
	event := &Event{
		Timestamp: unshiftTime(e.timestamp),
		Data:      make(map[string]interface{}),
	}

	// Map data by name instead of property id.
	for k, v := range e.data {
		p := t.propertiesByID[k]

		// Missing properties have been deleted so just ignore.
		if p == nil {
			continue
		}

		// Cast the value to the appropriate type.
		v = promote(v)

		// Defactorize value, if needed.
		if p.DataType == Factor {
			var err error
			if intValue, ok := v.(int64); ok {
				v, err = t.defactorize(p.ID, int(intValue))
				if err != nil {
					return nil, err
				}
			} else {
				return nil, &Error{fmt.Sprintf("invalid factor value: %v", v), nil}
			}
		}

		event.Data[p.Name] = v
	}

	return event, nil
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

// rawEvent represents an internal event structure.
type rawEvent struct {
	timestamp int64
	data      map[int]interface{}
}

// marshal encodes the raw event as a byte slice.
func (e *rawEvent) marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, e.timestamp)
	assert(err == nil, "timestamp marshal error: %v", err)

	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(&buf, &handle).Encode(e.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// unmarshal decodes a raw event from a byte slice.
func (e *rawEvent) unmarshal(b []byte) error {
	var buf = bytes.NewBuffer(b)
	err := binary.Read(buf, binary.BigEndian, &e.timestamp)
	assert(err == nil, "timestamp unmarshal error: %v", err)

	e.data = make(map[int]interface{})
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewDecoder(buf, &handle).Decode(&e.data); err != nil {
		return err
	}
	e.normalize()

	return nil
}

// normalize promotes all values of the raw event to appropriate types.
func (e *rawEvent) normalize() {
	for k, v := range e.data {
		e.data[k] = promote(v)
	}
}
