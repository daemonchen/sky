package sky

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// ErrObjectIDRequired is returned inserting, deleting, or retrieving
	// event data without specifying an object identifier.
	ErrObjectIDRequired = errors.New("object id required")
)

// DB represents a collection of objects.
type DB struct {
	sync.Mutex

	db   *bolt.DB
	path string
	stat Stat

	shardCount int
}

// Path returns the location of the table on disk.
func (db *DB) Path() string {
	return db.path
}

// ShardCount returns the number of shards in the table.
func (db *DB) ShardCount() int {
	if db.shardCount == 0 {
		db.shardCount = runtime.NumCPU()
	}
	return db.shardCount
}

// Open opens and initializes the table.
func (db *DB) Open() error {
	db.Lock()
	defer db.Unlock()

	if db.db != nil {
		return nil
	}

	// Create Bolt database.
	boltdb, err := bolt.Open(db.path, 0666)
	if err != nil {
		return fmt.Errorf("table open: %s", err)
	}
	db.db = boltdb

	// Initialize schema.
	err = db.Update(func(tx *Tx) error {
		// Create shard buckets.
		for i := 0; i < db.ShardCount(); i++ {
			if err := tx.CreateBucketIfNotExists(shardDBName(i)); err != nil {
				return fmt.Errorf("shard: %s", err)
			}
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	if db.db != nil {
		db.db.Close()
		db.db = nil
	}
}

// View executes a function in the context of a read-only transaction.
func (db *DB) View(fn func(*Tx) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, db})
	})
}

// Update executes a function in the context of a writable transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, db})
	})
}

// Tx represents a transaction.
type Tx struct {
	*bolt.Tx
	DB *DB
}

// Event returns a single event for an object at a given timestamp.
func (tx *Tx) Event(id string, timestamp time.Time) (*Event, error) {
	rawEvent, err := tx.getRawEvent(id, timestamp.UnixNano())
	if err != nil {
		return nil, err
	}

	return &Event{timestamp: timestamp, data: rawEvent.data}, nil
}

// Events returns all events for an object in chronological order.
func (tx *Tx) Events(id string) ([]*Event, error) {
	// Retrieve raw events.
	rawEvents, err := tx.getRawEvents(id)
	if err != nil {
		return nil, err
	}

	// Convert to regular events and return.
	var events []*Event
	for _, rawEvent := range rawEvents {
		event, err := tx.toEvent(rawEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (tx *Tx) getRawEvent(id string, timestamp int64) (*rawEvent, error) {
	if id == "" {
		return nil, ErrObjectIDRequired
	}

	shard := tx.Bucket(tx.DB.shardDBName(shardIndex(id)))

	object := shard.Bucket(prefix)
	if object == nil {
		return nil, fmt.Errorf("object key not found: %s", id)
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, timestamp)
	prefix := buf.Bytes()

	event := object.Get(prefix)
	if event == nil {
		return nil, fmt.Errorf("event not found: %d", timestamp)
	}

	return &rawEvent{timestamp: timestamp, data: event}, nil
}

func (tx *Tx) getRawEvents(id string) ([]*rawEvent, error) {
	if id == "" {
		return nil, ErrObjectIDRequired
	}

	// Retrieve all bytes from the database.
	var stat = bench()
	var slices [][]byte
	err := tx.txn(mdb.RDONLY, func(txn *transaction) error {
		var err error
		slices, err = txn.getAll(shardDBName(t.shardIndex(id)), []byte(id))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get raw events error: %s", err)
	} else if slices == nil {
		return nil, nil
	}
	stat.count += len(slices)
	stat.apply(&tx.stat.Event.Fetch.Count, &tx.stat.Event.Fetch.Duration)

	// Unmarshal each slice into a raw event.
	stat = bench()
	var events []*rawEvent
	for _, b := range slices {
		e := &rawEvent{}
		if err := e.unmarshal(b); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	stat.count += len(events)
	stat.apply(&tx.stat.Event.Unmarshal.Count, &tx.stat.Event.Unmarshal.Duration)
	return events, nil
}

// InsertEvent inserts an event for an object.
func (tx *Tx) InsertEvent(id string, event *Event) error {
	tx.Lock()
	defer tx.Unlock()
	if !tx.opened() {
		return fmt.Errorf("table not open: %s", t.name)
	}
	return tx.insertEvent(id, event)
}

// InsertEvents inserts multiple events for an object.
func (tx *Tx) InsertEvents(id string, events []*Event) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return fmt.Errorf("table not open: %s", t.name)
	}
	for _, event := range events {
		if err := t.insertEvent(id, event); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) insertEvent(id string, e *Event) error {
	if id == "" {
		return ErrObjectIDRequired
	}
	// Convert to raw event.
	rawEvent, err := t.toRawEvent(e)
	if err != nil {
		return fmt.Errorf("insert event error: %s", err)
	}

	// Truncate the id so it fits in our max key size.
	id = truncateFactor(id)

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
	var stat = bench()
	b, err := rawEvent.marshal()
	if err != nil {
		return err
	}
	stat.count++
	stat.apply(&t.stat.Event.Marshal.Count, &t.stat.Event.Marshal.Duration)

	// Create the timestamp prefix.
	stat = bench()
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rawEvent.timestamp)
	prefix := buf.Bytes()

	// Insert event into appropriate shard.
	err = t.txn(0, func(txn *transaction) error {
		if err := txn.putAt(shardDBName(t.shardIndex(id)), []byte(id), prefix, b); err != nil {
			return fmt.Errorf("insert event put error on id=%s and prefix=%s with event=%+v: %s", id, prefix, rawEvent, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("insert event txn error: %s", err)
	}
	stat.count++
	stat.apply(&t.stat.Event.Insert.Count, &t.stat.Event.Insert.Duration)
	return nil
}

// DeleteEvent removes a single event for an object at a given timestamp.
func (tx *Tx) DeleteEvent(id string, timestamp time.Time) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return fmt.Errorf("table not open: %s", t.name)
	}

	// Create the timestamp prefix.
	var stat = bench()
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, shiftTime(timestamp))
	prefix := buf.Bytes()

	// Delete the event.
	err := t.txn(0, func(txn *transaction) error {
		return txn.delAt(shardDBName(t.shardIndex(id)), []byte(id), prefix)
	})
	if err != nil {
		return fmt.Errorf("delete event txn error: %s", err)
	}
	stat.count++
	stat.apply(&t.stat.Event.Delete.Count, &t.stat.Event.Delete.Duration)
	return err
}

// DeleteEvents removes all events for an object.
func (tx *Tx) DeleteEvents(id string) error {
	t.Lock()
	defer t.Unlock()
	if !t.opened() {
		return fmt.Errorf("table not open: %s", t.name)
	}

	// Delete all events.
	err := t.txn(0, func(txn *transaction) error {
		return txn.del(shardDBName(t.shardIndex(id)), []byte(id))
	})
	if err != nil {
		return fmt.Errorf("delete events txn error: %s", err)
	}
	return nil
}

// ForEach executes a function once for each shard in the table.
// A different cursor is passed in for each function invocation.
//
// IMPORTANT: The function is responsible for closing the cursor.
func (tx *Tx) ForEach(fn func(c *Cursor)) error {
	if !t.opened() {
		return fmt.Errorf("table not open: %s", t.name)
	}
	for i := 0; i < t.DB.ShardCount(); i++ {
		txn, err := t.env.BeginTxn(nil, mdb.RDONLY)
		if err != nil {
			return fmt.Errorf("foreach txn error: %s", err)
		}

		shardDBName := shardDBName(i)
		dbi, err := txn.DBIOpen(&shardDBName, 0)
		if err != nil {
			return fmt.Errorf("foreach dbi error: %s", err)
		}

		c, err := txn.CursorOpen(dbi)
		if err != nil {
			return fmt.Errorf("foreach cursor error: %s", err)
		}

		fn(&Cursor{c})
	}

	return nil
}

// Keys returns all object keys in the table in sorted order.
func (tx *Tx) Keys() ([]string, error) {
	var keys = make([]string, 0)
	err := t.ForEach(func(c *Cursor) {
		for k, _, err := c.Get(nil, mdb.NEXT_NODUP); err != mdb.NotFound; k, _, err = c.Get(nil, mdb.NEXT_NODUP) {
			keys = append(keys, string(k))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("keys error: %s", err)
	}
	sort.Strings(keys)
	return keys, nil
}

// shardIndex returns the appropriate shard for a given object id.
func (db *DB) shardIndex(id string) int {
	return int(hash.Local(id)) % db.ShardCount()
}

// shardDBName returns the name of the shard table.
func shardDBName(index int) []byte {
	return []byte(fmt.Sprintf("shards/%d", index))
}

// Event represents the state for an object at a given point in time.
type Event struct {
	Timestamp time.Time
	Data      []byte
}

// rawEvent represents an internal event structure.
type rawEvent struct {
	timestamp int64
	data      []byte
}

// stat represents a simple counter and timer.
type stat struct {
	count int
	time  time.Time
}

// since returns the elapsed time since the stat began.
func (s *stat) since() time.Duration {
	return time.Since(s.time)
}

// apply increments the count and duration based on the stat.
func (s *stat) apply(count *int, duration *time.Duration) {
	*count += s.count
	*duration += time.Since(s.time)
}

// bench begins a timed stat counter.
func bench() stat {
	return stat{0, time.Now()}
}

// Stat represents statistics for a single table.
type Stat struct {
	Entries      uint64 `json:"entries"` // Number of data items
	Size         uint64 `json:"size"`    // Size of the data memory map
	Depth        uint   `json:"depth"`   // Depth (height) of the B-tree
	Transactions struct {
		Last uint64 `json:"last"` // ID of the last committed transaction
	} `json:"transactions"`
	Readers struct {
		Max     uint `json:"max"`     // maximum number of threads for the environment
		Current uint `json:"current"` // maximum number of threads used in the environment
	} `json:"readers"`
	Pages struct {
		Last     uint64 `json:"last"`     // ID of the last used page
		Size     uint   `json:"size"`     // Size of a database page. This is currently the same for all databases.
		Branch   uint64 `json:"branch"`   // Number of internal (non-leaf) pages
		Leaf     uint64 `json:"leaf"`     // Number of leaf pages
		Overflow uint64 `json:"overflow"` // Number of overflow pages
	} `json:"pages"`
	Event struct {
		Fetch struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"fetch"`
		Insert struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"insert"`
		Delete struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"delete"`
		Factorize struct {
			CacheHit struct {
				Count int `json:"count"`
			} `json:"cacheHit"`
			FetchHit struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchHit"`
			FetchMiss struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchMiss"`
			Create struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"create"`
		} `json:"factorize"`
		Defactorize struct {
			CacheHit struct {
				Count int `json:"count"`
			} `json:"cacheHit"`
			FetchHit struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchHit"`
			FetchMiss struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchMiss"`
		} `json:"defactorize"`
		Marshal struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"marshal"`
		Unmarshal struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"unmarshal"`
	} `json:"event"`
}

// Diff calculates the difference between a stat object and another.
func (s *Stat) Diff(other *Stat) *Stat {
	diff := &Stat{}
	diff.Entries = s.Entries - other.Entries
	diff.Size = s.Size - other.Size
	diff.Depth = s.Depth - other.Depth
	diff.Transactions.Last = s.Transactions.Last - other.Transactions.Last
	diff.Readers.Max = s.Readers.Max - other.Readers.Max
	diff.Readers.Current = s.Readers.Current - other.Readers.Current
	diff.Pages.Last = s.Pages.Last - other.Pages.Last
	diff.Pages.Size = s.Pages.Size - other.Pages.Size
	diff.Pages.Branch = s.Pages.Branch - other.Pages.Branch
	diff.Pages.Leaf = s.Pages.Leaf - other.Pages.Leaf
	diff.Pages.Overflow = s.Pages.Overflow - other.Pages.Overflow
	diff.Event.Fetch.Count = s.Event.Fetch.Count - other.Event.Fetch.Count
	diff.Event.Fetch.Duration = s.Event.Fetch.Duration - other.Event.Fetch.Duration
	diff.Event.Insert.Count = s.Event.Insert.Count - other.Event.Insert.Count
	diff.Event.Insert.Duration = s.Event.Insert.Duration - other.Event.Insert.Duration
	diff.Event.Delete.Count = s.Event.Delete.Count - other.Event.Delete.Count
	diff.Event.Delete.Duration = s.Event.Delete.Duration - other.Event.Delete.Duration
	diff.Event.Factorize.CacheHit.Count = s.Event.Factorize.CacheHit.Count - other.Event.Factorize.CacheHit.Count
	diff.Event.Factorize.FetchHit.Count = s.Event.Factorize.FetchHit.Count - other.Event.Factorize.FetchHit.Count
	diff.Event.Factorize.FetchHit.Duration = s.Event.Factorize.FetchHit.Duration - other.Event.Factorize.FetchHit.Duration
	diff.Event.Factorize.FetchMiss.Count = s.Event.Factorize.FetchMiss.Count - other.Event.Factorize.FetchMiss.Count
	diff.Event.Factorize.FetchMiss.Duration = s.Event.Factorize.FetchMiss.Duration - other.Event.Factorize.FetchMiss.Duration
	diff.Event.Factorize.Create.Count = s.Event.Factorize.Create.Count - other.Event.Factorize.Create.Count
	diff.Event.Factorize.Create.Duration = s.Event.Factorize.Create.Duration - other.Event.Factorize.Create.Duration
	diff.Event.Defactorize.CacheHit.Count = s.Event.Defactorize.CacheHit.Count - other.Event.Defactorize.CacheHit.Count
	diff.Event.Defactorize.FetchHit.Count = s.Event.Defactorize.FetchHit.Count - other.Event.Defactorize.FetchHit.Count
	diff.Event.Defactorize.FetchHit.Duration = s.Event.Defactorize.FetchHit.Duration - other.Event.Defactorize.FetchHit.Duration
	diff.Event.Defactorize.FetchMiss.Count = s.Event.Defactorize.FetchMiss.Count - other.Event.Defactorize.FetchMiss.Count
	diff.Event.Defactorize.FetchMiss.Duration = s.Event.Defactorize.FetchMiss.Duration - other.Event.Defactorize.FetchMiss.Duration
	diff.Event.Marshal.Count = s.Event.Marshal.Count - other.Event.Marshal.Count
	diff.Event.Marshal.Duration = s.Event.Marshal.Duration - other.Event.Marshal.Duration
	diff.Event.Unmarshal.Count = s.Event.Unmarshal.Count - other.Event.Unmarshal.Count
	diff.Event.Unmarshal.Duration = s.Event.Unmarshal.Duration - other.Event.Unmarshal.Duration
	return diff
}
