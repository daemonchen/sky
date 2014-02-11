package db

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/szferi/gomdb"
	"github.com/ugorji/go/codec"
)

// shard represents a subset of the database stored in a single LMDB environment.
type shard struct {
	sync.Mutex
	path string
	env  *mdb.Env
}

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
}

// newShard creates a new shard.
func newShard(path string) *shard {
	return &shard{path: path}
}

func (s *shard) Stat() (*Stat, error) {
	stat, err := s.env.Stat()
	if err != nil {
		return nil, err
	}
	info, err := s.env.Info()
	if err != nil {
		return nil, err
	}
	ss := &Stat{
		Entries: stat.Entries,
		Size:    info.MapSize,
		Depth:   stat.Depth,
	}
	ss.Transactions.Last = info.LastTxnID
	ss.Readers.Max = info.MaxReaders
	ss.Readers.Current = info.NumReaders
	ss.Pages.Last = info.LastPNO
	ss.Pages.Size = stat.PSize
	ss.Pages.Branch = stat.BranchPages
	ss.Pages.Leaf = stat.LeafPages
	ss.Pages.Overflow = stat.OwerflowPages
	return ss, nil
}

// Open allocates a new LMDB environment.
func (s *shard) Open(maxDBs uint, maxReaders uint, options uint) error {
	s.Lock()
	defer s.Unlock()
	s.close()

	if err := os.MkdirAll(s.path, 0700); err != nil {
		return err
	}

	var err error
	s.env, err = mdb.NewEnv()
	if err != nil {
		return &Error{"shard env error", err}
	}

	// LMDB environment settings.
	if err := s.env.SetMaxDBs(mdb.DBI(maxDBs)); err != nil {
		s.close()
		return &Error{"shard maxdbs error", err}
	} else if err := s.env.SetMaxReaders(maxReaders); err != nil {
		s.close()
		return &Error{"shard maxreaders error", err}
	} else if err := s.env.SetMapSize(2 << 40); err != nil {
		s.close()
		return &Error{"shard map size error", err}
	}

	// Open the LMDB environment.
	if err := s.env.Open(s.path, options, 0664); err != nil {
		s.close()
		return &Error{"shard open error", err}
	}

	return nil
}

// Close releases all shard resources.
func (s *shard) Close() {
	s.Lock()
	defer s.Unlock()
	s.close()
}

func (s *shard) close() {
	if s.env != nil {
		s.env.Close()
		s.env = nil
	}
}

// Cursor retrieves a cursor for iterating over the shard.
func (s *shard) Cursor(tablespace string) (*mdb.Cursor, error) {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, &Error{"shard cursor error", err}
	}

	c, err := s.cursor(txn, dbi)
	if err != nil {
		c.Close()
		txn.Commit()
		return nil, err
	}

	return c, err
}

// cursor retrieves a cursor for iterating over the shard.
func (s *shard) cursor(txn *mdb.Txn, dbi mdb.DBI) (*mdb.Cursor, error) {
	c, err := txn.CursorOpen(dbi)
	if err != nil {
		return nil, &Error{"shard cursor open error", err}
	}
	return c, nil
}

// InsertEvent adds a single event to the shard.
func (s *shard) InsertEvent(tablespace string, id string, event *Event) error {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return &Error{"shard txn begin error", err}
	}
	defer txn.Commit()

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return &Error{"shard cursor error", err}
	}
	defer c.Close()

	if err := s.insertEvent(txn, dbi, c, id, shiftTimeBytes(event.Timestamp), event.Data); err != nil {
		return err
	}

	return nil
}

func (s *shard) insertEvent(txn *mdb.Txn, dbi mdb.DBI, c *mdb.Cursor, id string, timestamp []byte, data map[int64]interface{}) error {
	// Get event at timestamp and merge if existing.
	if old, err := s.getEvent(c, id, timestamp); err != nil {
		return err
	} else if old != nil {
		for k, v := range data {
			old[k] = v
		}
		data = old
		if err := c.Del(0); err != nil {
			return &Error{"shard cursor del error", err}
		}
	}

	// Encode timestamp.
	var b bytes.Buffer
	if _, err := b.Write(timestamp); err != nil {
		return err
	}

	// Encode data.
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(&b, &handle).Encode(data); err != nil {
		return err
	}

	// Insert event.
	if err := txn.Put(dbi, []byte(id), b.Bytes(), 0); err != nil {
		return &Error{"shard txn put error", err}
	}

	return nil
}

// InsertEvents adds a multiple events for an object to the shard.
func (s *shard) InsertEvents(tablespace string, id string, events []*Event) error {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return &Error{"insert events error", err}
	}
	defer txn.Commit()

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return &Error{"insert events error", err}
	}
	defer c.Close()

	for _, event := range events {
		if err := s.insertEvent(txn, dbi, c, id, shiftTimeBytes(event.Timestamp), event.Data); err != nil {
			return err
		}
	}

	return nil
}

// Retrieves an event for a given object at a single point in time.
func (s *shard) GetEvent(tablespace string, id string, timestamp time.Time) (*Event, error) {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, &Error{"get event error", err}
	}
	defer txn.Commit()

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return nil, &Error{"get event error", err}
	}
	defer c.Close()

	data, err := s.getEvent(c, id, shiftTimeBytes(timestamp))
	if err != nil || data == nil {
		return nil, err
	}

	return &Event{Timestamp: timestamp, Data: data}, nil
}

func (s *shard) getEvent(c *mdb.Cursor, id string, timestamp []byte) (map[int64]interface{}, error) {
	// Position cursor at possible event.
	_, _, err := mdbGet2(c, []byte(id), timestamp, mdb.GET_RANGE)
	if err == mdb.NotFound || err == mdb.Incompatibile {
		return nil, nil
	} else if err != nil {
		return nil, &Error{"get event error", err}
	}

	// Retrieve current cursor value.
	_, val, err := c.Get(nil, mdb.GET_CURRENT)
	if err == mdb.NotFound {
		return nil, nil
	} else if err != nil {
		return nil, &Error{"get current error", err}
	}

	// Check if timestamp is equal.
	if !bytes.Equal(timestamp, val[0:8]) {
		return nil, nil
	}

	// Decode data.
	var data = make(map[int64]interface{})
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewDecoder(bytes.NewBuffer(val[8:]), &handle).Decode(&data); err != nil {
		return nil, err
	}
	for k, v := range data {
		data[k] = promote(v)
	}

	return data, nil
}

// Retrieves a list of events for a given object in a table.
func (s *shard) GetEvents(tablespace string, id string) ([]*Event, error) {
	s.Lock()
	defer s.Unlock()

	var events = make([]*Event, 0)

	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, &Error{"get events error", err}
	}
	defer txn.Commit()

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return nil, &Error{"get events error", err}
	}
	defer c.Close()

	// Initialize cursor.
	if _, _, err := mdbGet2(c, []byte(id), []byte{0}, mdb.GET_RANGE); err == mdb.NotFound || err == mdb.Incompatibile {
		return events, nil
	} else if err != nil {
		return nil, &Error{"get events error", err}
	}

	for {
		_, val, err := c.Get([]byte(id), mdb.GET_CURRENT)
		if err != nil {
			return nil, &Error{"get current error", err}
		}

		// Create event.
		event := &Event{
			Timestamp: unshiftTimeBytes(val[0:8]),
			Data:      make(map[int64]interface{}),
		}

		// Decode data.
		var handle codec.MsgpackHandle
		handle.RawToString = true
		if err := codec.NewDecoder(bytes.NewBuffer(val[8:]), &handle).Decode(&event.Data); err != nil {
			return nil, err
		}
		for k, v := range event.Data {
			event.Data[k] = promote(v)
		}

		events = append(events, event)

		// Move cursor forward.
		if _, _, err := c.Get([]byte(id), mdb.NEXT_DUP); err == mdb.NotFound {
			break
		} else if err != nil {
			return nil, &Error{"next dup error", err}
		}
	}

	return events, nil
}

// DeleteEvent removes a single event from the shard.
func (s *shard) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s.Lock()
	defer s.Unlock()

	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return &Error{"delete event error", err}
	}
	defer txn.Commit()

	c, err := s.cursor(txn, dbi)
	if err != nil {
		return &Error{"delete event error", err}
	}
	defer c.Close()

	// Check if event exists and move the cursor.
	if old, err := s.getEvent(c, id, shiftTimeBytes(timestamp)); err != nil {
		return err
	} else if old != nil {
		if err := c.Del(0); err != nil {
			return &Error{"delete event error", err}
		}
	}

	return nil
}

// Deletes all events for a given object in a table.
func (s *shard) DeleteObject(tablespace, id string) error {
	s.Lock()
	defer s.Unlock()

	// Begin a transaction.
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return &Error{"delete object error", err}
	}
	defer txn.Commit()

	// Delete the key.
	if err = txn.Del(dbi, []byte(id), nil); err != nil && err != mdb.NotFound {
		return &Error{"delete object error", err}
	}

	return nil
}

// Drop removes a table from the shard.
func (s *shard) Drop(tablespace string) error {
	s.Lock()
	defer s.Unlock()
	return s.drop(tablespace)
}

func (s *shard) drop(tablespace string) error {
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return &Error{"drop error", err}
	}
	defer txn.Commit()

	// Drop the table.
	if err = txn.Drop(dbi, 1); err != nil {
		return &Error{"drop error", err}
	}

	return nil
}

func (s *shard) txn(tablespace string, readOnly bool) (*mdb.Txn, mdb.DBI, error) {
	var flags uint = 0
	if readOnly {
		flags = flags | mdb.RDONLY
	}

	// Setup cursor to iterate over table.
	txn, err := s.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, 0, &Error{"txn error", err}
	}
	var dbi mdb.DBI
	if readOnly {
		if dbi, err = txn.DBIOpen(&tablespace, mdb.DUPSORT); err != nil && err != mdb.NotFound {
			return nil, 0, &Error{"read-only dbi error", err}
		}
	} else {
		if dbi, err = txn.DBIOpen(&tablespace, mdb.CREATE|mdb.DUPSORT); err != nil {
			return nil, 0, &Error{"rw dbi error", err}
		}
	}

	return txn, dbi, nil
}
