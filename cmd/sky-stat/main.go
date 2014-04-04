package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/skydb/sky/db"
	"github.com/szferi/gomdb"
	"github.com/ugorji/go/codec"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: sky-stat PATH\n")
		os.Exit(1)
	}
}

func main() {
	log.SetFlags(0)
	flag.Parse()
	path := flag.Arg(0)
	if path == "" {
		flag.Usage()
	}

	// Open the table environment.
	env, err := openEnv(path)
	if err != nil {
		log.Fatalln("env:", err)
	}
	defer env.Close()

	// Start transaction.
	txn, err := env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		log.Fatalln("txn:", err)
	}
	defer txn.Abort()

	// Read table meta.
	meta, err := readMeta(txn)
	if err != nil {
		log.Fatalln("meta:", err)
	}

	// Print meta info.
	fmt.Println("")
	fmt.Println("# META ##################################")
	fmt.Println("Table:", meta.Name)
	fmt.Println("Shard Count:", meta.ShardCount)
	fmt.Println("Property Count:", len(meta.Properties))
	fmt.Println("")

	// Show shard stats.
	for i := 0; i < meta.ShardCount; i++ {
		if err := printShardStats(txn, i); err != nil {
			log.Fatalf("shard %d: %v", i, err)
		}
	}

	// Show property stats.
	for _, p := range meta.Properties {
		if p.DataType == db.Factor {
			if err := printPropertyStats(txn, p); err != nil {
				log.Fatalf("property[%d/%s]: %v", p.ID, p.Name, err)
			}
		}
	}

}

func openEnv(path string) (*mdb.Env, error) {
	// Setup environment.
	env, err := mdb.NewEnv()
	if err != nil {
		return nil, fmt.Errorf("new: %s", err)
	}
	env.SetMaxDBs(mdb.DBI(10000))
	env.SetMaxReaders(400)
	env.SetMapSize(1 << 40)

	// Open environment.
	if err := env.Open(path, uint(mdb.NOTLS), 0600); err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}

	return env, nil
}

func readMeta(txn *mdb.Txn) (*tableRawMessage, error) {
	// Read meta value from meta DBI.
	var dbname = "meta"
	dbi, err := txn.DBIOpen(&dbname, 0)
	if err != nil {
		return nil, fmt.Errorf("dbi: %s", err)
	}
	value, err := txn.Get(dbi, []byte("meta"))
	if err != nil && err != mdb.NotFound {
		return nil, fmt.Errorf("get: %s", err)
	}

	// Unmarshal to a table message.
	meta := &tableRawMessage{}
	if err := json.Unmarshal(value, &meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func printShardStats(txn *mdb.Txn, index int) error {
	var dbname = shardDBName(index)
	dbi, err := txn.DBIOpen(&dbname, mdb.DUPSORT)
	if err != nil {
		return fmt.Errorf("dbi: %s", err)
	}

	// Create a cursor.
	c, err := txn.CursorOpen(dbi)
	if err != nil {
		return fmt.Errorf("foreach cursor error: %s", err)
	}
	defer c.Close()

	fmt.Printf("# SHARD %d ##############################\n", index)

	// Loop over every object.
	var size, propertyCount, objectCount, eventCount int
	for k, _, err := c.Get(nil, mdb.NEXT_NODUP); err != mdb.NotFound; k, _, err = c.Get(nil, mdb.NEXT_NODUP) {
		objectCount++

		for _, v, err := c.Get(k, mdb.GET_CURRENT); err != mdb.NotFound; _, v, err = c.Get(k, mdb.GET_CURRENT) {
			if err != nil {
				return fmt.Errorf("iter: %s", err)
			}

			size += len(v)
			eventCount++

			if len(v) > 8 {
				var buf = bytes.NewBuffer(v[8:])
				data := make(map[int]interface{})
				var handle codec.MsgpackHandle
				handle.RawToString = true
				if err := codec.NewDecoder(buf, &handle).Decode(&data); err != nil {
					return err
				}
				propertyCount += len(data)
			}

			// Move cursor forward.
			if _, _, err := c.Get(k, mdb.NEXT_DUP); err == mdb.NotFound {
				break
			} else if err != nil {
				return fmt.Errorf("iter2: %s", err)
			}
		}
	}

	fmt.Println("Total Size:", size)
	fmt.Println("Object Count:", objectCount)
	fmt.Println("Event Count:", eventCount)
	fmt.Println("Avg Events per Object:", eventCount/objectCount)
	fmt.Println("Avg Bytes per Event:", size/eventCount)
	fmt.Println("")

	return nil
}

func printPropertyStats(txn *mdb.Txn, p *db.Property) error {
	var dbname = factorDBName(p.ID)
	dbi, err := txn.DBIOpen(&dbname, 0)
	if err != nil {
		return fmt.Errorf("dbi: %s", err)
	}

	// Create a cursor.
	c, err := txn.CursorOpen(dbi)
	if err != nil {
		return fmt.Errorf("cursor: %s", err)
	}
	defer c.Close()

	// Loop over every object.
	var keySize, valueSize, count int
	for k, v, err := c.Get(nil, mdb.NEXT); err != mdb.NotFound; k, v, err = c.Get(nil, mdb.NEXT) {
		count++
		keySize += len(k)
		valueSize += len(v)
	}

	fmt.Printf("# FACTOR: %s ##############################\n", p.Name)
	fmt.Println("Total Size:", keySize+valueSize)
	fmt.Println("Count:", count)
	fmt.Println("Avg Bytes per Key:", keySize/count)
	fmt.Println("Avg Bytes per Value:", valueSize/count)
	fmt.Println("Avg Bytes per Item:", (keySize+valueSize)/count)
	fmt.Println("")

	return nil
}

func shardDBName(index int) string {
	return fmt.Sprintf("shards/%d", index)
}

func factorDBName(propertyID int) string {
	return fmt.Sprintf("factors/%d", propertyID)
}

type tableRawMessage struct {
	Name           string         `json:"name"`
	ShardCount     int            `json:"shardCount"`
	MaxPermanentID int            `json:"maxPermanentID"`
	MaxTransientID int            `json:"maxTransientID"`
	Properties     []*db.Property `json:"properties"`
}
