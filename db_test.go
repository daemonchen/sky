package sky

import (
	"os"
	"ioutil"
	)
	

// Ensure that a database can be opened without error.
func TestOpen(t *testing.T) {
	f, _ := ioutil.TempFile("", "skydb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	db, err := Open(path)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	db.Close()
}

// Ensure that a database can be opened without error.
func TestInsert(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error{
			tx.InsertEvent(Event{timestamp: time.Now(), data: []byte("hello")})	
		}
	})
}


// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "skydb-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// withTempPath executes a function with a database reference.
func withTempPath(fn func(string)) {
	path := tempfile()
	defer os.RemoveAll(path)
	fn(path)
}

// withOpenDB executes a function with an already opened database.
func withOpenDB(fn func(*DB, string)) {
	withTempPath(func(path string) {
		db, err := Open(path, 0666)
		if err != nil {
			panic("cannot open db: " + err.Error())
		}
		defer db.Close()
		fn(db, path)
}
