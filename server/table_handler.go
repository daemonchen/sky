package server

import (
	"fmt"
	"io/ioutil"
	"sort"

	"github.com/skydb/sky/core"
	"github.com/szferi/gomdb"
)

// tableHandler handles the management of tables in the database.
type tableHandler struct{}

// installTableHandler adds table routes to the server.
func installTableHandler(s *Server) *tableHandler {
	h := &tableHandler{}
	s.HandleFunc("/tables", HandleFunc(h.getTables)).Methods("GET")
	s.HandleFunc("/tables", EnsureMapHandler(HandleFunc(h.createTable))).Methods("POST")
	s.HandleFunc("/tables/{table}", EnsureTableHandler(HandleFunc(h.getTable))).Methods("GET")
	s.HandleFunc("/tables/{table}", EnsureTableHandler(HandleFunc(h.deleteTable))).Methods("DELETE")
	s.HandleFunc("/tables/{table}/keys", EnsureTableHandler(HandleFunc(h.getKeys))).Methods("GET")
	return h
}

// getTables retrieves metadata for all tables.
func (h *tableHandler) getTables(s *Server, req Request) (interface{}, error) {
	// Create a table object for each directory in the tables path.
	infos, err := ioutil.ReadDir(s.TablesPath())
	if err != nil {
		return nil, err
	}

	tables := []*core.Table{}
	for _, info := range infos {
		if info.IsDir() {
			tables = append(tables, &core.Table{Name: info.Name()})
		}
	}

	return tables, nil
}

// getTable retrieves metadata for a single table.
func (h *tableHandler) getTable(s *Server, req Request) (interface{}, error) {
	return req.Table(), nil
}

// createTable creates a new table.
func (h *tableHandler) createTable(s *Server, req Request) (interface{}, error) {
	data := req.Data().(map[string]interface{})
	name, _ := data["name"].(string)
	if table, _ := s.OpenTable(name); table != nil {
		return nil, fmt.Errorf("server: table already exists: %s", name)
	}

	t := &core.Table{Name: name}
	if err := t.Create(s.TablePath(name)); err != nil {
		return nil, err
	}
	return t, nil
}

// deleteTable deletes a single table.
func (h *tableHandler) deleteTable(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	if err := s.db.Drop(t.Name); err != nil {
		return nil, err
	}

	// Remove the table from the lookup and remove it's core.
	s.Lock()
	delete(s.tables, t.Name)
	defer s.Unlock()

	return nil, t.Delete()
}

// getKeys retrieves all object keys for a table.
func (h *tableHandler) getKeys(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	cursors, err := s.db.Cursors(t.Name)
	if err != nil {
		return nil, err
	}
	defer cursors.Close()

	keys := []string{}
	for _, c := range cursors {
		for {
			// Retrieve main key value.
			bkey, _, err := c.Get(nil, mdb.NEXT_NODUP)
			if err != nil {
				break
			}
			keys = append(keys, string(bkey))
		}
	}
	sort.Strings(keys)

	return keys, nil
}
