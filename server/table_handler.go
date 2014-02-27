package server

import (
	"io/ioutil"
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
	s.HandleFunc("/tables/{table}/stats", EnsureTableHandler(HandleFunc(h.stats))).Methods("GET")
	return h
}

// getTables retrieves metadata for all tables.
func (h *tableHandler) getTables(s *Server, req Request) (interface{}, error) {
	// Create a table object for each directory in the tables path.
	infos, err := ioutil.ReadDir(s.db.Path())
	if err != nil {
		return nil, err
	}

	tables := make([]*tableMessage, 0)
	for _, info := range infos {
		if info.IsDir() {
			tables = append(tables, &tableMessage{Name: info.Name()})
		}
	}

	return tables, nil
}

// getTable retrieves metadata for a single table.
func (h *tableHandler) getTable(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	return &tableMessage{Name: t.Name()}, nil
}

// createTable creates a new table.
func (h *tableHandler) createTable(s *Server, req Request) (interface{}, error) {
	data := req.Data().(map[string]interface{})
	name, _ := data["name"].(string)
	t, err := s.db.CreateTable(name, 0)
	if err != nil {
		return nil, err
	}
	return &tableMessage{Name: t.Name()}, nil
}

// deleteTable deletes a single table.
func (h *tableHandler) deleteTable(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	return nil, s.db.DropTable(t.Name())
}

// getKeys retrieves all object keys for a table.
func (h *tableHandler) getKeys(s *Server, req Request) (interface{}, error) {
	return req.Table().Keys()
}

// stats returns LMDB stats for a given table.
func (h *tableHandler) stats(s *Server, req Request) (interface{}, error) {
	return req.Table().Stat()
}


type tableMessage struct {
	Name string `json:"name"`
}
