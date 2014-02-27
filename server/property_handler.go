package server

import (
	"github.com/skydb/sky/db"
)

// propertyHandler handles the management of tables in the database.
type propertyHandler struct{}

// installPropertyHandler adds table routes to the server.
func installPropertyHandler(s *Server) *propertyHandler {
	h := &propertyHandler{}
	s.HandleFunc("/tables/{table}/properties", EnsureTableHandler(HandleFunc(h.getProperties))).Methods("GET")
	s.HandleFunc("/tables/{table}/properties", EnsureTableHandler(EnsureMapHandler(HandleFunc(h.createProperty)))).Methods("POST")
	s.HandleFunc("/tables/{table}/properties/{property}", EnsurePropertyHandler(HandleFunc(h.getProperty))).Methods("GET")
	s.HandleFunc("/tables/{table}/properties/{property}", EnsurePropertyHandler(EnsureMapHandler(HandleFunc(h.updateProperty)))).Methods("PATCH")
	s.HandleFunc("/tables/{table}/properties/{property}", EnsurePropertyHandler(HandleFunc(h.deleteProperty))).Methods("DELETE")
	return h
}

// getProperties retrieves all properties from a table.
func (h *propertyHandler) getProperties(s *Server, req Request) (interface{}, error) {
	properties, err := req.Table().Properties()
	if err != nil {
		return nil, err
	}

	var slice = make([]*db.Property, 0, len(properties))
	for _, p := range properties {
		slice = append(slice, p)
	}
	return slice, nil
}

// createProperty creates a new property on a table.
func (h *propertyHandler) createProperty(s *Server, req Request) (interface{}, error) {
	data := req.Data().(map[string]interface{})
	name, _ := data["name"].(string)
	transient, _ := data["transient"].(bool)
	dataType, _ := data["dataType"].(string)
	return req.Table().CreateProperty(name, dataType, transient)
}

// getProperty retrieves a property from a table by name.
func (h *propertyHandler) getProperty(s *Server, req Request) (interface{}, error) {
	return req.Property(), nil
}

// updateProperty updates a property on a table.
func (h *propertyHandler) updateProperty(s *Server, req Request) (interface{}, error) {
	data := req.Data().(map[string]interface{})
	return req.Table().RenameProperty(req.Property().Name, data["name"].(string))
}

// deleteProperty removes a property from a table.
func (h *propertyHandler) deleteProperty(s *Server, req Request) (interface{}, error) {
	return nil, req.Table().DeleteProperty(req.Property().Name)
}
