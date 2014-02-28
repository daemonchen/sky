package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/skydb/sky/db"
)

// eventHandler handles the management of events in the database.
type eventHandler struct {
	s *Server
}

type objectEvents map[string][]*db.Event

// installEventHandler adds table routes to the server.
func installEventHandler(s *Server) *eventHandler {
	h := &eventHandler{s: s}

	s.HandleFunc("/tables/{table}/objects/{id}/events", EnsureTableHandler(HandleFunc(h.getEvents))).Methods("GET")
	s.HandleFunc("/tables/{table}/objects/{id}/events", EnsureTableHandler(HandleFunc(h.deleteEvents))).Methods("DELETE")

	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureTableHandler(HandleFunc(h.getEvent))).Methods("GET")
	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureMapHandler(EnsureTableHandler(HandleFunc(h.insertEvent)))).Methods("PUT", "PATCH")
	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureTableHandler(HandleFunc(h.deleteEvent))).Methods("DELETE")

	s.Router.HandleFunc("/events", h.insertGenericEventStream).Methods("PATCH")
	s.Router.HandleFunc("/tables/{table}/events", h.insertTableEventStream).Methods("PATCH")

	return h
}

// getEvents retrieves a list of all events associated with an object.
func (h *eventHandler) getEvents(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	return t.GetEvents(req.Var("id"))
}

// deleteEvents deletes all events associated with an object.
func (h *eventHandler) deleteEvents(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	return nil, t.DeleteEvents(req.Var("id"))
}

// getEvent retrieves a single event for an object at a given point in time.
func (h *eventHandler) getEvent(s *Server, req Request) (interface{}, error) {
	timestamp, err := db.ParseTime(req.Var("timestamp"))
	if err != nil {
		return nil, err
	}

	t := req.Table()
	return t.GetEvent(req.Var("id"), timestamp)
}

// insertEvent adds a single event to an object.
func (h *eventHandler) insertEvent(s *Server, req Request) (interface{}, error) {
	timestamp, err := db.ParseTime(req.Var("timestamp"))
	if err != nil {
		return nil, err
	}

	// Create the event.
	e := &db.Event{Timestamp: timestamp}
	if root, ok := req.Data().(map[string]interface{}); ok {
		e.Data, _ = root["data"].(map[string]interface{})
	}

	// Insert the event.
	t := req.Table()
	return nil, t.InsertEvent(req.Var("id"), e)
}

// deleteEvent deletes a single event for an object at a given point in time.
func (h *eventHandler) deleteEvent(s *Server, req Request) (interface{}, error) {
	timestamp, err := db.ParseTime(req.Var("timestamp"))
	if err != nil {
		return nil, err
	}

	t := req.Table()
	return nil, t.DeleteEvent(req.Var("id"), timestamp)
}

// insertEventStream is a bulk insertion end point for a single table.
func (h *eventHandler) insertTableEventStream(w http.ResponseWriter, req *http.Request) {
	s := h.s
	vars := mux.Vars(req)

	// Open the requested table.
	var t *db.Table
	if vars["table"] != "" {
		var err error
		t, err = s.db.OpenTable(vars["table"])
		if err != nil {
			h.Error(w, err.Error(), http.StatusNotFound)
			return
		}
	}

	h.insertEventStream(w, req, t)
}

// insertGenericEventStream is a bulk insertion end point for multiple tables.
func (h *eventHandler) insertGenericEventStream(w http.ResponseWriter, req *http.Request) {
	h.insertEventStream(w, req, nil)
}

// insertEventStream is a bulk insertion end point.
func (h *eventHandler) insertEventStream(w http.ResponseWriter, req *http.Request, defaultTable *db.Table) {
	var count = 0
	var startTime = time.Now()

	// Stream in JSON event objects.
	var decoder = json.NewDecoder(req.Body)
	for {
		// Read in a JSON object.
		var message = new(eventMessage)
		if err := decoder.Decode(&message); err == io.EOF {
			break
		} else if err != nil {
			h.Error(w, fmt.Sprintf("%v: %d", err, count), http.StatusBadRequest)
			return
		}

		// Create the event.
		var event = &db.Event{Timestamp: message.Timestamp, Data: message.Data}

		// Find target table.
		var t = defaultTable
		if t == nil {
			var err error
			if t, err = h.s.db.OpenTable(message.Table); err != nil {
				h.Error(w, fmt.Sprintf("%v: %s: %d", err, message.Table, count), http.StatusBadRequest)
				return
			}
		}

		// Insert event.
		if err := t.InsertEvent(message.ID, event); err != nil {
			h.Error(w, fmt.Sprintf("%v: %s: %d", err, message.ID, count), http.StatusBadRequest)
			return
		}

		count++
	}

	// Write out total count.
	json.NewEncoder(w).Encode(map[string]interface{}{"count": count})

	// Log the total time.
	log.Printf("%s \"%s %s %s %d events OK\" %0.3f", req.RemoteAddr, req.Method, req.URL.Path, req.Proto, count, time.Since(startTime).Seconds())
}

// Error writes an error to the writer.
func (h *eventHandler) Error(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{"message": error})
}

type eventMessage struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Table     string                 `json:"table"`
	Data      map[string]interface{} `json:"data"`
}
