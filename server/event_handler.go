package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
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

	s.Router.HandleFunc("/tables/{table}/events", h.insertEventStream).Methods("PATCH")

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

// insertEventStream is a bulk insertion end point for a table.
func (h *eventHandler) insertEventStream(w http.ResponseWriter, req *http.Request) {
	s := h.s
	vars := mux.Vars(req)

	// Open the requested table.
	t, err := s.db.OpenTable(vars["table"])
	if t == nil {
		h.Error(w, "table not found: "+vars["table"], http.StatusNotFound)
		return
	} else if err != nil {
		h.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Check for flush period passed as a URL param.
	flushPeriod := s.StreamFlushPeriod
	if value, err := strconv.Atoi(req.FormValue("flush-period")); value > 0 {
		flushPeriod = uint(value)
	} else if err != nil && req.FormValue("flush-period") != "" {
		h.Error(w, fmt.Sprintf("invalid flush-period: %s", req.FormValue("flush-period")), http.StatusBadRequest)
		return
	}

	// Check for flush threshold/buffer passed as a URL param.
	flushThreshold := s.StreamFlushThreshold
	if value, err := strconv.Atoi(req.FormValue("flush-threshold")); value > 0 {
		flushThreshold = uint(value)
	} else if err != nil && req.FormValue("flush-threshold") != "" {
		h.Error(w, fmt.Sprintf("invalid flush-threshold: %s", req.FormValue("flush-threshold")), http.StatusBadRequest)
		return
	}

	// Flush on a separate thread.
	var mutex sync.Mutex
	var flush chan bool
	var closeNotifier = w.(http.CloseNotifier).CloseNotify()
	var count = 0
	var startTime = time.Now()
	var events = make(map[string][]*db.Event)
	go func() {
		for {
			var closed = false
			select {
			case <-time.After(time.Duration(flushPeriod) * time.Second):
			case <-flush:
			case <-closeNotifier:
				closed = true
			}

			// Flush events.
			mutex.Lock()
			if err := t.InsertObjects(events); err != nil {
				h.Error(w, fmt.Sprintf("flush: %v: %d", err, count), http.StatusBadRequest)
				req.Body.Close()
				mutex.Unlock()
				return
			}
			events = make(map[string][]*db.Event)
			count = 0
			mutex.Unlock()

			if closed {
				return
			}
		}
	}()

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
		} else if message.ID == "" {
			h.Error(w, "object id required", http.StatusBadRequest)
			return
		}

		// Create the event and append.
		var event = &db.Event{Timestamp: message.Timestamp, Data: message.Data}
		mutex.Lock()
		events[message.ID] = append(events[message.ID], event)
		count++
		if count > int(flushThreshold) {
			flush <- true
		}
		mutex.Unlock()
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
