package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/skydb/sky/core"
)

// eventHandler handles the management of events in the database.
type eventHandler struct {
	s *Server
}

type objectEvents map[string][]*core.Event

// installEventHandler adds table routes to the server.
func installEventHandler(s *Server) *eventHandler {
	h := &eventHandler{s: s}

	s.HandleFunc("/tables/{table}/objects/{id}/events", EnsureTableHandler(HandleFunc(h.getEvents))).Methods("GET")
	s.HandleFunc("/tables/{table}/objects/{id}/events", EnsureTableHandler(HandleFunc(h.deleteEvents))).Methods("DELETE")

	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureTableHandler(HandleFunc(h.getEvent))).Methods("GET")
	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureMapHandler(EnsureTableHandler(HandleFunc(h.insertEvent)))).Methods("PUT", "PATCH")
	s.HandleFunc("/tables/{table}/objects/{id}/events/{timestamp}", EnsureTableHandler(HandleFunc(h.deleteEvent))).Methods("DELETE")

	s.Router.HandleFunc("/events", h.insertEventStream).Methods("PATCH")
	s.Router.HandleFunc("/tables/{table}/events", h.insertEventStream).Methods("PATCH")

	return h
}

// getEvents retrieves a list of all events associated with an object.
func (h *eventHandler) getEvents(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	events, err := s.db.GetEvents(t.Name, req.Var("id"))
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		f, err := s.db.Factorizer(t.Name)
		if err != nil {
			return nil, err
		}
		if err := f.DefactorizeEvent(event, t.PropertyFile()); err != nil {
			return nil, err
		}
	}
	return t.SerializeEvents(events)
}

// deleteEvents deletes all events associated with an object.
func (h *eventHandler) deleteEvents(s *Server, req Request) (interface{}, error) {
	return nil, s.db.DeleteObject(req.Table().Name, req.Var("id"))
}

// getEvent retrieves a single event for an object at a given point in time.
func (h *eventHandler) getEvent(s *Server, req Request) (interface{}, error) {
	timestamp, err := time.Parse(time.RFC3339, req.Var("timestamp"))
	if err != nil {
		return nil, fmt.Errorf("server: invalid timestamp: %s", req.Var("timestamp"))
	}

	// Find event.
	t := req.Table()
	event, err := s.db.GetEvent(t.Name, req.Var("id"), timestamp)
	if err != nil {
		return nil, err
	} else if event == nil {
		event = core.NewEvent(req.Var("timestamp"), map[int64]interface{}{})
	}

	// Convert an event to a serializable object.
	f, err := s.db.Factorizer(t.Name)
	if err != nil {
		return nil, err
	}
	if err := f.DefactorizeEvent(event, t.PropertyFile()); err != nil {
		return nil, err
	}
	return t.SerializeEvent(event)
}

// insertEvent adds a single event to an object.
func (h *eventHandler) insertEvent(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	data := req.Data().(map[string]interface{})
	data["timestamp"] = req.Var("timestamp")

	event, err := t.DeserializeEvent(data)
	if err != nil {
		return nil, err
	}

	f, err := s.db.Factorizer(t.Name)
	if err != nil {
		return nil, err
	}
	if err = f.FactorizeEvent(event, t.PropertyFile(), true); err != nil {
		return nil, err
	}
	return nil, s.db.InsertEvent(t.Name, req.Var("id"), event)
}

// deleteEvent deletes a single event for an object at a given point in time.
func (h *eventHandler) deleteEvent(s *Server, req Request) (interface{}, error) {
	timestamp, err := time.Parse(time.RFC3339, req.Var("timestamp"))
	if err != nil {
		return nil, fmt.Errorf("server: invalid timestamp: %s", req.Var("timestamp"))
	}

	t := req.Table()
	return nil, s.db.DeleteEvent(t.Name, req.Var("id"), timestamp)
}

// insertEventStream is a bulk insertion end point.
func (h *eventHandler) insertEventStream(w http.ResponseWriter, req *http.Request) {
	s := h.s
	vars := mux.Vars(req)
	t0 := time.Now()

	var table *core.Table
	tableName := vars["table"]
	if tableName != "" {
		var err error
		table, err = s.OpenTable(tableName)
		if err != nil {
			s.logger.Printf("ERR %v", err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{"message":"%v"}`, err)
			return
		}
	}

	tableObjects := make(map[*core.Table]objectEvents)

	events_written := 0
	err := func() error {
		// Stream in JSON event objects.
		decoder := json.NewDecoder(req.Body)
		for {
			// Read in a JSON object.
			rawEvent := map[string]interface{}{}
			if err := decoder.Decode(&rawEvent); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("Malformed json event: %v", err)
			}

			// Extract table name, if necessary.
			var eventTable *core.Table
			if table == nil {
				tableName, ok := rawEvent["table"].(string)
				if !ok {
					return fmt.Errorf("Table name required within event when using generic event stream.")
				}
				var err error
				eventTable, err = s.OpenTable(tableName)
				if err != nil {
					s.logger.Printf("ERR %v", err)
					fmt.Fprintf(w, `{"message":"%v"}`, err)
					return fmt.Errorf("Cannot open table %s: %+v", tableName, err)
				}
				delete(rawEvent, "table")
			} else {
				eventTable = table
			}

			// Extract the object identifier.
			objectId, ok := rawEvent["id"].(string)
			if !ok {
				return fmt.Errorf("Object identifier required")
			}

			// Convert to a Sky event and insert.
			event, err := eventTable.DeserializeEvent(rawEvent)
			if err != nil {
				return fmt.Errorf("Cannot deserialize: %v", err)
			}

			f, err := s.db.Factorizer(eventTable.Name)
			if err != nil {
				return err
			}
			if err := f.FactorizeEvent(event, eventTable.PropertyFile(), true); err != nil {
				return fmt.Errorf("Cannot factorize: %v", err)
			}

			if _, ok := tableObjects[eventTable]; !ok {
				tableObjects[eventTable] = make(objectEvents)
			}
			tableObjects[eventTable][objectId] = append(tableObjects[eventTable][objectId], event)
		}

		return nil
	}()

	if err == nil {
		err = func() error {
			for table, objects := range tableObjects {
				count, err := s.db.InsertObjects(table.Name, objects)
				if err != nil {
					return fmt.Errorf("Cannot put event: %v", err)
				}
				events_written += count
			}
			return nil
		}()
	}

	if err != nil {
		s.logger.Printf("ERR %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message":"%v", "events_written":%v}`, err, events_written)
		return
	}

	fmt.Fprintf(w, `{"events_written":%v}`, events_written)

	s.logger.Printf("%s \"%s %s %s %d events OK\" %0.3f", req.RemoteAddr, req.Method, req.URL.Path, req.Proto, events_written, time.Since(t0).Seconds())
}
