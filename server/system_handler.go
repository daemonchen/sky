package server

// systemHandler handles the miscellaneous system-level handlers.
type systemHandler struct{}

// installSystemHandler adds table routes to the server.
func installSystemHandler(s *Server) *systemHandler {
	h := &systemHandler{}
	s.HandleFunc("/", HandleFunc(h.root)).Methods("GET")
	s.HandleFunc("/ping", HandleFunc(h.ping)).Methods("GET")
	return h
}

// root returns a welcome message and the server version.
func (h *systemHandler) root(s *Server, req Request) (interface{}, error) {
	return map[string]interface{}{"sky": "welcome", "version": s.Version}, nil
}

// ping returns a simple ok message.
func (h *systemHandler) ping(s *Server, req Request) (interface{}, error) {
	return map[string]interface{}{"message": "ok"}, nil
}

