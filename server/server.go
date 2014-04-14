package server

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/skydb/sky/db"
)

// Server is the HTTP transport used to connect to the databsae.
type Server struct {
	sync.Mutex
	*http.Server
	*mux.Router
	db                   *db.DB
	path                 string
	listener             net.Listener
	StreamFlushPeriod    uint
	StreamFlushThreshold uint
	Version              string
}

// NewServer creates a new Server instance.
func NewServer(port uint, path string) *Server {
	s := &Server{
		Server:               &http.Server{Addr: fmt.Sprintf(":%d", port)},
		Router:               mux.NewRouter(),
		path:                 path,
		StreamFlushPeriod:    60, // seconds
		StreamFlushThreshold: 1000,
	}
	s.Handler = s

	installTableHandler(s)
	installPropertyHandler(s)
	installEventHandler(s)
	installQueryHandler(s)
	installObjectHandler(s)
	installSystemHandler(s)
	installDebugHandler(s)

	return s
}

// The root server path.
func (s *Server) Path() string {
	return s.path
}

// ListenAndServe starts the server and listens on the appropriate port.
func (s *Server) ListenAndServe() error {
	defer s.Close()

	// Initialize and open database.
	s.db = &db.DB{}
	if err := s.db.Open(s.path); err != nil {
		return err
	}

	// Initialize the TCP listener and save the reference.
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.listener = listener

	// Listen and block until a signal is received.
	return s.Server.Serve(s.listener)
}

// Close closes the port and shuts down the database.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}

	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

// HandleFunc serializes and deserializes incoming requests before passing off to Gorilla.
func (s *Server) HandleFunc(path string, h Handler) *mux.Route {
	return s.Router.Handle(path, &httpHandler{s, h})
}
