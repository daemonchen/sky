package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"

	"github.com/gorilla/mux"
	"github.com/skydb/sky/db"
)

// The number of servlets created on startup defaults to the number
// of logical cores on a machine.
var defaultServletCount = runtime.NumCPU()

// Server is the HTTP transport used to connect to the databsae.
type Server struct {
	sync.Mutex
	*http.Server
	*mux.Router
	logger           *log.Logger
	db               *db.DB
	path             string
	listener         net.Listener
	tables           map[string]*db.Table
	shutdownChannel  chan bool
	shutdownFinished chan bool
	NoSync           bool
	MaxDBs           uint
	MaxReaders       uint
	Version          string
}

// NewServer creates a new Server instance.
func NewServer(port uint, path string) *Server {
	s := &Server{
		Server:     &http.Server{Addr: fmt.Sprintf(":%d", port)},
		Router:     mux.NewRouter(),
		logger:     log.New(os.Stdout, "", log.LstdFlags),
		path:       path,
		tables:     make(map[string]*db.Table),
		NoSync:     false,
		MaxDBs:     4096,
		MaxReaders: 126,
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

// The path to the table metadata directory.
func (s *Server) TablesPath() string {
	return fmt.Sprintf("%v/tables", s.path)
}

// Generates the path for a table attached to the server.
func (s *Server) TablePath(name string) string {
	return fmt.Sprintf("%v/%v", s.TablesPath(), name)
}

// Runs the server.
func (s *Server) ListenAndServe(shutdownChannel chan bool) error {
	s.shutdownChannel = shutdownChannel

	err := s.open()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		s.close()
		return err
	}
	s.listener = listener

	s.shutdownFinished = make(chan bool)
	go func() {
		s.Serve(s.listener)
		s.shutdownFinished <- true
	}()

	s.logger.Printf("Sky v%s is now listening on http://localhost%s\n", s.Version, s.Addr)

	return nil
}

// Stops the server.
func (s *Server) Shutdown() error {
	// Close servlets.
	s.close()

	// Close socket.
	if s.listener != nil {
		// Then stop the server.
		err := s.listener.Close()
		s.listener = nil
		if err != nil {
			return err
		}
	}
	// wait for server goroutine to finish
	<-s.shutdownFinished

	// Notify that the server is shutdown.
	if s.shutdownChannel != nil {
		s.shutdownChannel <- true
	}

	return nil
}

// Checks if the server is listening for new connections.
func (s *Server) Running() bool {
	return (s.listener != nil)
}

// Opens the data directory and servlets.
func (s *Server) open() error {
	s.close()

	// Initialize and open database.
	s.db = &db.DB{}
	if err := s.db.Open(s.path); err != nil {
		s.close()
		return err
	}

	return nil
}

// Closes the database.
func (s *Server) close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

// Silences the log.
func (s *Server) Silence() {
	s.logger = log.New(ioutil.Discard, "", log.LstdFlags)
}

// HandleFunc serializes and deserializes incoming requests before passing off to Gorilla.
func (s *Server) HandleFunc(path string, h Handler) *mux.Route {
	return s.Router.Handle(path, &httpHandler{s, h})
}
