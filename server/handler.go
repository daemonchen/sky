package server

import (
	"fmt"
)

// Handler provides an interface for individual handler methods. These can be
// wrapped to provide filtering or other services.
type Handler interface {
	Serve(*Server, Request) (interface{}, error)
}

// HandleFunc is an adapter to allow plain functions to implement the Handler interface.
type HandleFunc func(*Server, Request) (interface{}, error)

// Serve calls f(req).
func (f HandleFunc) Serve(s *Server, req Request) (interface{}, error) {
	return f(s, req)
}

// Loads the requested table before passing off the handler.
func EnsureTableHandler(handler Handler) Handler {
	return &ensureTableHandler{handler}
}

type ensureTableHandler struct {
	handler Handler
}

func (h *ensureTableHandler) Serve(s *Server, req Request) (interface{}, error) {
	t, err := s.OpenTable(req.Var("table"))
	if err != nil {
		return nil, err
	}
	req.SetTable(t)
	return h.handler.Serve(s, req)
}

// Loads the requested table and property before passing off the handler.
func EnsurePropertyHandler(handler Handler) Handler {
	return EnsureTableHandler(&ensurePropertyHandler{handler})
}

type ensurePropertyHandler struct {
	handler Handler
}

func (h *ensurePropertyHandler) Serve(s *Server, req Request) (interface{}, error) {
	p, err := req.Table().Property(req.Var("property"))
	if err != nil {
		return nil, err
	} else if p == nil {
		return nil, fmt.Errorf("property not found: %s", req.Var("property"))
	}
	req.SetProperty(p)
	return h.handler.Serve(s, req)
}

// Checks that a map is passed into the body before passing off the handler.
func EnsureMapHandler(handler Handler) Handler {
	return &ensureMapHandler{handler}
}

type ensureMapHandler struct {
	handler Handler
}

func (h *ensureMapHandler) Serve(s *Server, req Request) (interface{}, error) {
	if _, ok := req.Data().(map[string]interface{}); !ok {
		return nil, fmt.Errorf("server: map input required")
	}
	return h.handler.Serve(s, req)
}
