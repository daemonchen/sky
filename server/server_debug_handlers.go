package server

import (
	"expvar"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/pprof"
)

func (s *Server) addDebugHandlers() {
	s.router.Handle("/pprof/", http.HandlerFunc(pprof.Index))
	s.router.Handle("/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	s.router.Handle("/pprof/profile", http.HandlerFunc(pprof.Profile))
	s.router.Handle("/pprof/symbol", http.HandlerFunc(pprof.Symbol))

	s.router.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	s.router.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	s.router.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	s.router.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))

	s.router.HandleFunc("/debug/vars", getVarsHandler)
	s.router.HandleFunc("/debug/vars/{name}", getVarsHandler)
}

func getVarsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if vars["name"] == "" || vars["name"] == kv.Key {
			if !first {
				fmt.Fprintf(w, ",\n")
			}
			first = false
			fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
		}
	})
	fmt.Fprintf(w, "\n}\n")
}
