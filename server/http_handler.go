package server

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
)

// httpHandler abstracts the HTTP interface from the server handler.
type httpHandler struct {
	server  *Server
	handler Handler
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// warn("»", req.Method, req.URL.Path)

	response := new(response)

	// Log the response time.
	t0 := time.Now()

	// Parse incoming data by type.
	var request *request
	request, response.err = h.parseRequest(req)

	// Execute handler function.
	if response.err == nil {
		response.data, response.err = h.handler.Serve(h.server, request)
	}

	// Send back the response.
	h.writeResponse(w, req, response, time.Since(t0).Seconds())
}

// parseRequest converts the HTTP request to a Sky server request.
func (h *httpHandler) parseRequest(req *http.Request) (*request, error) {
	contentType := req.Header.Get("Content-Type")
	switch contentType {
	case "application/json":
		return h.parseJSONRequest(req)
	default:
		return h.parseTextRequest(req)
	}
}

// parseJSONRequest converts a JSON HTTP request to a Sky server request.
func (h *httpHandler) parseJSONRequest(req *http.Request) (*request, error) {
	var data map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&data); err != nil && err != io.EOF {
		return nil, fmt.Errorf("server: json request error: %v", err)
	}
	return &request{vars: h.readVars(req), data: data}, nil
}

// parseTextRequest converts a Text HTTP request to a Sky server request.
func (h *httpHandler) parseTextRequest(req *http.Request) (*request, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("server: text request error: %v", err)
	}
	return &request{vars: h.readVars(req), data: data}, nil
}

// writeResponse writes the Sky server response to the HTTP response writer.
func (h *httpHandler) writeResponse(w http.ResponseWriter, req *http.Request, resp Response, elapsedTime float64) {
	// contentType := req.Header.Get("Accept")

	// Write header.
	var status int
	if resp.Error() == nil {
		status = http.StatusOK
	} else {
		status = http.StatusInternalServerError
		h.server.logger.Printf("[error] %v", resp.Error())
	}

	// Log request.
	h.server.logger.Printf("%s \"%s %s %s\" %d %0.3f", req.RemoteAddr, req.Method, req.RequestURI, req.Proto, status, elapsedTime)

	// Write header and body.
	w.WriteHeader(status)
	h.writeJSONResponse(w, resp)
}

// writeJSONResponse writes the Sky server response as JSON to the HTTP response writer.
func (h *httpHandler) writeJSONResponse(w http.ResponseWriter, resp Response) {
	w.Header().Set("Content-Type", "application/json")

	if resp.Error() != nil {
		ret := map[string]interface{}{"message": resp.Error().Error()}
		// json.NewEncoder(os.Stderr).Encode(ret)
		if err := json.NewEncoder(w).Encode(ret); err != nil {
			h.server.logger.Printf("server: encoding error[err]: %v", err)
		}
	} else if resp.Data() != nil {
		// json.NewEncoder(os.Stderr).Encode(resp.Data())
		if err := json.NewEncoder(w).Encode(resp.Data()); err != nil {
			h.server.logger.Printf("server: encoding error: %v", err)
		}
	}
}

// readVars reads a map of key/value pairs from the URL.
func (h *httpHandler) readVars(req *http.Request) map[string]string {
	vars := make(map[string]string)
	values, _ := url.ParseQuery(req.URL.RawQuery)
	for k, _ := range values {
		vars[k] = values.Get(k)
	}
	for k, v := range mux.Vars(req) {
		vars[k] = v
	}
	return vars
}
