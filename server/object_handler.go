package server

// objectHandler handles the management of objects in the database.
type objectHandler struct{}

// installEventHandler adds table routes to the server.
func installObjectHandler(s *Server) *objectHandler {
	h := &objectHandler{}
	// s.HandleFunc("/tables/{table}/objects/{id}/merge", EnsureTableHandler(EnsureMapHandler(HandleFunc(h.mergeObjects)))).Methods("POST")
	return h
}

// mergeObjects merges the timeline of one object into the timeline of another.
/*
func (h *objectHandler) mergeObjects(s *Server, req Request) (interface{}, error) {
	t := req.Table()
	data := req.Data().(map[string]interface{})
	destId := req.Var("id")
	srcId, ok := data["id"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid source id: %v", data["id"])
	} else if destId == srcId {
		return nil, fmt.Errorf("cannot merge an object into itself")
	}

	return nil, t.Merge(destId, srcId)
}
*/
