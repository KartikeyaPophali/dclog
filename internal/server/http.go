package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer accepts an address for the server to run on and returns an *http.Server
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceRequest contains the Record that should be appended to the Log.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse contains the Offset at which the new Record was saved in the Log.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest contains the Offset of the Record which is requested from the service.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse contains the requested Record that is to be returned by the service.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (hs *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := hs.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(ProduceResponse{Offset: offset})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (hs *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := hs.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(ConsumeResponse{Record: record})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
