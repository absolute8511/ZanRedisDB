package server

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/node"
	"github.com/coreos/etcd/raft/raftpb"
)

func (self *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	switch {
	case r.Method == "GET":
		ns, realKey, err := common.ExtractNamesapce([]byte(key))
		if err != nil {
			http.Error(w, "Failed:"+err.Error(), http.StatusBadRequest)
			return
		}
		kv, ok := self.kvNodes[ns]
		if !ok || kv == nil {
			http.Error(w, "Namespace not found:"+ns, http.StatusNotFound)
			return
		}
		if v, err := kv.node.Lookup(realKey); err == nil {
			w.Write(v)
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		var m node.MemberInfo
		err = json.Unmarshal(data, &m)
		if err != nil {
			http.Error(w, "POST data decode failed", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: data,
		}
		self.ProposeConfChange(m.Namespace, cc)

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}
		q := r.URL.Query()
		ns := q.Get("ns")

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		self.ProposeConfChange(ns, cc)

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func (self *Server) serveHttpAPI(port int, stopC <-chan struct{}) {
	go http.ListenAndServe("localhost:6666", nil)
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: self,
	}
	l, err := common.NewStoppableListener(srv.Addr, stopC)
	if err != nil {
		panic(err)
	}
	err = srv.Serve(l)
	// exit when raft goes down
	log.Printf("http server stopped: %v", err)
}
