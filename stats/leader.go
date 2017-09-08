// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"encoding/json"
	"math"
	"sync"
	"time"
)

// encapsulates
// statistics about communication with its peers
type PeersStats struct {
	// clarify that these are IDs, not names
	Peers map[string]*PeerStats `json:"peers"`

	sync.Mutex
}

func NewPeersStats() *PeersStats {
	return &PeersStats{
		Peers: make(map[string]*PeerStats),
	}
}

func (ls *PeersStats) JSON() []byte {
	ls.Lock()
	stats := PeersStats{
		Peers: ls.Peers,
	}
	ls.Unlock()
	b, _ := json.Marshal(stats)
	return b
}

func (ls *PeersStats) RemovePeer(name string) {
	ls.Lock()
	delete(ls.Peers, name)
	ls.Unlock()
}

func (ls *PeersStats) Peer(name string) *PeerStats {
	ls.Lock()
	defer ls.Unlock()
	fs, ok := ls.Peers[name]
	if !ok {
		fs = &PeerStats{}
		fs.Latency.Minimum = 1 << 63
		ls.Peers[name] = fs
	}
	return fs
}

// PeerStats encapsulates various statistics about a follower in an raft cluster
type PeerStats struct {
	Latency LatencyStats `json:"latency"`
	Counts  CountsStats  `json:"counts"`

	sync.Mutex
}

// LatencyStats encapsulates latency statistics.
type LatencyStats struct {
	Current           float64 `json:"current"`
	Average           float64 `json:"average"`
	averageSquare     float64
	StandardDeviation float64 `json:"standardDeviation"`
	Minimum           float64 `json:"minimum"`
	Maximum           float64 `json:"maximum"`
}

// CountsStats encapsulates raft statistics.
type CountsStats struct {
	Fail    uint64 `json:"fail"`
	Success uint64 `json:"success"`
}

// Succ updates the PeerStats with a successful send
func (fs *PeerStats) Succ(d time.Duration) {
	fs.Lock()
	defer fs.Unlock()

	total := float64(fs.Counts.Success) * fs.Latency.Average
	totalSquare := float64(fs.Counts.Success) * fs.Latency.averageSquare

	fs.Counts.Success++

	fs.Latency.Current = float64(d) / (1000000.0)

	if fs.Latency.Current > fs.Latency.Maximum {
		fs.Latency.Maximum = fs.Latency.Current
	}

	if fs.Latency.Current < fs.Latency.Minimum {
		fs.Latency.Minimum = fs.Latency.Current
	}

	fs.Latency.Average = (total + fs.Latency.Current) / float64(fs.Counts.Success)
	fs.Latency.averageSquare = (totalSquare + fs.Latency.Current*fs.Latency.Current) / float64(fs.Counts.Success)

	// sdv = sqrt(avg(x^2) - avg(x)^2)
	fs.Latency.StandardDeviation = math.Sqrt(fs.Latency.averageSquare - fs.Latency.Average*fs.Latency.Average)
}

// Fail updates the PeerStats with an unsuccessful send
func (fs *PeerStats) Fail() {
	fs.Lock()
	defer fs.Unlock()
	fs.Counts.Fail++
}
