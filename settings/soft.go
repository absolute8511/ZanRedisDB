package settings

import "runtime"

// Soft settings are some configurations than can be safely changed and
// the app need to be restarted to apply such configuration changes.
var Soft = getSoftSettings()

type soft struct {
	// test
	TestInt  uint64 `json:"test_int,omitempty"`
	TestBool bool   `json:"test_bool"`
	TestStr  string `json:"test_str"`
	// raft
	MaxCommittedSizePerReady uint64 `json:"max_committed_size_per_ready"`
	MaxSizePerMsg            uint64 `json:"max_size_per_msg"`
	MaxInflightMsgs          uint64 `json:"max_inflight_msgs"`
	DefaultSnapCount         uint64 `json:"default_snap_count"`
	MaxInFlightMsgSnap       uint64 `json:"max_in_flight_msg_snap"`
	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthIntervalSec uint64 `json:"health_interval_sec"`

	// transport

	// statemachine
	CommitBufferLen uint64 `json:"commit_buffer_len"`

	// server

	// raft proposal queue length for client queue loop (1024*4 for default, suggest use default)
	ProposalQueueLen uint64 `json:"proposal_queue_len"`
	// how many queues used for proposal, suggest use CPU nums
	ProposalQueueNum uint64 `json:"proposal_queue_num"`
}

func getSoftSettings() soft {
	d := defaultSoftSettings()
	overwriteSettingsWithFile(&d, "soft-settings.json")
	return d
}

func defaultSoftSettings() soft {
	return soft{
		MaxCommittedSizePerReady: 1024 * 1024 * 16,
		DefaultSnapCount:         160000,
		HealthIntervalSec:        5,
		// max number of in-flight snapshot messages allows to have
		MaxInFlightMsgSnap: 16,
		MaxSizePerMsg:      512 * 1024,
		MaxInflightMsgs:    256,
		CommitBufferLen:    1024 * 8,
		ProposalQueueLen:   1024 * 4,
		ProposalQueueNum:   uint64(runtime.NumCPU()),
	}
}
