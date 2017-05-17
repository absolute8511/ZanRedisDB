package server

import (
	"testing"
)

func TestRestartFollower(t *testing.T) {
}

func TestRestartLeader(t *testing.T) {
}

func TestMarkLeaderAsRemoving(t *testing.T) {
}

func TestMarkFollowerAsRemoving(t *testing.T) {
}

func TestRestartWithCleanData(t *testing.T) {
}

func TestRestartWithMigrate(t *testing.T) {
	// stop for a while and wait the data migrate to others
	// and then start this node to join the cluster and wait
	// data migrate back to this node
}

func TestRestartCluster(t *testing.T) {
	// stop all nodes in cluster and start one by one
}
