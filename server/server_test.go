package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRestartFollower(t *testing.T) {
	assert.Nil(t, nil)
}

func TestRestartLeader(t *testing.T) {
	// TODO:
}

func TestMarkLeaderAsRemoving(t *testing.T) {
	// TODO:
}

func TestMarkFollowerAsRemoving(t *testing.T) {
	// TODO:
}

func TestTransferLeaderWhileReplicaNotReady(t *testing.T) {
	// TODO: test transfer leader while replica is restarting and not catchuped fully.
	// should only transfer leader when replica has almost the newest raft logs
}
func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// TODO:
	// should not mark as remove while there is not enough for replica
}

func TestRestartWithCleanData(t *testing.T) {
	// TODO:
}

func TestRestartWithMigrate(t *testing.T) {
	// TODO:
	// stop for a while and wait the data migrate to others
	// and then start this node to join the cluster and wait
	// data migrate back to this node
}

func TestRestartCluster(t *testing.T) {
	// TODO:
	// stop all nodes in cluster and start one by one
}
