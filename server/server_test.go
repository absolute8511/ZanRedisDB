package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRestartFollower(t *testing.T) {
	assert.Nil(t, nil)
}

func TestRestartLeader(t *testing.T) {
}

func TestMarkLeaderAsRemoving(t *testing.T) {
}

func TestMarkFollowerAsRemoving(t *testing.T) {
}

func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// should not mark as remove while there is not enough for replica
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
