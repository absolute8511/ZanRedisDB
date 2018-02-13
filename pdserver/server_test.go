package pdserver

import (
	"testing"
)

func TestClusterBalanceAcrossMultiDC(t *testing.T) {
	// TODO:
}

func TestClusterRemoveNode(t *testing.T) {
	// TODO:
}
func TestMarkLeaderAsRemoving(t *testing.T) {
	// migrate raft node to another and mark leader as removing
	// should wait leader transfer before propose remove node
}

func TestMarkFollowerAsRemoving(t *testing.T) {
	// TODO:
	// migrate raft node to another and mark non-leader as removing
	// should propose remove node after raft all ready
}

func TestTransferLeaderWhileReplicaNotReady(t *testing.T) {
	// TODO: test transfer leader while replica is restarting and not catchuped fully.
	// should only transfer leader when replica has almost the newest raft logs
}

func TestMarkAsRemovingWhileNotEnoughAlives(t *testing.T) {
	// TODO:
	// should not mark as remove while there is not enough for replica
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

func TestRestartWithForceAlone(t *testing.T) {
	// TODO: test force restart with alone
}

func TestInstallSnapshotFailed(t *testing.T) {
	// TODO: test the follower fall behind too much, and the leader send the snapshot to follower,
	// However, the follower failed to pull the snapshot data from leader. So the raft node should stop
	// and restart later.

	// test case should make sure the snap will be not persisted to the stable storage since the snapshot data is failed to pull.
}

func TestClusterBalanceWhileNewNodeAdd(t *testing.T) {
	// TODO: while replica is not enough, we will add new replica node while check namespace,
	// and then it should wait the new replica is raft synced before we can start to balance
}

func TestClusterAddReplicaOneByOne(t *testing.T) {
	// TODO: while replica is not enough, we will add new replica node while check namespace.
	// If two replica are removed, we need add new replica one by one to avoid 2 failed node in raft.
}
