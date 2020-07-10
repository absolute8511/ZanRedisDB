package datanode_coord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/cluster"
)

var testEtcdServers = "http://127.0.0.1:2379"

func TestDataCoordKV(t *testing.T) {
	ChangeIntervalForTest()
	cluster.SetLogger(0, nil)
	ninfo := &cluster.NodeInfo{
		NodeIP:            "127.0.0.1",
		Hostname:          "localhost",
		RedisPort:         "1234",
		HttpPort:          "1235",
		RpcPort:           "1236",
		RaftTransportAddr: "127.0.0.1:2379",
	}
	r, err := cluster.NewDNEtcdRegister(testEtcdServers)
	assert.Nil(t, err)

	dc := NewDataCoordinator("unit-test-cluster", ninfo, nil)

	err = dc.SetRegister(r)
	assert.Nil(t, err)
	dc.Start()
	defer dc.Stop()

	v, err := dc.GetSyncerWriteOnly()
	assert.Equal(t, cluster.ErrKeyNotFound, err)
	err = dc.UpdateSyncerWriteOnly(true)
	assert.Nil(t, err)
	v, err = dc.GetSyncerWriteOnly()
	assert.Nil(t, err)
	assert.Equal(t, true, v)

	ninfo2 := &cluster.NodeInfo{
		NodeIP:            "127.0.0.1",
		Hostname:          "localhost",
		RedisPort:         "2234",
		HttpPort:          "2235",
		RpcPort:           "2236",
		RaftTransportAddr: "127.0.0.1:2379",
	}
	dc2 := NewDataCoordinator("unit-test-cluster", ninfo2, nil)

	r2, err := cluster.NewDNEtcdRegister(testEtcdServers)
	assert.Nil(t, err)
	err = dc2.SetRegister(r2)
	assert.Nil(t, err)
	dc2.Start()
	defer dc2.Stop()
	_, err = dc2.GetSyncerWriteOnly()
	assert.Equal(t, cluster.ErrKeyNotFound, err)

	err = dc2.UpdateSyncerWriteOnly(false)
	assert.Nil(t, err)
	v2, err := dc2.GetSyncerWriteOnly()
	assert.Nil(t, err)
	assert.Equal(t, false, v2)

	// should not affect the node1
	v, err = dc.GetSyncerWriteOnly()
	assert.Nil(t, err)
	assert.Equal(t, true, v)
}
