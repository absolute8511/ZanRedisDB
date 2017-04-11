# ZanRedisDB

## Build
Install the dependency:
`go get github.com/absolute8511/c-rocksdb`
`go get github.com/absolute8511/gorocksdb`

Build the source (only support go version 1.7.4+):
`go build -tags=embed`

## Deploy

 * Deploy the rsync daemon which is needed on all server node to transfer the snapshot data for raft
 * Deploy etcd cluster which is needed for the meta data for the namespaces
 * Deploy the placedriver which is used for data placement
 * Deploy the ZanRedisDB for data storage server

## API
placedriver has several HTTP APIs to manager the namespace
 * list the namespace: `GET /namespaces`
 * list the data nodes: `GET /datanodes`
 * list the placedriver nodes: `GET /listpd`
 * query the namespace meta info: `GET /query/namespace_name`
 * create the namespace (handle only by leader) : `POST /cluster/namespace/create?namespace=test_p16&partition_num=16&replicator=3`
 * delete the namespace (handle only by leader): `POST /cluster/namespace/delete?namespace=test_p16&partition=**`

storage server HTTP APIs for stats:
 * namespace stats : `GET /stats`
 * namespace raft internal stats : `GET /raft/stats`
 * optimize the data storage : `POST /kv/optimize`
 * get the raft leader of the namespace partition: `GET /cluster/leader/namespace-partition`

storage server also support the redis apis for read/write :
 * KV:
 * Hash Set:
 * List:
 * Sorted Set:
 * ZSet:
