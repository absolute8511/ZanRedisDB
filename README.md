# ZanRedisDB

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/3288ed77f27c4f8a998e35ef936edc6f)](https://www.codacy.com/app/cool8511/ZanRedisDB?utm_source=github.com&utm_medium=referral&utm_content=absolute8511/ZanRedisDB&utm_campaign=badger)
[![Build Status](https://travis-ci.org/absolute8511/ZanRedisDB.svg?branch=master)](https://travis-ci.org/absolute8511/ZanRedisDB) [![GitHub release](https://img.shields.io/github/release/absolute8511/ZanRedisDB.svg)](https://github.com/absolute8511/ZanRedisDB/releases/latest) [![codecov](https://codecov.io/gh/absolute8511/ZanRedisDB/branch/master/graph/badge.svg)](https://codecov.io/gh/absolute8511/ZanRedisDB) [![Go Report Card](https://goreportcard.com/badge/github.com/absolute8511/ZanRedisDB)](https://goreportcard.com/report/github.com/absolute8511/ZanRedisDB)

## Build

Install the dependency:
<pre>
go get github.com/absolute8511/c-rocksdb
go get github.com/absolute8511/gorocksdb
go get github.com/cockroachdb/c-lz4
go get github.com/cockroachdb/c-snappy
</pre>

use the `gpm` to install other dependencies
<pre>
wget https://raw.githubusercontent.com/pote/gpm/v1.4.0/bin/gpm && chmod +x gpm && sudo mv gpm /usr/local/bin
gpm get
</pre>

Install the Snappy library
<pre>
yum install snappy-devel (for CentOS)
apt-get install libsnappy-dev (for Debian/Ubuntu)
brew install snappy (for Mac)
</pre>

Build zankv and placedriver from the source (only support go version 1.7.4+, gcc 4.9+ or xcode-command-line-tools on Mac):
<pre>
make
</pre>

If you want package the binary release run the scripts
<pre>
./pre-dist.sh
./dist.sh
</pre>

## Deploy

 * Deploy the rsync daemon which is needed on all server node to transfer the snapshot data for raft
 * Deploy etcd cluster which is needed for the meta data for the namespaces
 * Deploy the placedriver which is used for data placement: `placedriver -config=/path/to/config`
 * Deploy the zankv for data storage server `zankv -config=/path/to/config`

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

## Client
Golang client SDK : [client-sdk] , a redis proxy can be deployed 
based on this golang sdk if you want use the redis client in other language.

## Roadmap
* Redis data structures
  - [x] KV
  - [x] Hash
  - [x] List
  - [x] Set
  - [x] Sorted Set
  - [ ] Geo
  - [x] Expires
  - [ ] HyperLogLog
  - [x] JSON
* Distributed system
  - [x] Raft based replication
  - [x] Partitions
  - [x] Auto balance and migrate
  - [x] Support namespace
  - [x] High available
  - [x] Distributed scan on table
* Searchable and Indexing
  - [ ] Secondary index support on Hash fields
  - [ ] Secondary index support for json kv
  - [ ] Full text search support
* Operation
  - [x] Backup and restore for cluster
  - [ ] More stats for read/write performance and errors.
* Client 
  - [x] High available for redis commands (Retry on fail)
  - [ ] Extand redis commands to support index and search
  - [x] Extand redis commands for advance scan
* Others (maybe)
  - [ ] Support configure for Column storage friendly for OLAP
  - [ ] BoltDB as storage engine (read/range optimize)
  - [ ] Lua scripts support
  - [ ] Support export data to other systems


[client-sdk]: https://github.com/absolute8511/go-zanredisdb
