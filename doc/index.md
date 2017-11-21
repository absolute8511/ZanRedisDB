# ZanRedisDB: redis-based distributed kv storage system

## Features

 * redis compatible
   * support KV, HASH, SET, ZSET, LIST, GEOHASH, HYPERLOGLOG
   * extend data such as JSON
 * fully distributed support with raft
 * all data goto disk, not limited by memory
 * secondary index
 * support deploy across data center

## Technical Specs
 * etcd raft
 * rocksdb as persistent storage
 * redis

## Roadmap
* Redis data structures
  - [x] KV
  - [x] Hash
  - [x] List
  - [x] Set
  - [x] Sorted Set
  - [x] GeoHash
  - [x] Expires
  - [x] HyperLogLog
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
