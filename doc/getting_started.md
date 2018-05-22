# Getting Started

## Build

Install the compress library
<pre>
yum install snappy-devel (for CentOS)
apt-get install libsnappy1 libsnappy-dev (for Debian/Ubuntu)
brew install snappy (for Mac)
</pre>

Build the rocksdb 
<pre>
git clone https://github.com/absolute8511/rocksdb.git
cd rocksdb
git checkout v5.8.8-share-rate-limiter
make static_lib
</pre>

Install the dependency:
<pre>
CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lsnappy -lrt" go get github.com/youzan/gorocksdb

CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lsnappy" go get github.com/youzan/gorocksdb (for MacOS)
</pre>

use the `dep ensure` to install other dependencies

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

  Example config for rsync as below, and start rsync as daemon using `sudo rsync --daemon` 
```
pid file = /var/run/rsyncd.pid
port = 873
log file = /var/log/rsync.log
list = yes
hosts allow= *
max connections = 1024
log format = %t %a %m %f %b
syslog facility = local3
timeout = 300

# module name must be exactly same as the data_rsync_module in zankv
[zankv]
## this path must be exactly same as the data_dir in zankv node
path = /data/zankv/
read only = yes
list=yes
log file = /data/logs/rsyncd/zankv.log
exclude = /data/zankv/myid myid /zankv/myid
```

* Deploy etcd cluster which is needed for the meta data for the namespaces

  Please refer the etcd documents.

* Deploy the placedriver which is used for data placement: `placedriver -config=/path/to/config`

  Example config as below:
```
## <addr>:<port> to listen on for HTTP clients
http_address = "0.0.0.0:13801"

## the network interface for broadcast, the ip will be detected automatically.
broadcast_interface = "eth0"

cluster_id = "test-default"
## the etcd cluster ip list
cluster_leadership_addresses = "http://127.0.0.1:2379,http://127.0.0.2:2379"

## the detail of the log, larger number means more details
log_level = 2

## if empty, use the default flag value in glog
log_dir = "/data/logs/zankv"

## the time period (in hour) that the balance is allowed.
balance_interval = ["1", "23"]
auto_balance_and_migrate = true
```

* Deploy the zankv for data storage server `zankv -config=/path/to/config`

  Example config for zankv as below:
```
{
    "server_conf": {
        "broadcast_interface":"eth0",
        "cluster_id":"test-default",
        "etcd_cluster_addresses":"http://127.0.0.1:2379,http://127.0.0.2:2379",
        "data_dir":"/data/zankv",
        "data_rsync_module":"zankv",
        "redis_api_port": 13381,
        "http_api_port": 13380,
        "grpc_api_port": 13382,
        "election_tick": 30,
        "tick_ms": 200,
        "local_raft_addr": "http://0.0.0.0:13379",
        "rocksdb_opts": {
            "block_cache":0,
            "use_shared_cache":true,
            "use_shared_rate_limiter":true,
            "rate_bytes_per_sec":50000000,
            "cache_index_and_filter_blocks": false
        }
    }
}
```

## API

placedriver has several HTTP APIs to manager the namespace

- list the namespace: `GET /namespaces`
- list the data nodes: `GET /datanodes`
- list the placedriver nodes: `GET /listpd`
- query the namespace meta info: `GET /query/namespace_name`
- create the namespace (handle only by leader) : `POST /cluster/namespace/create?namespace=test_p16&partition_num=16&replicator=3`
- delete the namespace (handle only by leader): `POST /cluster/namespace/delete?namespace=test_p16&partition=**`

storage server HTTP APIs for stats:

- namespace stats : `GET /stats`
- namespace raft internal stats : `GET /raft/stats`
- optimize the data storage : `POST /kv/optimize`
- get the raft leader of the namespace partition: `GET /cluster/leader/namespace-partition`

storage server also support the redis apis for read/write :

* KV:
* Hash Set:
* List:
* Sorted Set:
* ZSet:

## Client
Golang client SDK : [client-sdk] , a redis proxy can be deployed 
based on this golang sdk if you want use the redis client in other language.


[client-sdk]: https://github.com/youzan/go-zanredisdb
