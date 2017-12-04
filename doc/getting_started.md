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
USE_SSE=1 make static_lib
</pre>

Install the dependency:
<pre>
CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lsnappy -lrt" go get github.com/absolute8511/gorocksdb

CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lsnappy" go get github.com/absolute8511/gorocksdb (for MacOS)
</pre>

use the `gpm` to install other dependencies
<pre>
wget https://raw.githubusercontent.com/pote/gpm/v1.4.0/bin/gpm && chmod +x gpm && sudo mv gpm /usr/local/bin
gpm get
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


[client-sdk]: https://github.com/absolute8511/go-zanredisdb
