#!/bin/bash

# for centos, scl toolkit, gcc 4.9+, go 1.8+
#sudo yum install scl-utils
#sudo yum install snappy-devel zlib-devel bzip2-devel
#sudo rpm -ivh "https://www.softwarecollections.org/repos/rhscl/devtoolset-3/epel-6-x86_64/noarch/rhscl-devtoolset-3-epel-6-x86_64-1-2.noarch.rpm"
#sudo yum install devtoolset-3-gcc devtoolset-3-gcc-c++ devtoolset-3-gdb

echo `pwd`
GoDep=`go env GOPATH`/src/golang.org/x
mkdir -p $GoDep
if [ ! -d "$GoDep/net" ]; then
  pushd $GoDep && git clone https://github.com/golang/net.git && popd
fi
if [ ! -d "$GoDep/sys" ]; then
  pushd $GoDep && git clone https://github.com/golang/sys.git && popd
fi

googleDep=`go env GOPATH`/src/google.golang.org
mkdir -p $googleDep
if [ ! -d "$googleDep/grpc" ]; then
  pushd $googleDep && git clone https://github.com/grpc/grpc-go.git grpc && popd
fi
if [ ! -d "$googleDep/genproto" ]; then
  pushd $googleDep && git https://github.com/google/go-genproto clone genproto && popd
fi

go get -d github.com/youzan/ZanRedisDB/...
arch=$(go env GOARCH)
os=$(go env GOOS)
goversion=$(go version | awk '{print $3}')
LATEST="zankv-latest.$os-$arch.$goversion"

etcdurl=$ETCD_URL

scl enable devtoolset-3 bash

rocksdb=`pwd`/rocksdb
if [ ! -f "$rocksdb/Makefile" ]; then
  rm -rf $rocksdb
  git clone https://github.com/absolute8511/rocksdb.git $rocksdb
fi
pushd $rocksdb
git pull
git checkout v5.8.8-share-rate-limiter
CC=/opt/rh/devtoolset-3/root/usr/bin/gcc CXX=/opt/rh/devtoolset-3/root/usr/bin/g++ LD=/opt/rh/devtoolset-3/root/usr/bin/ld USE_SSE=1 make static_lib
popd

LD=/opt/rh/devtoolset-3/root/usr/bin/ld CGO_CFLAGS="-I$rocksdb/include" CGO_LDFLAGS="-L/opt/rh/devtoolset-3/root/usr/lib/gcc/x86_64-redhat-linux/4.9.2 -L$rocksdb -lrocksdb -lstdc++ -lm -lsnappy -lrt -lz -lbz2" go get -u github.com/youzan/gorocksdb

wget -c https://raw.githubusercontent.com/pote/gpm/v1.4.0/bin/gpm && chmod +x gpm
export PATH=`pwd`:$PATH

echo `pwd`
pushd `go env GOPATH`/src/github.com/youzan/ZanRedisDB/
git pull

./pre-dist.sh || true

## we also use gpm in ci because some dep can not be pulled since gfw.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOPATH=$DIR/.godeps gpm get || true

./dist.sh
popd

if [ ! -f "`pwd`/etcd-v2.3.8-linux-amd64/etcd" ] && [ -z "$etcdurl" ]; then
  rm -rf etcd-v2.3.8-linux-amd64
  wget -c https://github.com/coreos/etcd/releases/download/v2.3.8/etcd-v2.3.8-linux-amd64.tar.gz
  tar -xvzf etcd-v2.3.8-linux-amd64.tar.gz 
fi

if [ -z "$etcdurl" ]; then
  etcdurl="http://127.0.0.1:2379"
fi

echo $etcdurl
echo $ETCD_URL

cp -fp `go env GOPATH`/src/github.com/youzan/ZanRedisDB/dist/$LATEST.tar.gz .
killall zankv || true
killall placedriver || true
killall etcd || true
sleep 3

rm -rf zankv
rm -rf zankv-data
rm -rf test-etcd
rm -rf pd.config
rm -rf zankv.config
tar -xvzf $LATEST.tar.gz && mv $LATEST zankv

cat <<EOF > pd.config

http_address = "0.0.0.0:18001"
broadcast_interface = "eth0"
cluster_id = "jenkins-test-zanredis-deploy"

cluster_leadership_addresses = "$etcdurl"

log_level = 3

## if empty, use the default flag value in glog
log_dir = "./"

auto_balance_and_migrate = true
balance_interval = ["1", "23"]
EOF

cat <<EOF > zankv.config
{
    "server_conf": {
        "broadcast_interface":"eth0",
        "cluster_id":"jenkins-test-zanredis-deploy",
        "etcd_cluster_addresses":"$etcdurl",
        "data_dir":"./zankv-data",
        "data_rsync_module":"zankv",
        "redis_api_port": 12381,
        "http_api_port": 12380,
        "election_tick": 30,
        "tick_ms": 200,
        "local_raft_addr": "http://0.0.0.0:12379",
        "rocksdb_opts": {
            "block_cache":64000000,
            "write_buffer_size":32000000,
            "max_write_buffer_number":2,
            "cache_index_and_filter_blocks": true
        }
    }
}
EOF

mkdir zankv-data

if [ -z "$ETCD_URL" ]; then
  BUILD_ID=dontKillMe nohup ./etcd-v2.3.8-linux-amd64/etcd -name=test-etcd0 -initial-advertise-peer-urls=http://127.0.0.1:2380 -listen-client-urls=http://127.0.0.1:2379 -advertise-client-urls=http://127.0.0.1:2379 -listen-peer-urls=http://127.0.0.1:2380 -initial-cluster="test-etcd0=http://127.0.0.1:2380" -initial-cluster-state=new --data-dir ./test-etcd > etcd.log 2>&1 &
  sleep 3
fi

BUILD_ID=dontKillMe nohup ./zankv/bin/placedriver -config=./pd.config > pd.log 2>&1 &
sleep 3
BUILD_ID=dontKillMe nohup ./zankv/bin/zankv -config=./zankv.config > zankv.log 2>&1 &
sleep 3
