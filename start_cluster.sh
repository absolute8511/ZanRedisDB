#!/bin/bash
# start single node cluster
./ZanRedisDB --config=./default.conf 
# start two node cluster
./ZanRedisDB --config=./default.conf 
./ZanRedisDB --config=./default2.conf
# add new node to cluster  and start the new node and join cluster
curl -L http://127.0.0.1:12380/cluster/node/add -XPOST -d "{\"id\":4, \"broadcast\":\"127.0.0.1\",
\"cluster_id\":1000, \"http_api_port\":12380,\"namespace\":\"default\", \"peer_urls\":[\"http://127.0.0.1:42379\"], \"data_dir\":\"./test4/default\"}"

./ZanRedisDB --config=./default4.conf 

# remove node from namespace in cluster
curl -X DELETE http://127.0.0.1:12380/cluster/node/remove/default/4
