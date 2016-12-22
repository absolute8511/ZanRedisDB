# // start single node cluster
./ZanRedisDB --config=./default.conf --raftaddr="http://127.0.0.1:12379"
# // start two node cluster
./ZanRedisDB --config=./default.conf --raftaddr="http://127.0.0.1:12379"
./ZanRedisDB --config=./default2.conf --raftaddr="http://127.0.0.1:22379"
# // add new node to cluster  and start the new node and join cluster
curl -L http://127.0.0.1:12380/4 -XPOST -d "{\"id\":4, \"broadcast\":\"127.0.0.1\",\"namespace\":\"default\", \"peer_urls\":[\"http://127.0.0.1:42379\"], \"data_dir\":\"./test4\"}"
./ZanRedisDB --config=./default4.conf --raftaddr="http://127.0.0.1:42379" --join 

# // put key
curl -L http://127.0.0.1:12380/my-key -XPUT -d bar
# remove node from namespace in cluster
curl -X DELETE http://127.0.0.1:12380/4?ns=default
