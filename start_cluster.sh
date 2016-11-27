# // start single node cluster
./ZanRedisDB --id 1 --cluster="[{\"id\":1,\"addr\":\"http://127.0.0.1:12379\"}]" --port 12380 --redis_port 12381 -data="./test1"
# // start two node cluster
./ZanRedisDB --id 1 --raftaddr="http://127.0.0.1:12379" --cluster="[{\"id\":1,\"addr\":\"http://127.0.0.1:12379\"},{\"id\":2,\"addr\":\"http://127.0.0.1:22379\"}]" --port 12380 --redis_port 12381 -data="./test1"
./ZanRedisDB --id 2 --raftaddr="http://127.0.0.1:22379" --cluster="[{\"id\":1,\"addr\":\"http://127.0.0.1:12379\"},{\"id\":2,\"addr\":\"http://127.0.0.1:22379\"}]" --port 22380 --redis_port 22381 -data="./test2"
# // add new node to cluster  and start the new node and join cluster
curl -L http://127.0.0.1:12380/4 -XPOST -d "{\"id\":4, \"broadcast\":\"127.0.0.1\", \"peer_urls\":[\"http://127.0.0.1:42379\"], \"data_dir\":\"./test4\"}"
./ZanRedisDB --id 4 --raftaddr="http://127.0.0.1:42379" --port 42380 --redis_port 42381 -data="./test4" --join --cluster="[{\"id\":1,\"addr\":\"http://127.0.0.1:12379\"},{\"id\":2,\"addr\":\"http://127.0.0.1:22379\"}]"

# // put key
curl -L http://127.0.0.1:12380/my-key -XPUT -d bar
