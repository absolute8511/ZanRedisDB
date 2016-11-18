#!/bin/bash
for ((i=0;i<12000;i++)); do
    curl -L http://127.0.0.1:12380/my-key-$i -XPUT -d bar-$i
done
