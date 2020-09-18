FROM busybox

RUN mkdir -p /data/logs/zankv/ && yum install -y rsync
ADD dist/docker/bin/ /opt/zankv/bin/
ADD script/ /opt/zankv/script/

EXPOSE 18001 12380 12381 12379

VOLUME /data
