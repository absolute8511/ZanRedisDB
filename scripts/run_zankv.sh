#!/bin/bash
exec 2>&1
sleep 1
rsync --daemon --config=/opt/zankv/conf/rsyncd.conf
exec /opt/zankv/bin/zankv -config /opt/zankv/conf/zankv.conf