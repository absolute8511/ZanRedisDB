#!/bin/bash
exec 2>&1
sleep 1
exec /opt/zankv/bin/zanpd -config /opt/zankv/conf/placedriver.conf