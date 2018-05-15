package node

func (kvsm *kvStoreSM) registerHandlers() {
	// only write command need to be registered as internal
	// kv
	kvsm.router.RegisterInternal("del", kvsm.localDelCommand)
	kvsm.router.RegisterInternal("set", kvsm.localSetCommand)
	kvsm.router.RegisterInternal("setnx", kvsm.localSetnxCommand)
	kvsm.router.RegisterInternal("mset", kvsm.localMSetCommand)
	kvsm.router.RegisterInternal("incr", kvsm.localIncrCommand)
	kvsm.router.RegisterInternal("incrby", kvsm.localIncrByCommand)
	kvsm.router.RegisterInternal("plset", kvsm.localPlsetCommand)
	kvsm.router.RegisterInternal("pfadd", kvsm.localPFAddCommand)
	//kvsm.router.RegisterInternal("pfcount", kvsm.localPFCountCommand)
	// hash
	kvsm.router.RegisterInternal("hset", kvsm.localHSetCommand)
	kvsm.router.RegisterInternal("hsetnx", kvsm.localHSetNXCommand)
	kvsm.router.RegisterInternal("hmset", kvsm.localHMsetCommand)
	kvsm.router.RegisterInternal("hdel", kvsm.localHDelCommand)
	kvsm.router.RegisterInternal("hincrby", kvsm.localHIncrbyCommand)
	kvsm.router.RegisterInternal("hclear", kvsm.localHclearCommand)
	kvsm.router.RegisterInternal("hmclear", kvsm.localHMClearCommand)
	// for json
	kvsm.router.RegisterInternal("json.set", kvsm.localJSONSetCommand)
	kvsm.router.RegisterInternal("json.del", kvsm.localJSONDelCommand)
	kvsm.router.RegisterInternal("json.arrappend", kvsm.localJSONArrayAppendCommand)
	kvsm.router.RegisterInternal("json.arrpop", kvsm.localJSONArrayPopCommand)
	// list
	kvsm.router.RegisterInternal("lfixkey", kvsm.localLfixkeyCommand)
	kvsm.router.RegisterInternal("lpop", kvsm.localLpopCommand)
	kvsm.router.RegisterInternal("lpush", kvsm.localLpushCommand)
	kvsm.router.RegisterInternal("lset", kvsm.localLsetCommand)
	kvsm.router.RegisterInternal("ltrim", kvsm.localLtrimCommand)
	kvsm.router.RegisterInternal("rpop", kvsm.localRpopCommand)
	kvsm.router.RegisterInternal("rpush", kvsm.localRpushCommand)
	kvsm.router.RegisterInternal("lclear", kvsm.localLclearCommand)
	kvsm.router.RegisterInternal("lmclear", kvsm.localLMClearCommand)
	// zset
	kvsm.router.RegisterInternal("zfixkey", kvsm.localZFixKeyCommand)
	kvsm.router.RegisterInternal("zadd", kvsm.localZaddCommand)
	kvsm.router.RegisterInternal("zincrby", kvsm.localZincrbyCommand)
	kvsm.router.RegisterInternal("zrem", kvsm.localZremCommand)
	kvsm.router.RegisterInternal("zremrangebyrank", kvsm.localZremrangebyrankCommand)
	kvsm.router.RegisterInternal("zremrangebyscore", kvsm.localZremrangebyscoreCommand)
	kvsm.router.RegisterInternal("zremrangebylex", kvsm.localZremrangebylexCommand)
	kvsm.router.RegisterInternal("zclear", kvsm.localZclearCommand)
	kvsm.router.RegisterInternal("zmclear", kvsm.localZMClearCommand)
	// set
	kvsm.router.RegisterInternal("sadd", kvsm.localSadd)
	kvsm.router.RegisterInternal("srem", kvsm.localSrem)
	kvsm.router.RegisterInternal("sclear", kvsm.localSclear)
	kvsm.router.RegisterInternal("smclear", kvsm.localSmclear)
	kvsm.router.RegisterInternal("spop", kvsm.localSpop)
	// expire
	kvsm.router.RegisterInternal("setex", kvsm.localSetexCommand)
	kvsm.router.RegisterInternal("expire", kvsm.localExpireCommand)
	kvsm.router.RegisterInternal("lexpire", kvsm.localListExpireCommand)
	kvsm.router.RegisterInternal("hexpire", kvsm.localHashExpireCommand)
	kvsm.router.RegisterInternal("sexpire", kvsm.localSetExpireCommand)
	kvsm.router.RegisterInternal("zexpire", kvsm.localZSetExpireCommand)
	kvsm.router.RegisterInternal("persist", kvsm.localPersistCommand)
	kvsm.router.RegisterInternal("hpersist", kvsm.localHashPersistCommand)
	kvsm.router.RegisterInternal("lpersist", kvsm.localListPersistCommand)
	kvsm.router.RegisterInternal("spersist", kvsm.localSetPersistCommand)
	kvsm.router.RegisterInternal("zpersist", kvsm.localZSetPersistCommand)
}

func (nd *KVNode) registerHandler() {
	if nd.machineConfig.LearnerRole != "" {
		// other learner role should only sync from raft log, so no need redis API
		return
	}
	// for kv
	nd.router.Register(false, "get", wrapReadCommandK(nd.getCommand))
	nd.router.Register(false, "mget", wrapReadCommandKK(nd.mgetCommand))
	nd.router.Register(true, "set", wrapWriteCommandKV(nd, nd.setCommand))
	nd.router.Register(true, "setnx", wrapWriteCommandKV(nd, nd.setnxCommand))
	nd.router.Register(true, "incr", wrapWriteCommandK(nd, nd.incrCommand))
	nd.router.Register(true, "incrby", wrapWriteCommandKV(nd, nd.incrbyCommand))
	nd.router.Register(true, "pfadd", wrapWriteCommandKAnySubkey(nd, nd.pfaddCommand, 0))
	nd.router.Register(false, "pfcount", wrapReadCommandK(nd.pfcountCommand))
	// for hash
	nd.router.Register(false, "hget", wrapReadCommandKSubkey(nd.hgetCommand))
	nd.router.Register(false, "hgetall", wrapReadCommandK(nd.hgetallCommand))
	nd.router.Register(false, "hkeys", wrapReadCommandK(nd.hkeysCommand))
	nd.router.Register(false, "hexists", wrapReadCommandKSubkey(nd.hexistsCommand))
	nd.router.Register(false, "hmget", wrapReadCommandKSubkeySubkey(nd.hmgetCommand))
	nd.router.Register(false, "hlen", wrapReadCommandK(nd.hlenCommand))
	nd.router.Register(true, "hset", wrapWriteCommandKSubkeyV(nd, nd.hsetCommand))
	nd.router.Register(true, "hsetnx", wrapWriteCommandKSubkeyV(nd, nd.hsetnxCommand))
	nd.router.Register(true, "hmset", wrapWriteCommandKSubkeyVSubkeyV(nd, nd.hmsetCommand))
	nd.router.Register(true, "hdel", wrapWriteCommandKSubkeySubkey(nd, nd.hdelCommand))
	nd.router.Register(true, "hincrby", wrapWriteCommandKSubkeyV(nd, nd.hincrbyCommand))
	nd.router.Register(true, "hclear", wrapWriteCommandK(nd, nd.hclearCommand))
	// for json
	nd.router.Register(false, "json.get", wrapReadCommandKSubkeySubkey(nd.jsonGetCommand))
	nd.router.Register(false, "json.keyexists", wrapReadCommandK(nd.jsonKeyExistsCommand))
	// get the same path from several json keys
	nd.router.Register(false, "json.mkget", nd.jsonmkGetCommand)
	nd.router.Register(false, "json.type", wrapReadCommandKAnySubkey(nd.jsonTypeCommand))
	nd.router.Register(false, "json.arrlen", wrapReadCommandKAnySubkey(nd.jsonArrayLenCommand))
	nd.router.Register(false, "json.objkeys", wrapReadCommandKAnySubkey(nd.jsonObjKeysCommand))
	nd.router.Register(false, "json.objlen", wrapReadCommandKAnySubkey(nd.jsonObjLenCommand))
	nd.router.Register(true, "json.set", wrapWriteCommandKSubkeyV(nd, nd.jsonSetCommand))
	nd.router.Register(true, "json.del", wrapWriteCommandKSubkeySubkey(nd, nd.jsonDelCommand))
	nd.router.Register(true, "json.arrappend", wrapWriteCommandKAnySubkey(nd, nd.jsonArrayAppendCommand, 2))
	nd.router.Register(true, "json.arrpop", wrapWriteCommandKAnySubkey(nd, nd.jsonArrayPopCommand, 0))
	// for list
	nd.router.Register(false, "lindex", wrapReadCommandKSubkey(nd.lindexCommand))
	nd.router.Register(false, "llen", wrapReadCommandK(nd.llenCommand))
	nd.router.Register(false, "lrange", wrapReadCommandKAnySubkey(nd.lrangeCommand))
	nd.router.Register(true, "lfixkey", wrapWriteCommandK(nd, nd.lfixkeyCommand))
	nd.router.Register(true, "lpop", wrapWriteCommandK(nd, nd.lpopCommand))
	nd.router.Register(true, "lpush", wrapWriteCommandKVV(nd, nd.lpushCommand))
	nd.router.Register(true, "lset", nd.lsetCommand)
	nd.router.Register(true, "ltrim", nd.ltrimCommand)
	nd.router.Register(true, "rpop", wrapWriteCommandK(nd, nd.rpopCommand))
	nd.router.Register(true, "rpush", wrapWriteCommandKVV(nd, nd.rpushCommand))
	nd.router.Register(true, "lclear", wrapWriteCommandK(nd, nd.lclearCommand))
	// for zset
	nd.router.Register(false, "zscore", wrapReadCommandKSubkey(nd.zscoreCommand))
	nd.router.Register(false, "zcount", wrapReadCommandKAnySubkey(nd.zcountCommand))
	nd.router.Register(false, "zcard", wrapReadCommandK(nd.zcardCommand))
	nd.router.Register(false, "zlexcount", wrapReadCommandKAnySubkey(nd.zlexcountCommand))
	nd.router.Register(false, "zrange", wrapReadCommandKAnySubkey(nd.zrangeCommand))
	nd.router.Register(false, "zrevrange", wrapReadCommandKAnySubkey(nd.zrevrangeCommand))
	nd.router.Register(false, "zrangebylex", wrapReadCommandKAnySubkey(nd.zrangebylexCommand))
	nd.router.Register(false, "zrangebyscore", wrapReadCommandKAnySubkey(nd.zrangebyscoreCommand))
	nd.router.Register(false, "zrevrangebyscore", wrapReadCommandKAnySubkey(nd.zrevrangebyscoreCommand))
	nd.router.Register(false, "zrank", wrapReadCommandKSubkey(nd.zrankCommand))
	nd.router.Register(false, "zrevrank", wrapReadCommandKSubkey(nd.zrevrankCommand))

	nd.router.Register(true, "zfixkey", wrapWriteCommandK(nd, nd.zfixkeyCommand))
	nd.router.Register(true, "zadd", nd.zaddCommand)
	nd.router.Register(true, "zincrby", nd.zincrbyCommand)
	nd.router.Register(true, "zrem", wrapWriteCommandKSubkeySubkey(nd, nd.zremCommand))
	nd.router.Register(true, "zremrangebyrank", nd.zremrangebyrankCommand)
	nd.router.Register(true, "zremrangebyscore", nd.zremrangebyscoreCommand)
	nd.router.Register(true, "zremrangebylex", nd.zremrangebylexCommand)
	nd.router.Register(true, "zclear", wrapWriteCommandK(nd, nd.zclearCommand))
	// for set
	nd.router.Register(false, "scard", wrapReadCommandK(nd.scardCommand))
	nd.router.Register(false, "sismember", wrapReadCommandKSubkey(nd.sismemberCommand))
	nd.router.Register(false, "smembers", wrapReadCommandK(nd.smembersCommand))
	nd.router.Register(true, "spop", nd.spopCommand)
	nd.router.Register(true, "sadd", wrapWriteCommandKSubkeySubkey(nd, nd.saddCommand))
	nd.router.Register(true, "srem", wrapWriteCommandKSubkeySubkey(nd, nd.sremCommand))
	nd.router.Register(true, "sclear", wrapWriteCommandK(nd, nd.sclearCommand))
	// for ttl
	nd.router.Register(false, "ttl", wrapReadCommandK(nd.ttlCommand))
	nd.router.Register(false, "httl", wrapReadCommandK(nd.httlCommand))
	nd.router.Register(false, "lttl", wrapReadCommandK(nd.lttlCommand))
	nd.router.Register(false, "sttl", wrapReadCommandK(nd.sttlCommand))
	nd.router.Register(false, "zttl", wrapReadCommandK(nd.zttlCommand))

	nd.router.Register(true, "setex", wrapWriteCommandKVV(nd, nd.setexCommand))
	nd.router.Register(true, "expire", wrapWriteCommandKV(nd, nd.expireCommand))
	nd.router.Register(true, "hexpire", wrapWriteCommandKV(nd, nd.hashExpireCommand))
	nd.router.Register(true, "lexpire", wrapWriteCommandKV(nd, nd.listExpireCommand))
	nd.router.Register(true, "sexpire", wrapWriteCommandKV(nd, nd.setExpireCommand))
	nd.router.Register(true, "zexpire", wrapWriteCommandKV(nd, nd.zsetExpireCommand))

	nd.router.Register(true, "persist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "hpersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "lpersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "spersist", wrapWriteCommandK(nd, nd.persistCommand))
	nd.router.Register(true, "zpersist", wrapWriteCommandK(nd, nd.persistCommand))

	// for scan
	nd.router.Register(false, "hscan", wrapReadCommandKAnySubkey(nd.hscanCommand))
	nd.router.Register(false, "sscan", wrapReadCommandKAnySubkey(nd.sscanCommand))
	nd.router.Register(false, "zscan", wrapReadCommandKAnySubkey(nd.zscanCommand))

	// for geohash
	nd.router.Register(true, "geoadd", nd.geoaddCommand)
	nd.router.Register(false, "geohash", wrapReadCommandKAnySubkeyN(nd.geohashCommand, 1))
	nd.router.Register(false, "geodist", wrapReadCommandKAnySubkey(nd.geodistCommand))
	nd.router.Register(false, "geopos", wrapReadCommandKAnySubkeyN(nd.geoposCommand, 1))
	nd.router.Register(false, "georadius", wrapReadCommandKAnySubkeyN(nd.geoRadiusCommand, 4))
	nd.router.Register(false, "georadiusbymember", wrapReadCommandKAnySubkeyN(nd.geoRadiusByMemberCommand, 3))

	//for cross mutil partion
	nd.router.RegisterMerge("scan", wrapMergeCommand(nd.scanCommand))
	nd.router.RegisterMerge("advscan", nd.advanceScanCommand)
	nd.router.RegisterMerge("fullscan", nd.fullScanCommand)
	nd.router.RegisterMerge("hidx.from", nd.hindexSearchCommand)

	nd.router.RegisterMerge("exists", wrapMergeCommandKK(nd.existsCommand))
	nd.router.RegisterWriteMerge("del", wrapWriteMergeCommandKK(nd, nd.delCommand))
	//nd.router.RegisterWriteMerge("mset", nd.msetCommand)
	nd.router.RegisterWriteMerge("plset", wrapWriteMergeCommandKVKV(nd, nd.plsetCommand))

}

func (kvsm *kvStoreSM) registerConflictHandlers() {
	// only write command
	kvsm.cRouter.Register("del", kvsm.checkKVConflict)
	kvsm.cRouter.Register("set", kvsm.checkKVConflict)
	kvsm.cRouter.Register("setnx", kvsm.checkKVConflict)
	kvsm.cRouter.Register("incr", kvsm.checkKVConflict)
	kvsm.cRouter.Register("incrby", kvsm.checkKVConflict)
	kvsm.cRouter.Register("plset", kvsm.checkKVKVConflict)
	// hll
	kvsm.cRouter.Register("pfadd", kvsm.checkHLLConflict)
	// hash
	kvsm.cRouter.Register("hset", kvsm.checkHashKFVConflict)
	kvsm.cRouter.Register("hsetnx", kvsm.checkHashKFVConflict)
	kvsm.cRouter.Register("hincrby", kvsm.checkHashKFVConflict)
	kvsm.cRouter.Register("hmset", kvsm.checkHashKFVConflict)
	kvsm.cRouter.Register("hdel", kvsm.checkHashKFFConflict)

	// list
	kvsm.cRouter.Register("lpop", kvsm.checkListConflict)
	kvsm.cRouter.Register("lpush", kvsm.checkListConflict)
	kvsm.cRouter.Register("lset", kvsm.checkListConflict)
	kvsm.cRouter.Register("ltrim", kvsm.checkListConflict)
	kvsm.cRouter.Register("rpop", kvsm.checkListConflict)
	kvsm.cRouter.Register("rpush", kvsm.checkListConflict)
	// zset
	kvsm.cRouter.Register("zadd", kvsm.checkZSetConflict)
	kvsm.cRouter.Register("zincrby", kvsm.checkZSetConflict)
	kvsm.cRouter.Register("zrem", kvsm.checkZSetConflict)
	kvsm.cRouter.Register("zremrangebyrank", kvsm.checkZSetConflict)
	kvsm.cRouter.Register("zremrangebyscore", kvsm.checkZSetConflict)
	kvsm.cRouter.Register("zremrangebylex", kvsm.checkZSetConflict)
	// set
	kvsm.cRouter.Register("sadd", kvsm.checkSetConflict)
	kvsm.cRouter.Register("srem", kvsm.checkSetConflict)
	kvsm.cRouter.Register("spop", kvsm.checkSetConflict)
	// expire
	kvsm.cRouter.Register("setex", kvsm.checkKVConflict)
	kvsm.cRouter.Register("expire", kvsm.checkKVConflict)
	kvsm.cRouter.Register("persist", kvsm.checkKVConflict)
}
