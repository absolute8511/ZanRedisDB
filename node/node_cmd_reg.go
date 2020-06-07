package node

func (kvsm *kvStoreSM) registerHandlers() {

	kvsm.router.RegisterInternal("noopwrite", kvsm.localNoOpWriteCommand)
	// only write command need to be registered as internal
	// kv
	kvsm.router.RegisterInternal("del", kvsm.localDelCommand)
	kvsm.router.RegisterInternal("set", kvsm.localSetCommand)
	kvsm.router.RegisterInternal("append", kvsm.localAppendCommand)
	kvsm.router.RegisterInternal("setrange", kvsm.localSetRangeCommand)
	kvsm.router.RegisterInternal("getset", kvsm.localGetSetCommand)
	kvsm.router.RegisterInternal("setbit", kvsm.localBitSetCommand)
	kvsm.router.RegisterInternal("setbitv2", kvsm.localBitSetV2Command)
	kvsm.router.RegisterInternal("setnx", kvsm.localSetnxCommand)
	kvsm.router.RegisterInternal("mset", kvsm.localMSetCommand)
	kvsm.router.RegisterInternal("incr", kvsm.localIncrCommand)
	kvsm.router.RegisterInternal("incrby", kvsm.localIncrByCommand)
	kvsm.router.RegisterInternal("plset", kvsm.localPlsetCommand)
	kvsm.router.RegisterInternal("pfadd", kvsm.localPFAddCommand)
	//kvsm.router.RegisterInternal("pfcount", kvsm.localPFCountCommand)
	// bitmap
	kvsm.router.RegisterInternal("bitclear", kvsm.localBitClearCommand)

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
	// expire&persist
	kvsm.router.RegisterInternal("setex", kvsm.localSetexCommand)
	kvsm.router.RegisterInternal("expire", kvsm.localExpireCommand)
	kvsm.router.RegisterInternal("lexpire", kvsm.localListExpireCommand)
	kvsm.router.RegisterInternal("hexpire", kvsm.localHashExpireCommand)
	kvsm.router.RegisterInternal("sexpire", kvsm.localSetExpireCommand)
	kvsm.router.RegisterInternal("zexpire", kvsm.localZSetExpireCommand)
	kvsm.router.RegisterInternal("bexpire", kvsm.localBitExpireCommand)

	kvsm.router.RegisterInternal("persist", kvsm.localPersistCommand)
	kvsm.router.RegisterInternal("hpersist", kvsm.localHashPersistCommand)
	kvsm.router.RegisterInternal("lpersist", kvsm.localListPersistCommand)
	kvsm.router.RegisterInternal("spersist", kvsm.localSetPersistCommand)
	kvsm.router.RegisterInternal("zpersist", kvsm.localZSetPersistCommand)
	kvsm.router.RegisterInternal("bpersist", kvsm.localBitPersistCommand)

	if enableSlowLimiterTest && kvsm.slowLimiter != nil {
		kvsm.router.RegisterInternal("slowwrite1s_test", kvsm.slowLimiter.testSlowWrite1s)
		kvsm.router.RegisterInternal("slowwrite100ms_test", kvsm.slowLimiter.testSlowWrite100ms)
		kvsm.router.RegisterInternal("slowwrite50ms_test", kvsm.slowLimiter.testSlowWrite50ms)
		kvsm.router.RegisterInternal("slowwrite5ms_test", kvsm.slowLimiter.testSlowWrite5ms)
	}
}

func (nd *KVNode) registerHandler() {
	if nd.machineConfig.LearnerRole != "" {
		// other learner role should only sync from raft log, so no need redis API
		return
	}
	// for test on no rocks
	nd.router.RegisterWrite("noopwrite", wrapWriteCommandKV(nd, checkOKRsp))
	// for kv
	nd.router.RegisterRead("get", wrapReadCommandK(nd.getCommand))
	nd.router.RegisterRead("strlen", wrapReadCommandK(nd.strlenCommand))
	nd.router.RegisterRead("getnolock", wrapReadCommandK(nd.getNoLockCommand))
	nd.router.RegisterRead("getbit", wrapReadCommandKAnySubkeyN(nd.getbitCommand, 1))
	nd.router.RegisterRead("bitcount", wrapReadCommandKAnySubkey(nd.bitcountCommand))
	nd.router.RegisterRead("mget", wrapReadCommandKK(nd.mgetCommand))
	nd.router.RegisterWrite("set", wrapWriteCommandKV(nd, checkOKRsp))
	nd.router.RegisterWrite("append", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("setrange", wrapWriteCommandKAnySubkey(nd, checkAndRewriteIntRsp, 2))
	nd.router.RegisterWrite("getset", wrapWriteCommandKV(nd, checkAndRewriteBulkRsp))
	nd.router.RegisterWrite("setbit", nd.setbitCommand)
	nd.router.RegisterWrite("setbitv2", nd.setbitCommand)
	nd.router.RegisterWrite("setnx", nd.setnxCommand)
	nd.router.RegisterWrite("incr", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("incrby", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("pfadd", wrapWriteCommandKAnySubkey(nd, checkAndRewriteIntRsp, 0))
	nd.router.RegisterRead("pfcount", wrapReadCommandK(nd.pfcountCommand))
	nd.router.RegisterWrite("bitclear", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	// for hash
	nd.router.RegisterRead("hget", wrapReadCommandKSubkey(nd.hgetCommand))
	nd.router.RegisterRead("hgetall", wrapReadCommandK(nd.hgetallCommand))
	nd.router.RegisterRead("hkeys", wrapReadCommandK(nd.hkeysCommand))
	nd.router.RegisterRead("hvals", wrapReadCommandK(nd.hvalsCommand))
	nd.router.RegisterRead("hexists", wrapReadCommandKSubkey(nd.hexistsCommand))
	nd.router.RegisterRead("hmget", wrapReadCommandKSubkeySubkey(nd.hmgetCommand))
	nd.router.RegisterRead("hlen", wrapReadCommandK(nd.hlenCommand))
	nd.router.RegisterWrite("hset", wrapWriteCommandKSubkeyV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hsetnx", wrapWriteCommandKSubkeyV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hmset", wrapWriteCommandKSubkeyVSubkeyV(nd, checkOKRsp))
	nd.router.RegisterWrite("hdel", wrapWriteCommandKSubkeySubkey(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hincrby", wrapWriteCommandKSubkeyV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hclear", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	// for json
	nd.router.RegisterRead("json.get", wrapReadCommandKAnySubkey(nd.jsonGetCommand))
	nd.router.RegisterRead("json.keyexists", wrapReadCommandK(nd.jsonKeyExistsCommand))
	// get the same path from several json keys
	nd.router.RegisterRead("json.mkget", nd.jsonmkGetCommand)
	nd.router.RegisterRead("json.type", wrapReadCommandKAnySubkey(nd.jsonTypeCommand))
	nd.router.RegisterRead("json.arrlen", wrapReadCommandKAnySubkey(nd.jsonArrayLenCommand))
	nd.router.RegisterRead("json.objkeys", wrapReadCommandKAnySubkey(nd.jsonObjKeysCommand))
	nd.router.RegisterRead("json.objlen", wrapReadCommandKAnySubkey(nd.jsonObjLenCommand))
	nd.router.RegisterWrite("json.set", wrapWriteCommandKSubkeyV(nd, checkOKRsp))
	nd.router.RegisterWrite("json.del", wrapWriteCommandKAnySubkey(nd, checkAndRewriteIntRsp, 0))
	nd.router.RegisterWrite("json.arrappend", wrapWriteCommandKAnySubkey(nd, checkAndRewriteIntRsp, 2))
	nd.router.RegisterWrite("json.arrpop", wrapWriteCommandKAnySubkey(nd, checkAndRewriteBulkRsp, 0))
	// for list
	nd.router.RegisterRead("lindex", wrapReadCommandKSubkey(nd.lindexCommand))
	nd.router.RegisterRead("llen", wrapReadCommandK(nd.llenCommand))
	nd.router.RegisterRead("lrange", wrapReadCommandKAnySubkey(nd.lrangeCommand))
	nd.router.RegisterWrite("lfixkey", wrapWriteCommandK(nd, checkOKRsp))
	nd.router.RegisterWrite("lpop", wrapWriteCommandK(nd, checkAndRewriteBulkRsp))
	nd.router.RegisterWrite("lpush", wrapWriteCommandKVV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("lset", nd.lsetCommand)
	nd.router.RegisterWrite("ltrim", nd.ltrimCommand)
	nd.router.RegisterWrite("rpop", wrapWriteCommandK(nd, checkAndRewriteBulkRsp))
	nd.router.RegisterWrite("rpush", wrapWriteCommandKVV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("lclear", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	// for zset
	nd.router.RegisterRead("zscore", wrapReadCommandKSubkey(nd.zscoreCommand))
	nd.router.RegisterRead("zcount", wrapReadCommandKAnySubkey(nd.zcountCommand))
	nd.router.RegisterRead("zcard", wrapReadCommandK(nd.zcardCommand))
	nd.router.RegisterRead("zlexcount", wrapReadCommandKAnySubkey(nd.zlexcountCommand))
	nd.router.RegisterRead("zrange", wrapReadCommandKAnySubkey(nd.zrangeCommand))
	nd.router.RegisterRead("zrevrange", wrapReadCommandKAnySubkey(nd.zrevrangeCommand))
	nd.router.RegisterRead("zrangebylex", wrapReadCommandKAnySubkey(nd.zrangebylexCommand))
	nd.router.RegisterRead("zrangebyscore", wrapReadCommandKAnySubkey(nd.zrangebyscoreCommand))
	nd.router.RegisterRead("zrevrangebyscore", wrapReadCommandKAnySubkey(nd.zrevrangebyscoreCommand))
	nd.router.RegisterRead("zrank", wrapReadCommandKSubkey(nd.zrankCommand))
	nd.router.RegisterRead("zrevrank", wrapReadCommandKSubkey(nd.zrevrankCommand))

	nd.router.RegisterWrite("zfixkey", wrapWriteCommandK(nd, checkOKRsp))
	nd.router.RegisterWrite("zadd", nd.zaddCommand)
	nd.router.RegisterWrite("zincrby", nd.zincrbyCommand)
	nd.router.RegisterWrite("zrem", wrapWriteCommandKSubkeySubkey(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("zremrangebyrank", nd.zremrangebyrankCommand)
	nd.router.RegisterWrite("zremrangebyscore", nd.zremrangebyscoreCommand)
	nd.router.RegisterWrite("zremrangebylex", nd.zremrangebylexCommand)
	nd.router.RegisterWrite("zclear", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	// for set
	nd.router.RegisterRead("scard", wrapReadCommandK(nd.scardCommand))
	nd.router.RegisterRead("sismember", wrapReadCommandKSubkey(nd.sismemberCommand))
	nd.router.RegisterRead("smembers", wrapReadCommandK(nd.smembersCommand))
	nd.router.RegisterRead("srandmember", wrapReadCommandKAnySubkey(nd.srandmembersCommand))
	nd.router.RegisterWrite("spop", nd.spopCommand)
	nd.router.RegisterWrite("sadd", nd.saddCommand)
	nd.router.RegisterWrite("srem", wrapWriteCommandKSubkeySubkey(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("sclear", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	// for ttl
	nd.router.RegisterRead("ttl", wrapReadCommandK(nd.ttlCommand))
	nd.router.RegisterRead("httl", wrapReadCommandK(nd.httlCommand))
	nd.router.RegisterRead("lttl", wrapReadCommandK(nd.lttlCommand))
	nd.router.RegisterRead("sttl", wrapReadCommandK(nd.sttlCommand))
	nd.router.RegisterRead("zttl", wrapReadCommandK(nd.zttlCommand))
	nd.router.RegisterRead("bttl", wrapReadCommandK(nd.bttlCommand))
	// extended exist
	nd.router.RegisterRead("hkeyexist", wrapReadCommandK(nd.hKeyExistCommand))
	nd.router.RegisterRead("lkeyexist", wrapReadCommandK(nd.lKeyExistCommand))
	nd.router.RegisterRead("skeyexist", wrapReadCommandK(nd.sKeyExistCommand))
	nd.router.RegisterRead("zkeyexist", wrapReadCommandK(nd.zKeyExistCommand))
	nd.router.RegisterRead("bkeyexist", wrapReadCommandK(nd.bKeyExistCommand))

	nd.router.RegisterWrite("setex", wrapWriteCommandKVV(nd, checkOKRsp))
	nd.router.RegisterWrite("expire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hexpire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("lexpire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("sexpire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("zexpire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("bexpire", wrapWriteCommandKV(nd, checkAndRewriteIntRsp))

	nd.router.RegisterWrite("persist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("hpersist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("lpersist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("spersist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("zpersist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))
	nd.router.RegisterWrite("bpersist", wrapWriteCommandK(nd, checkAndRewriteIntRsp))

	// for scan
	nd.router.RegisterRead("hscan", wrapReadCommandKAnySubkey(nd.hscanCommand))
	nd.router.RegisterRead("sscan", wrapReadCommandKAnySubkey(nd.sscanCommand))
	nd.router.RegisterRead("zscan", wrapReadCommandKAnySubkey(nd.zscanCommand))
	nd.router.RegisterRead("hrevscan", wrapReadCommandKAnySubkey(nd.hscanCommand))
	nd.router.RegisterRead("srevscan", wrapReadCommandKAnySubkey(nd.sscanCommand))
	nd.router.RegisterRead("zrevscan", wrapReadCommandKAnySubkey(nd.zscanCommand))

	// for geohash
	nd.router.RegisterWrite("geoadd", nd.geoaddCommand)
	nd.router.RegisterRead("geohash", wrapReadCommandKAnySubkeyN(nd.geohashCommand, 1))
	nd.router.RegisterRead("geodist", wrapReadCommandKAnySubkey(nd.geodistCommand))
	nd.router.RegisterRead("geopos", wrapReadCommandKAnySubkeyN(nd.geoposCommand, 1))
	nd.router.RegisterRead("georadius", wrapReadCommandKAnySubkeyN(nd.geoRadiusCommand, 4))
	nd.router.RegisterRead("georadiusbymember", wrapReadCommandKAnySubkeyN(nd.geoRadiusByMemberCommand, 3))

	//for cross mutil partion
	nd.router.RegisterMerge("scan", wrapMergeCommand(nd.scanCommand))
	nd.router.RegisterMerge("advscan", nd.advanceScanCommand)
	nd.router.RegisterMerge("revscan", wrapMergeCommand(nd.scanCommand))
	nd.router.RegisterMerge("advrevscan", nd.advanceScanCommand)
	nd.router.RegisterMerge("fullscan", nd.fullScanCommand)
	nd.router.RegisterMerge("hidx.from", nd.hindexSearchCommand)

	nd.router.RegisterMerge("exists", wrapMergeCommandKK(nd.existsCommand))
	// make sure the merged write command will be stopped if cluster is not allowed to write
	nd.router.RegisterWriteMerge("del", wrapWriteMergeCommandKK(nd, checkAndRewriteIntRsp))
	//nd.router.RegisterWriteMerge("mset", nd.msetCommand)
	nd.router.RegisterWriteMerge("plset", wrapWriteMergeCommandKVKV(nd, nil))

	if enableSlowLimiterTest {
		nd.router.RegisterWrite("slowwrite1s_test", wrapWriteCommandKV(nd, checkOKRsp))
		nd.router.RegisterWrite("slowwrite100ms_test", wrapWriteCommandKV(nd, checkOKRsp))
		nd.router.RegisterWrite("slowwrite50ms_test", wrapWriteCommandKV(nd, checkOKRsp))
		nd.router.RegisterWrite("slowwrite5ms_test", wrapWriteCommandKV(nd, checkOKRsp))
	}
}

func (kvsm *kvStoreSM) registerConflictHandlers() {
	// only write command
	kvsm.cRouter.Register("del", kvsm.checkKVConflict)
	kvsm.cRouter.Register("set", kvsm.checkKVConflict)
	kvsm.cRouter.Register("append", kvsm.checkKVConflict)
	kvsm.cRouter.Register("setrange", kvsm.checkKVConflict)
	kvsm.cRouter.Register("getset", kvsm.checkKVConflict)
	kvsm.cRouter.Register("setnx", kvsm.checkKVConflict)
	kvsm.cRouter.Register("incr", kvsm.checkKVConflict)
	kvsm.cRouter.Register("incrby", kvsm.checkKVConflict)
	kvsm.cRouter.Register("plset", kvsm.checkKVKVConflict)
	// hll
	kvsm.cRouter.Register("pfadd", kvsm.checkHLLConflict)
	// bitmap
	kvsm.cRouter.Register("setbitv2", kvsm.checkBitmapConflict)
	kvsm.cRouter.Register("setbit", kvsm.checkBitmapConflict)
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
