package node

import (
	"strings"

	"github.com/absolute8511/redcon"
)

type ConflictCheckFunc func(redcon.Command, int64) bool

type conflictRouter struct {
	checkCmds map[string]ConflictCheckFunc
}

func NewConflictRouter() *conflictRouter {
	return &conflictRouter{
		checkCmds: make(map[string]ConflictCheckFunc),
	}
}

func (r *conflictRouter) Register(name string, f ConflictCheckFunc) bool {
	if _, ok := r.checkCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.checkCmds[name] = f
	return true
}

func (r *conflictRouter) GetHandler(name string) (ConflictCheckFunc, bool) {
	v, ok := r.checkCmds[strings.ToLower(name)]
	return v, ok
}

func (kvsm *kvStoreSM) checkKVConflict(cmd redcon.Command, reqTs int64) bool {
	oldTs, err := kvsm.store.KVGetVer(cmd.Args[1])
	if err != nil {
		kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
	}
	if oldTs < reqTs {
		return false
	}
	return true
}

func (kvsm *kvStoreSM) checkKVKVConflict(cmd redcon.Command, reqTs int64) bool {
	kvs := cmd.Args[1:]
	for i := 0; i < len(kvs)-1; i += 2 {
		oldTs, err := kvsm.store.KVGetVer(kvs[i])
		if err != nil {
			kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
		}
		if oldTs >= reqTs {
			return true
		}
	}
	return false
}

func (kvsm *kvStoreSM) checkHashKFVConflict(cmd redcon.Command, reqTs int64) bool {
	fvs := cmd.Args[2:]
	for i := 0; i < len(fvs)-1; i += 2 {
		oldTs, err := kvsm.store.HGetVer(cmd.Args[1], fvs[i])
		if err != nil {
			kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
		}
		if oldTs >= reqTs {
			return true
		}
	}
	return false
}

func (kvsm *kvStoreSM) checkHashKFFConflict(cmd redcon.Command, reqTs int64) bool {
	fvs := cmd.Args[2:]
	for i := 0; i < len(fvs); i++ {
		oldTs, err := kvsm.store.HGetVer(cmd.Args[1], fvs[i])
		if err != nil {
			kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
		}
		if oldTs >= reqTs {
			return true
		}
	}
	return false
}

// for list, set, zset, ts may be not enough.
// If clusterA modified and clusterB modified later and then clusterA
// modified again, the second modification in clusterA will be allowed since its timestamp is newer
// but this will cause inconsistence. So we need forbiden write sync if both timestamps are newer
// than the time we switched the cluster.
func (kvsm *kvStoreSM) checkListConflict(cmd redcon.Command, reqTs int64) bool {
	oldTs, err := kvsm.store.LVer(cmd.Args[1])
	if err != nil {
		kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
	}
	if oldTs >= GetSyncedOnlyChangedTs() && reqTs >= GetSyncedOnlyChangedTs() {
		return true
	}
	if oldTs < reqTs {
		return false
	}
	return true
}

func (kvsm *kvStoreSM) checkSetConflict(cmd redcon.Command, reqTs int64) bool {
	oldTs, err := kvsm.store.SGetVer(cmd.Args[1])
	if err != nil {
		kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
	}
	if oldTs >= GetSyncedOnlyChangedTs() && reqTs >= GetSyncedOnlyChangedTs() {
		return true
	}
	if oldTs < reqTs {
		return false
	}
	return true
}

func (kvsm *kvStoreSM) checkZSetConflict(cmd redcon.Command, reqTs int64) bool {
	oldTs, err := kvsm.store.ZGetVer(cmd.Args[1])
	if err != nil {
		kvsm.Infof("key %v failed to get modify version: %v", cmd.Args[1], err)
	}
	if oldTs >= GetSyncedOnlyChangedTs() && reqTs >= GetSyncedOnlyChangedTs() {
		return true
	}
	if oldTs < reqTs {
		return false
	}
	return true
}

func (kvsm *kvStoreSM) checkHLLConflict(cmd redcon.Command, reqTs int64) bool {
	// hll no need handle conflict since it is not accurately
	return false
}

func (kvsm *kvStoreSM) checkJsonConflict(cmd redcon.Command, reqTs int64) bool {
	return true
}
