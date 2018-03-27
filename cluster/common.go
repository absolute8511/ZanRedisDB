package cluster

import (
	"bytes"
	"strconv"
	"strings"
)

const (
	ErrFailedOnNotLeader = "E_FAILED_ON_NOT_LEADER"
)

const (
	MAX_WRITE_RETRY = 10
)

type CoordErrType int

const (
	CoordNoErr CoordErrType = iota
	CoordCommonErr
	CoordNetErr
	CoordClusterErr
	CoordLocalErr
	CoordLocalTmpErr
	CoordTmpErr
	CoordClusterNoRetryWriteErr
	CoordRegisterErr
)

const (
	RpcNoErr ErrRPCRetCode = iota
	RpcCommonErr
)

const (
	RpcErrLeavingISRWait ErrRPCRetCode = iota + 10
	RpcErrNoLeader
	RpcErrLeaderSessionMismatch
	RpcErrNamespaceNotExist
	RpcErrMissingNamespaceCoord
	RpcErrNamespaceCoordConflicted
	RpcErrNamespaceCoordStateInvalid
	RpcErrNamespaceLoading
	RpcErrWriteOnNonISR
)

type ErrRPCRetCode int

// note: since the gorpc will treat error type as special,
// we should not implement the error interface for CoordErr as response type
type CoordErr struct {
	ErrMsg  string
	ErrCode ErrRPCRetCode
	ErrType CoordErrType
}

type CommonCoordErr struct {
	CoordErr
}

func (self *CommonCoordErr) Error() string {
	return self.String()
}

func NewCoordErr(msg string, etype CoordErrType) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: RpcCommonErr,
	}
}

func NewCoordErrWithCode(msg string, etype CoordErrType, code ErrRPCRetCode) *CoordErr {
	return &CoordErr{
		ErrMsg:  msg,
		ErrType: etype,
		ErrCode: code,
	}
}

func (self *CoordErr) ToErrorType() error {
	e := &CommonCoordErr{}
	e.ErrMsg = self.ErrMsg
	e.ErrType = self.ErrType
	e.ErrCode = self.ErrCode
	return e
}

func (self *CoordErr) String() string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString("ErrType:")
	tmpbuf.WriteString(strconv.Itoa(int(self.ErrType)))
	tmpbuf.WriteString(" ErrCode:")
	tmpbuf.WriteString(strconv.Itoa(int(self.ErrCode)))
	tmpbuf.WriteString(" : ")
	tmpbuf.WriteString(self.ErrMsg)
	return tmpbuf.String()
}

func (self *CoordErr) HasError() bool {
	if self.ErrType == CoordNoErr && self.ErrCode == RpcNoErr {
		return false
	}
	return true
}

func (self *CoordErr) IsEqual(other *CoordErr) bool {
	if other == nil || self == nil {
		return false
	}

	if self == other {
		return true
	}

	if other.ErrCode != self.ErrCode || other.ErrType != self.ErrType {
		return false
	}

	if other.ErrCode != RpcCommonErr {
		return true
	}
	// only common error need to check if errmsg is equal
	if other.ErrMsg == self.ErrMsg {
		return true
	}
	return false
}

func (self *CoordErr) IsNetErr() bool {
	return self.ErrType == CoordNetErr
}

func (self *CoordErr) IsLocalErr() bool {
	return self.ErrType == CoordLocalErr
}

func (self *CoordErr) CanRetryWrite(retryTimes int) bool {
	if self.ErrType == CoordClusterNoRetryWriteErr {
		return false
	}
	if self.ErrType == CoordTmpErr ||
		self.ErrType == CoordLocalTmpErr {
		if retryTimes > MAX_WRITE_RETRY*3 {
			return false
		}
		return true
	}
	return self.IsNetErr()
}

var (
	ErrNamespaceInfoNotFound          = NewCoordErr("namespace info not found", CoordClusterErr)
	ErrNamespaceCoordTmpConflicted    = NewCoordErrWithCode("namespace coordinator is conflicted temporally", CoordClusterErr, RpcErrNamespaceCoordConflicted)
	ErrMissingNamespaceCoord          = NewCoordErrWithCode("missing namespace coordinator", CoordClusterErr, RpcErrMissingNamespaceCoord)
	ErrNamespaceCoordStateInvalid     = NewCoordErrWithCode("invalid coordinator state", CoordClusterErr, RpcErrNamespaceCoordStateInvalid)
	ErrClusterChanged                 = NewCoordErrWithCode("cluster changed ", CoordTmpErr, RpcNoErr)
	ErrNamespaceCatchupAlreadyRunning = NewCoordErrWithCode("already try joining the raft group", CoordTmpErr, RpcNoErr)
	ErrCatchupRunningBusy             = NewCoordErrWithCode("too much namespace node joining the raft group", CoordTmpErr, RpcNoErr)
	ErrNamespaceExiting               = NewCoordErrWithCode("too much namespace node joining the raft group", CoordLocalErr, RpcNoErr)
	ErrEpochLessThanCurrent           = NewCoordErrWithCode("epoch should be increased", CoordClusterErr, RpcNoErr)
	ErrLocalInitNamespaceFailed       = NewCoordErrWithCode("init local namespace failed", CoordLocalErr, RpcNoErr)
	ErrNamespaceNotCreated            = NewCoordErrWithCode("namespace not created", CoordLocalErr, RpcNoErr)
	ErrNamespaceConfInvalid           = NewCoordErrWithCode("namespace config is invalid", CoordClusterErr, RpcNoErr)
	ErrNamespaceWaitingSync           = NewCoordErrWithCode("namespace is still waiting sync", CoordTmpErr, RpcNoErr)
	ErrRegisterServiceUnstable        = NewCoordErr("the register service is unstable", CoordTmpErr)
)

func GenNodeID(n *NodeInfo, extra string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(strconv.FormatInt(int64(n.RegID), 10))
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.NodeIP)
	tmpbuf.WriteString(":")
	// rpc port is not used in older version,
	// in order to make it compatible we should keep rpc port empty
	// it is enough to identify the node id with ip+redisport+httpport
	//tmpbuf.WriteString(n.RpcPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RedisPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.HttpPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(extra)
	return tmpbuf.String()
}

func ExtractRegIDFromGenID(nid string) uint64 {
	pos1 := strings.Index(nid, ":")
	if pos1 == -1 {
		return 0
	}
	v, _ := strconv.ParseInt(nid[:pos1], 10, 64)
	return uint64(v)
}

func ExtractNodeInfoFromID(nid string) (ip string, rpc string, redis string, http string) {
	lines := strings.SplitN(nid, ":", 6)
	if len(lines) < 5 {
		return
	}
	ip = lines[1]
	rpc = lines[2]
	redis = lines[3]
	http = lines[4]
	return
}

func FindSlice(in []string, e string) int {
	for i, v := range in {
		if v == e {
			return i
		}
	}
	return -1
}

func MergeList(l []string, r []string) []string {
	newl := make([]string, len(l))
	copy(newl, l)
	for _, e := range r {
		if FindSlice(l, e) == -1 {
			newl = append(newl, e)
		}
	}
	return newl
}

func FilterList(l []string, filter []string) []string {
	newl := make([]string, 0, len(l))
	for _, e := range l {
		if FindSlice(filter, e) != -1 {
			continue
		}
		newl = append(newl, e)
	}
	return newl
}

type NamespaceNameInfo struct {
	NamespaceName      string
	NamespacePartition int
}

func (self *NamespaceNameInfo) String() string {
	return self.NamespaceName + "-" + strconv.Itoa(self.NamespacePartition)
}

type Options struct {
	AutoBalanceAndMigrate bool
	BalanceStart          int
	BalanceEnd            int
	DataDir               string
}
