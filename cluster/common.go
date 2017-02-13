package cluster

import (
	"bytes"
	"strconv"
	"strings"
)

const (
	ErrFailedOnNotLeader   = "E_FAILED_ON_NOT_LEADER"
	ErrFailedOnNotWritable = "E_FAILED_ON_NOT_WRITABLE"
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
	CoordSlaveErr
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
	return self.ErrType == CoordSlaveErr ||
		self.IsNetErr()
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
)

func GenNodeID(n *NodeInfo, extra string) string {
	var tmpbuf bytes.Buffer
	tmpbuf.WriteString(n.NodeIP)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RpcPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.RedisPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(n.HttpPort)
	tmpbuf.WriteString(":")
	tmpbuf.WriteString(extra)
	return tmpbuf.String()
}

func ExtractRpcAddrFromID(nid string) string {
	pos1 := strings.Index(nid, ":")
	pos2 := strings.Index(nid[pos1+1:], ":")
	return nid[:pos1+pos2+1]
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
	for _, e := range r {
		if FindSlice(l, e) == -1 {
			l = append(l, e)
		}
	}
	return l
}

func FilterList(l []string, filter []string) []string {
	for _, e := range filter {
		for i, v := range l {
			if e == v {
				copy(l[i:], l[i+1:])
				l = l[:len(l)-1]
				break
			}
		}
	}
	return l
}

type NamespaceNameInfo struct {
	NamespaceName      string
	NamespacePartition int
}

func (self *NamespaceNameInfo) String() string {
	return self.NamespaceName + "-" + strconv.Itoa(self.NamespacePartition)
}
