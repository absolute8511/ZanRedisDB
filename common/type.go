package common

import (
	"github.com/tidwall/redcon"
	"strings"
)

const (
	DIR_PERM  = 0755
	FILE_PERM = 0744
)

type WriteCmd struct {
	Operation string
	Args      [][]byte
}

type KVRecord struct {
	Key   []byte
	Value []byte
}

type KVRecordRet struct {
	Rec KVRecord
	Err error
}

var redisRouter = NewCmdRouter()

type CommandFunc func(redcon.Conn, redcon.Command)
type InternalCommandFunc func(redcon.Command) (interface{}, error)

func GetInternalHandler(name string) (InternalCommandFunc, bool) {
	return redisRouter.GetInternalCmdHandler(name)
}
func GetHandler(name string) (CommandFunc, bool) {
	return redisRouter.GetCmdHandler(name)
}

func RegisterRedisHandler(name string, f CommandFunc) {
	redisRouter.Register(name, f)
}

func RegisterRedisInternalHandler(name string, f InternalCommandFunc) {
	redisRouter.RegisterInternal(name, f)
}

type CmdRouter struct {
	cmds         map[string]CommandFunc
	internalCmds map[string]InternalCommandFunc
}

func NewCmdRouter() *CmdRouter {
	return &CmdRouter{
		cmds:         make(map[string]CommandFunc),
		internalCmds: make(map[string]InternalCommandFunc),
	}
}

func (r *CmdRouter) Register(name string, f CommandFunc) bool {
	if _, ok := r.cmds[strings.ToLower(name)]; ok {
		return false
	}
	r.cmds[name] = f
	return true
}

func (r *CmdRouter) GetCmdHandler(name string) (CommandFunc, bool) {
	v, ok := r.cmds[strings.ToLower(name)]
	return v, ok
}

func (r *CmdRouter) RegisterInternal(name string, f InternalCommandFunc) bool {
	if _, ok := r.internalCmds[strings.ToLower(name)]; ok {
		return false
	}
	r.internalCmds[name] = f
	return true
}

func (r *CmdRouter) GetInternalCmdHandler(name string) (InternalCommandFunc, bool) {
	v, ok := r.internalCmds[strings.ToLower(name)]
	return v, ok
}
