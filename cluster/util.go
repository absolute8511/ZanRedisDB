package cluster

import (
	"github.com/absolute8511/ZanRedisDB/common"
)

var coordLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("cluster"))

func SetLogLevel(level int) {
	coordLog.SetLevel(int32(level))
}

func SetLogger(level int32, logger common.Logger) {
	coordLog.SetLevel(level)
	coordLog.Logger = logger
}
