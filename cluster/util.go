package cluster

import (
	"github.com/absolute8511/ZanRedisDB/common"
)

var coordLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("cluster"))

func SetLogger(level int32, logger common.Logger) {
	coordLog.SetLevel(level)
	coordLog.Logger = logger
}
