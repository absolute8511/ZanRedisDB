package common

import (
	"fmt"
	"runtime"
)

var (
	VerBinary = "unset"
	BuildTime = "unset"
	Commit    = "unset"
)

func VerString(app string) string {
	return fmt.Sprintf("%s v%s (built w/%s), build at: %s-%s", app, VerBinary, runtime.Version(), BuildTime, Commit)
}
