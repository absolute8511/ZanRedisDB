package common

import (
	"fmt"
	"runtime"
)

const Binary = "0.0.1"

func String(app string) string {
	return fmt.Sprintf("%s v%s (built w/%s)", app, Binary, runtime.Version())
}
