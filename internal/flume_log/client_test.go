package flume_log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient_Info(t *testing.T) {
	client := NewClient("127.0.0.1:5140", "zankv", "zankv_slowlog")
	detail := make(map[string]interface{})

	detail["Scope"] = "youzan:test"
	detail["Key"] = "testkey"
	detail["Note"] = "note_info"
	err := client.Info("测试 Info", detail)
	assert.Nil(t, err)

	detail["Note"] = "note_warn"
	err = client.Warn("测试 Warn", detail)
	assert.Nil(t, err)

	detail["Note"] = "note_debug"
	err = client.Debug("测试 Debug", detail)
	assert.Nil(t, err)

	detail["Note"] = "note_err"
	err = client.Error("测试 error", detail)
	assert.Nil(t, err)
	// wait remote log flush
	time.Sleep(time.Second * 10)
}
