package main

import (
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/pdserver"
)

func TestAppConfigParse(t *testing.T) {
	flagSet.Parse([]string{})

	configFile := "../../pdserver/pdconf.example.conf"
	var cfg map[string]interface{}
	_, err := toml.DecodeFile(configFile, &cfg)
	if err != nil {
		t.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
	}
	opts := pdserver.NewServerConfig()
	options.Resolve(opts, flagSet, cfg)
	opts.LogDir = path.Join(os.TempDir(), strconv.Itoa(int(time.Now().UnixNano())))
	os.MkdirAll(opts.LogDir, 0755)
	common.SetZapRotateOptions(false, true, path.Join(opts.LogDir, "test.log"), 0, 0, 0)
	s, err := pdserver.NewServer(opts)
	t.Log(err)
	assert.Equal(t, "v2", opts.BalanceVer)

	s.Start()
	t.Log(opts.LogDir)

	time.Sleep(time.Second)
	s.Stop()
}
